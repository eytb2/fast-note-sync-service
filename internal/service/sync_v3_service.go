// Package service: WS v3 git 式快照同步服务。
// 职责：对账（SyncPlan，调 reconcile 引擎）→ blob 需求计算 → 原子 ManifestCommit（乐观锁）
// → 提交后的两方 diff（NotifyManifest 提示用）。规则见 git-sync-redesign.md §2。
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"go.uber.org/zap"
)

// SyncV3Service WS v3 快照同步服务接口
type SyncV3Service interface {
	// SyncPlan 对账：产出计划 + 服务器缺失的 blob 清单（BlobNeed）
	SyncPlan(ctx context.Context, uid int64, vaultName string, req *dto.V3SyncRequest) (*dto.V3SyncPlanMessage, []dto.V3BlobNeedMessage, error)
	// Commit 原子应用客户端变更；成功返回新 epoch 与提示性 ops（NotifyManifest 用）
	Commit(ctx context.Context, uid int64, vaultName string, req *dto.V3ManifestCommitRequest, client string) (*dto.V3ManifestCommitAckMessage, []reconcile.Op, error)
	// ReadBlobInline 读取小体积 blob（笔记文本内联）；超过 limit 返回 false（客户端走分块）
	ReadBlobInline(ctx context.Context, uid int64, hash string, limit int64) ([]byte, bool, error)
	// OpenBlob 打开 blob 读取（分块下载）
	OpenBlob(ctx context.Context, uid int64, hash string) (io.ReadCloser, int64, error)
	// FinalizeBlobUpload 校验暂存文件哈希并移入 blob store（分块上传完成时调用）
	FinalizeBlobUpload(ctx context.Context, uid int64, expectedHash, tempPath string) error
	// BlobExists 判断 blob 是否已存在（秒传）
	BlobExists(uid int64, hash string) bool
	// AddCommitListener 注册提交副作用监听（app 装配期调用，避开构造循环依赖）
	AddCommitListener(l CommitListener)
}

// VaultResolver v3 服务只需要 vault 名 → ID 解析这一项能力（测试用极小桩替身即可）
type VaultResolver interface {
	GetOrCreate(ctx context.Context, uid int64, name string) (*domain.Vault, error)
}

type syncV3Service struct {
	fsRepo    domain.FsEntryRepository
	manifest  domain.VaultManifestRepository
	histRepo  domain.EntryHistoryRepository
	blobs     domain.BlobStore
	vaultSvc  VaultResolver
	logger    *zap.Logger
	listeners []CommitListener
}

// CommitEvent 提交成功事件（副作用监听的输入：FTS/链接索引/同步日志/备份/Git/分享撤销）
type CommitEvent struct {
	UID      int64
	VaultID  int64
	Vault    string
	NewEpoch int64
	Client   string
	Changes  []reconcile.Change
}

// CommitListener 提交副作用监听。实现方自行决定同步/异步执行；
// 监听失败不得影响提交结果（提交已落盘）。
type CommitListener interface {
	OnCommit(ev *CommitEvent)
}

// AddCommitListener 注册提交副作用监听（app 装配期调用，避开服务构造的循环依赖）
func (s *syncV3Service) AddCommitListener(l CommitListener) {
	s.listeners = append(s.listeners, l)
}

// notifyCommit 逐个通知监听；单个监听 panic 不扩散（提交已完成，副作用尽力而为）
func (s *syncV3Service) notifyCommit(ev *CommitEvent) {
	for _, l := range s.listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error("v3 commit listener panic", zap.Any("recover", r))
				}
			}()
			l.OnCommit(ev)
		}()
	}
}

// fillFinalIDs 把 ack 中的最终身份回填进变更列表（add 未带 id 的项以服务器分配结果补齐）
func fillFinalIDs(changes []reconcile.Change, ackItems []dto.V3CommitAckItem) {
	byPath := make(map[string]string, len(ackItems))
	for _, a := range ackItems {
		if a.ID != "" {
			byPath[a.Path] = a.ID
		}
	}
	for i := range changes {
		if changes[i].Item.ID == "" {
			changes[i].Item.ID = byPath[changes[i].Item.Path]
		}
	}
}

// NewSyncV3Service creates SyncV3Service instance
// NewSyncV3Service 创建 SyncV3Service 实例
func NewSyncV3Service(
	fsRepo domain.FsEntryRepository,
	manifest domain.VaultManifestRepository,
	histRepo domain.EntryHistoryRepository,
	blobs domain.BlobStore,
	vaultSvc VaultResolver,
	logger *zap.Logger,
) SyncV3Service {
	return &syncV3Service{
		fsRepo:   fsRepo,
		manifest: manifest,
		histRepo: histRepo,
		blobs:    blobs,
		vaultSvc: vaultSvc,
		logger:   logger,
	}
}

// resolveVault vault 名 → ID（不存在则建）
func (s *syncV3Service) resolveVault(ctx context.Context, uid int64, vaultName string) (*domain.Vault, error) {
	v, err := s.vaultSvc.GetOrCreate(ctx, uid, vaultName)
	if err != nil {
		return nil, code.ErrorV3SyncPlanFailed.WithDetails("vault resolve: " + err.Error())
	}
	return v, nil
}

func manifestItems(m *domain.Manifest) []domain.ManifestItem {
	if m == nil {
		return nil
	}
	return m.Items
}

func manifestEpoch(m *domain.Manifest) int64 {
	if m == nil {
		return 0
	}
	return m.Epoch
}

// SyncPlan 对账入口：B 从服务器按 baseEpoch 解析（不可得则按无基线全量对账），
// S 取当前清单，交给 reconcile 引擎，再按 blob 存在性把 Uploads 折算成 BlobNeed。
func (s *syncV3Service) SyncPlan(ctx context.Context, uid int64, vaultName string, req *dto.V3SyncRequest) (*dto.V3SyncPlanMessage, []dto.V3BlobNeedMessage, error) {
	v, err := s.resolveVault(ctx, uid, vaultName)
	if err != nil {
		return nil, nil, err
	}

	cur, err := s.manifest.Current(ctx, v.ID, uid)
	if err != nil {
		return nil, nil, code.ErrorV3SyncPlanFailed.WithDetails("manifest current: " + err.Error())
	}

	var base []domain.ManifestItem
	if req.BaseEpoch > 0 {
		b, err := s.manifest.GetByEpoch(ctx, req.BaseEpoch, v.ID, uid)
		if err != nil {
			return nil, nil, code.ErrorV3SyncPlanFailed.WithDetails("manifest base: " + err.Error())
		}
		base = manifestItems(b) // b==nil：基线已不可得 → 无基线，全量对账（§2.1 末条）
	}

	plan := reconcile.Reconcile(reconcile.Input{
		Local:      req.Manifest,
		Tombstones: req.Tombstones,
		Base:       base,
		Server:     manifestItems(cur),
		Scope:      req.Scope,
	})

	msg := &dto.V3SyncPlanMessage{
		Vault:       vaultName,
		ServerEpoch: manifestEpoch(cur),
		BaseEpoch:   req.BaseEpoch,
		Ops:         plan.Ops,
		Conflicts:   plan.Conflicts,
		Expected:    plan.Expected,
	}

	needs := make([]dto.V3BlobNeedMessage, 0, len(plan.Uploads))
	seen := map[string]bool{}
	for _, u := range plan.Uploads {
		if seen[u.Hash] || s.blobs.BlobExists(uid, u.Hash) {
			continue // 服务器已有（秒传）
		}
		seen[u.Hash] = true
		needs = append(needs, dto.V3BlobNeedMessage{Vault: vaultName, Path: u.Path, Hash: u.Hash, Size: u.Size})
	}
	// 注意：冲突（Conflicts）不产生 BlobNeed。冲突策略由客户端决定（server-wins/merge/copy），
	// 本地版本若要上推，会在客户端解决后的下一轮作为普通 modify 进入 Expected 并按需上传。

	return msg, needs, nil
}

// Commit 原子提交：预检 blob → CommitOptimistic（乐观锁 + 同事务应用 fs_entry 变更）→ 历史与提示 ops
func (s *syncV3Service) Commit(ctx context.Context, uid int64, vaultName string, req *dto.V3ManifestCommitRequest, client string) (*dto.V3ManifestCommitAckMessage, []reconcile.Op, error) {
	v, err := s.resolveVault(ctx, uid, vaultName)
	if err != nil {
		return nil, nil, err
	}

	// 预检：内容变更引用的 blob 必须已在 blob store（分块上传未完成就提交 → 明确报错而非静默丢内容）
	for _, ch := range req.Changes {
		// 路径必须是合法 UTF-8：清单树经 JSON 序列化落盘，非法字节会被替换成 U+FFFD，
		// 服务端路径从此与客户端原始字节永不相等 → 每轮对账都判为删+增（不收敛振荡）。
		// 明确拒绝而非静默损坏（P6 收敛 fuzz 发现；Linux 允许文件名含任意字节）。
		if !utf8.ValidString(ch.Item.Path) || (ch.OldPath != "" && !utf8.ValidString(ch.OldPath)) {
			return nil, nil, code.ErrorInvalidParams.WithPath(ch.Item.Path).
				WithDetails("path must be valid UTF-8")
		}
		if !changeNeedsBlob(ch) {
			continue
		}
		if !s.blobs.BlobExists(uid, ch.Item.BlobHash) {
			return nil, nil, code.ErrorV3BlobMissing.
				WithPath(ch.Item.Path).
				WithData(dto.V3BlobNeedMessage{Vault: vaultName, Path: ch.Item.Path, Hash: ch.Item.BlobHash, Size: ch.Item.Size})
		}
	}

	// 提交前快照（NotifyManifest 的提示性两方 diff 用；并发提交时可能略旧，可接受——通知只是优化）
	prev, err := s.manifest.Current(ctx, v.ID, uid)
	if err != nil {
		return nil, nil, code.ErrorV3CommitFailed.WithDetails("manifest current: " + err.Error())
	}

	var ackItems []dto.V3CommitAckItem // apply 回填：涉及条目的最终 (path, id)

	newEpoch, err := s.manifest.CommitOptimistic(ctx, v.ID, uid, req.BaseEpoch, func(store domain.FsEntryStore) ([]domain.ManifestItem, error) {
		applied, items, err := applyChanges(store, v.ID, req.Changes, client)
		if err != nil {
			return nil, err
		}
		ackItems = applied
		return items, nil
	})
	if err != nil {
		if errors.Is(err, domain.ErrEpochConflict) {
			cur, _ := s.manifest.Current(ctx, v.ID, uid)
			return nil, nil, code.ErrorV3EpochConflict.WithData(dto.V3EpochConflictData{CurrentEpoch: manifestEpoch(cur)})
		}
		if c, ok := err.(*code.Code); ok {
			return nil, nil, c
		}
		return nil, nil, code.ErrorV3CommitFailed.WithDetails(err.Error())
	}

	// 提交后两方 diff 生成提示 ops（读不到新清单时宁可不发，也不发误导性 ops）
	var notifyOps []reconcile.Op
	if cur, err := s.manifest.GetByEpoch(ctx, newEpoch, v.ID, uid); err == nil && cur != nil {
		notifyOps = diffManifests(manifestItems(prev), cur.Items)
	}
	if notifyOps == nil {
		notifyOps = []reconcile.Op{} // wire 上空集合必须是 []（同下）
	}

	// wire 上空集合必须是 []（nil slice 编成 null，客户端迭代会炸）
	if ackItems == nil {
		ackItems = []dto.V3CommitAckItem{}
	}

	// 提交副作用（FTS/链接/日志/备份/Git/分享撤销）——尽力而为，不影响已落盘的提交
	if len(req.Changes) > 0 {
		fillFinalIDs(req.Changes, ackItems) // add 时客户端不感知 id：副作用监听需要服务器分配的最终身份
		s.notifyCommit(&CommitEvent{
			UID: uid, VaultID: v.ID, Vault: vaultName, NewEpoch: newEpoch,
			Client: client, Changes: req.Changes,
		})
	}

	return &dto.V3ManifestCommitAckMessage{Vault: vaultName, NewEpoch: newEpoch, Items: ackItems}, notifyOps, nil
}

// changeNeedsBlob 该变更是否引用了新内容
func changeNeedsBlob(ch reconcile.Change) bool {
	if ch.Item.BlobHash == "" {
		return false
	}
	return ch.Op == "add" || ch.Op == "modify"
}

// ReadBlobInline 小体积 blob 内联读取（笔记文本）
func (s *syncV3Service) ReadBlobInline(ctx context.Context, uid int64, hash string, limit int64) ([]byte, bool, error) {
	size, ok := s.blobs.BlobSize(uid, hash)
	if !ok {
		return nil, false, code.ErrorV3BlobNotFound
	}
	if size > limit {
		return nil, false, nil
	}
	data, err := s.blobs.BlobReadAll(uid, hash)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

// OpenBlob 打开 blob 读取（分块下载）
func (s *syncV3Service) OpenBlob(ctx context.Context, uid int64, hash string) (io.ReadCloser, int64, error) {
	size, ok := s.blobs.BlobSize(uid, hash)
	if !ok {
		return nil, 0, code.ErrorV3BlobNotFound
	}
	rc, err := s.blobs.BlobOpen(uid, hash)
	if err != nil {
		return nil, 0, err
	}
	return rc, size, nil
}

// FinalizeBlobUpload 校验暂存文件 SHA-256 后移入 blob store
func (s *syncV3Service) FinalizeBlobUpload(ctx context.Context, uid int64, expectedHash, tempPath string) error {
	if err := s.blobs.BlobStoreFromTemp(uid, tempPath, expectedHash); err != nil {
		if strings.Contains(err.Error(), "mismatch") {
			return code.ErrorV3BlobHashInvalid.WithDetails(err.Error())
		}
		return code.ErrorV3CommitFailed.WithDetails(err.Error())
	}
	return nil
}

// BlobExists 判断 blob 是否已存在（秒传）
func (s *syncV3Service) BlobExists(uid int64, hash string) bool {
	return s.blobs.BlobExists(uid, hash)
}

// ==================== 提交应用 ====================

// conflictPath 冲突副本路径：a/b.md → a/b.conflict.{id}.md（保留扩展名）
func conflictPath(path, id string) string {
	ext := filepath.Ext(path)
	stem := strings.TrimSuffix(path, ext)
	return stem + ".conflict." + id + ext
}

// applyChanges 在事务内把客户端变更落到 fs_entry，返回涉及条目的最终 (path, id)（ack 用）
// 与提交后的新快照（省去末尾再全表扫一次）。
// 幂等：重复提交同内容不产生额外效果；条目缺失时 add/modify 落为新建、delete/move 容忍缺失。
//
// 性能：万级条目提交（首灌）逐条 SELECT/INSERT 会让事务跑几十秒（E2E 12.8K 条实测 >30s），
// 改为「开始时一次全量快照 → 内存索引判定 → 批量落盘」：
//   - ListAll 一次读全（含墓碑——复活判定需要按 id 找到已删行）
//   - 纯新增（无冲突占用）走缓冲，收尾 CreateBatch/AppendHistoryBatch 批量插入
//   - 更新/删除/移动/复活直写（批量场景中占比小），写前先冲刷缓冲，
//     使其后所有判定与逐条执行的原语义完全一致
func applyChanges(store domain.FsEntryStore, vaultID int64, changes []reconcile.Change, client string) ([]dto.V3CommitAckItem, []domain.ManifestItem, error) {
	all, err := store.ListAll(vaultID)
	if err != nil {
		return nil, nil, err
	}
	cw := &changeWriter{
		store: store, vaultID: vaultID, client: client,
		byID:   make(map[string]*domain.FsEntry, len(all)),
		byPath: make(map[string]*domain.FsEntry, len(all)),
	}
	for _, e := range all {
		cw.byID[e.ID] = e
		if e.Deleted {
			continue
		}
		if prev, dup := cw.byPath[e.Path]; dup {
			// 自愈同名双活行（只可能来自旧版缺陷落库）：快照按路径只取其一，
			// 落选行从此改不动也不可见，持有其 id 的客户端每轮重报同一变更 →
			// 永不收敛。按 (Mtime, ID) 确定性保留一行，落选行转墓碑（可从回收站恢复）。
			keep, drop := pickDupSurvivor(prev, e)
			if err := store.MarkDeleted(drop.ID); err != nil {
				return nil, nil, err
			}
			drop.Deleted = true
			cw.byPath[e.Path] = keep
			continue
		}
		cw.byPath[e.Path] = e
	}
	for _, ch := range changes {
		// 身份申报消毒：add/modify 携带的 id 若指向“另一路径上的条目”（活跃或墓碑），
		// 说明客户端的 id→路径映射已过期（该 id 从未在申报路径落位）。服务器是身份
		// 权威：丢弃申报 id，按路径现占条目或全新条目处理，ack 回传真实落位纠正客户端。
		// 否则按 id 命中他路径条目 → 内容写错对象（活跃）或墓碑在他路径复活（删除），
		// 两种都伴随 ack 谎报路径，客户端从此持有双路径同 id 的矛盾状态，形成
		// “add/modify 反复提交同一变更”的振荡（P6 收敛 fuzz 发现）。
		// 同路径墓碑命中不受影响——那才是真正的“复活”语义。
		if ch.Op == "add" || ch.Op == "modify" {
			if e := cw.resolve(ch.Item.ID, ch.Item.Path); e != nil && e.Path != ch.Item.Path {
				ch.Item.ID = ""
			}
		}
		if err := cw.apply(ch); err != nil {
			return nil, nil, err
		}
	}
	if err := cw.flush(); err != nil {
		return nil, nil, err
	}
	return cw.applied, cw.snapshotItems(), nil
}

// changeWriter 大清单提交的内存索引 + 批量缓冲。
// byID 含墓碑（复活判定），byPath 仅活条目；所有写操作同步维护索引，
// 后续变更的判定等价于逐条执行时数据库的真实状态。
type changeWriter struct {
	store          domain.FsEntryStore
	vaultID        int64
	client         string
	byID           map[string]*domain.FsEntry
	byPath         map[string]*domain.FsEntry
	pendingCreates []*domain.FsEntry
	pendingHistory []domain.HistoryAppend
	applied        []dto.V3CommitAckItem
}

// pickDupSurvivor 同路径双活行的确定性去重：保留 (Mtime, ID) 最大者，落选者转墓碑。
// 与迭代顺序无关，任意顺序加载结果一致。
func pickDupSurvivor(a, b *domain.FsEntry) (keep, drop *domain.FsEntry) {
	if a.Mtime != b.Mtime {
		if a.Mtime > b.Mtime {
			return a, b
		}
		return b, a
	}
	if a.ID > b.ID {
		return a, b
	}
	return b, a
}

// resolve 与原 resolveEntry 同语义：id 优先（含墓碑），路径其次（仅活条目）
func (cw *changeWriter) resolve(id, path string) *domain.FsEntry {
	if id != "" {
		if e, ok := cw.byID[id]; ok {
			return e
		}
	}
	if path != "" {
		return cw.byPath[path]
	}
	return nil
}

// flush 把缓冲中的新建/历史批量落盘；之后所有条目都是真实行，逐条语义原样可用
func (cw *changeWriter) flush() error {
	if err := cw.store.CreateBatch(cw.pendingCreates); err != nil {
		return err
	}
	if err := cw.store.AppendHistoryBatch(cw.pendingHistory); err != nil {
		return err
	}
	cw.pendingCreates = nil
	cw.pendingHistory = nil
	return nil
}

// snapshotItems 提交后的新快照（与 ListLive 的 path ASC 序一致）
func (cw *changeWriter) snapshotItems() []domain.ManifestItem {
	items := make([]domain.ManifestItem, 0, len(cw.byPath))
	for _, e := range cw.byPath {
		items = append(items, e.ToManifestItem())
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Path < items[j].Path })
	return items
}

// bufferCreate 纯新增走缓冲（路径空闲且无同 id 行）
func (cw *changeWriter) bufferCreate(it domain.ManifestItem) string {
	id := it.ID
	if id == "" {
		id = uuid.New().String() // 服务器分配终身 UUID
	}
	e := &domain.FsEntry{
		ID: id, VaultID: cw.vaultID, IsNote: it.IsNote, Path: it.Path,
		BlobHash: it.BlobHash, Size: it.Size, Ctime: it.Ctime, Mtime: it.Mtime,
		ClientName: cw.client,
	}
	cw.byID[id] = e
	cw.byPath[it.Path] = e
	cw.pendingCreates = append(cw.pendingCreates, e)
	cw.pendingHistory = append(cw.pendingHistory, domain.HistoryAppend{
		EntryID: id, BlobHash: it.BlobHash, Size: it.Size, Client: cw.client,
	})
	cw.applied = append(cw.applied, dto.V3CommitAckItem{Path: it.Path, ID: id})
	return id
}

// moveAwayConflict 目标路径被其他条目占用时，把占用者挪到冲突副本路径（双方各留一份，§2.1）。
// DB 行与索引同步更新。
func (cw *changeWriter) moveAwayConflict(path, keepID string) error {
	e := cw.byPath[path]
	if e == nil || e.ID == keepID {
		return nil
	}
	newPath := conflictPath(path, e.ID)
	if err := cw.store.MovePath(e.ID, newPath, e.Mtime); err != nil {
		return err
	}
	delete(cw.byPath, path)
	e.Path = newPath
	cw.byPath[newPath] = e
	return nil
}

func (cw *changeWriter) apply(ch reconcile.Change) error {
	it := ch.Item
	switch ch.Op {
	case "add", "modify":
		e := cw.resolve(it.ID, it.Path)
		if e == nil && cw.byPath[it.Path] == nil {
			// 快路径：路径空闲且无同 id 行 → 批量缓冲（首灌即全走此路）
			cw.bufferCreate(it)
			return nil
		}
		if e != nil && e.Deleted && it.ID != "" && e.ID == it.ID {
			// 墓碑复活：同 id 条目重新上报（新建同 id 行会撞主键）。
			// 复活前须让出现占者：墓碑停留期间他端可能已在同路径新建条目，
			// 直接把复活行放回 byPath 会让占用者的 DB 行变成“同名双活行”——
			// 快照只取其一，另一行永远改不动 → 客户端对账振荡（P6 fuzz seed=188）。
			if err := cw.flush(); err != nil {
				return err
			}
			if err := cw.store.Restore(e.ID); err != nil {
				return err
			}
			e.Deleted = false
			if err := cw.moveAwayConflict(e.Path, e.ID); err != nil {
				return err
			}
			cw.byPath[e.Path] = e
		}
		if e != nil && !e.Deleted {
			if err := cw.flush(); err != nil {
				return err
			}
			if err := cw.store.UpdateContent(e.ID, it.BlobHash, it.Size, it.Mtime, cw.client); err != nil {
				return err
			}
			if e.BlobHash != it.BlobHash {
				cw.pendingHistory = append(cw.pendingHistory, domain.HistoryAppend{
					EntryID: e.ID, BlobHash: it.BlobHash, Size: it.Size, Client: cw.client,
				})
			}
			e.BlobHash, e.Size, e.Mtime = it.BlobHash, it.Size, it.Mtime
			cw.applied = append(cw.applied, dto.V3CommitAckItem{Path: it.Path, ID: e.ID})
			return nil
		}
		if err := cw.flush(); err != nil {
			return err
		}
		if err := cw.moveAwayConflict(it.Path, it.ID); err != nil {
			return err
		}
		cw.bufferCreate(it)
		return nil

	case "delete":
		e := cw.resolve(it.ID, it.Path)
		if e == nil || e.Deleted {
			return nil // 幂等：已删/不存在
		}
		if err := cw.flush(); err != nil { // 待插行先落盘，墓碑语义与逐条执行一致
			return err
		}
		if err := cw.store.MarkDeleted(e.ID); err != nil {
			return err
		}
		e.Deleted = true
		delete(cw.byPath, e.Path)
		cw.applied = append(cw.applied, dto.V3CommitAckItem{Path: it.Path, ID: e.ID})
		return nil

	case "move":
		e := cw.resolve(it.ID, ch.OldPath)
		if e == nil {
			// 原条目已不存在（被物理清除）：按新建落位，身份按客户端申报。
			// 目标路径可能已被他端条目占用（推断式 move 拿不到原 id，resolve 按
			// OldPath 也常落空）——不让占用者挪走就会造出同名双活行
			if err := cw.flush(); err != nil {
				return err
			}
			if err := cw.moveAwayConflict(it.Path, it.ID); err != nil {
				return err
			}
			cw.bufferCreate(it)
			return nil
		}
		if err := cw.flush(); err != nil {
			return err
		}
		if e.Deleted {
			// 墓碑状态下的 move：复活到新路径
			if err := cw.store.Restore(e.ID); err != nil {
				return err
			}
			e.Deleted = false
		}
		if err := cw.moveAwayConflict(it.Path, e.ID); err != nil {
			return err
		}
		if err := cw.store.MovePath(e.ID, it.Path, it.Mtime); err != nil {
			return err
		}
		delete(cw.byPath, e.Path)
		e.Path = it.Path
		cw.byPath[it.Path] = e
		if it.BlobHash != "" && it.BlobHash != e.BlobHash {
			if err := cw.store.UpdateContent(e.ID, it.BlobHash, it.Size, it.Mtime, cw.client); err != nil {
				return err
			}
			cw.pendingHistory = append(cw.pendingHistory, domain.HistoryAppend{
				EntryID: e.ID, BlobHash: it.BlobHash, Size: it.Size, Client: cw.client,
			})
			e.BlobHash, e.Size = it.BlobHash, it.Size
		}
		e.Mtime = it.Mtime
		cw.applied = append(cw.applied, dto.V3CommitAckItem{Path: it.Path, ID: e.ID})
		return nil

	default:
		return fmt.Errorf("unknown change op %q", ch.Op)
	}
}

// ==================== 两方 diff（NotifyManifest 提示用） ====================

// diffManifests prev → next 的两方 diff，产出提示性 ops（新增/变更→pull，消失→delete，同 id 换路径→move）。
// 只用于实时通知的优化展示，不参与正确性；客户端以重新对账为准。
func diffManifests(prev, next []domain.ManifestItem) []reconcile.Op {
	if len(prev) == 0 && len(next) == 0 {
		return nil
	}
	prevByPath := make(map[string]domain.ManifestItem, len(prev))
	prevByID := make(map[string]domain.ManifestItem, len(prev))
	for _, it := range prev {
		prevByPath[it.Path] = it
		if it.ID != "" {
			prevByID[it.ID] = it
		}
	}
	nextByPath := make(map[string]domain.ManifestItem, len(next))
	for _, it := range next {
		nextByPath[it.Path] = it
	}

	var ops []reconcile.Op
	moved := map[string]bool{} // prev 中已判 move 的路径
	for _, it := range next {
		p, hasP := prevByPath[it.Path]
		if !hasP {
			if old, ok := prevByID[it.ID]; ok && old.Path != it.Path {
				ops = append(ops, reconcile.Op{Kind: reconcile.OpMove, Item: it, From: old.Path})
				moved[old.Path] = true
				continue
			}
			ops = append(ops, reconcile.Op{Kind: reconcile.OpPull, Item: it})
			continue
		}
		if p.BlobHash != it.BlobHash {
			ops = append(ops, reconcile.Op{Kind: reconcile.OpPull, Item: it})
		}
	}
	for _, it := range prev {
		if _, still := nextByPath[it.Path]; !still && !moved[it.Path] {
			ops = append(ops, reconcile.Op{Kind: reconcile.OpDelete, Item: it})
		}
	}
	return ops
}
