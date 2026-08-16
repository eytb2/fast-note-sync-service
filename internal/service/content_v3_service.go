// Package service: ContentV3Service —— REST/MCP/Web 等「服务器侧内容读写」的 v3 门面。
// 读走 fs_entry/manifest + blob store；写走 SyncV3Service.Commit 管线，
// 使服务器侧写入与客户端提交同构：epoch 推进、乐观锁重试、NotifyManifest 广播、
// 提交副作用监听（FTS/链接/日志/备份/Git/分享撤销）一应俱全。
// 规则见 git-sync-redesign.md §8（P5 功能回接）。
package service

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"go.uber.org/zap"
)

// ManifestBroadcaster NotifyManifest 广播出口（app 装配期接 WS 服务器）。
// REST/MCP 写入经此实时分发给在线 v3 客户端（纯优化，与 WS 提交路径的广播同构）。
type ManifestBroadcaster interface {
	BroadcastManifest(uid int64, msg *dto.V3NotifyManifestMessage)
}

// ContentV3Service 服务器侧内容读写门面（REST/MCP/Web 共用）
type ContentV3Service interface {
	// GetEntryByPath 精确路径取条目（不存在返回 ErrEntryNotFound 语义的 code）
	GetEntryByPath(ctx context.Context, uid int64, vaultName, path string) (*domain.FsEntry, error)
	// ListEntries 前缀列举（树/目录内容）；isNote 双态过滤；afterPath 起翻页；limit<=0 用默认
	ListEntries(ctx context.Context, uid int64, vaultName, prefix string, isNote *bool, afterPath string, limit int) ([]*domain.FsEntry, error)
	// ReadEntry 取条目 + 全量内容（小文件/笔记）
	ReadEntry(ctx context.Context, uid int64, vaultName, path string) (*domain.FsEntry, []byte, error)
	// ReadEntryBlob 按 hash 直读 blob（回收站元数据/历史内容）
	ReadEntryBlob(uid int64, blobHash string) ([]byte, error)
	// OpenEntry 流式打开内容（大附件下载）
	OpenEntry(ctx context.Context, uid int64, vaultName, path string) (io.ReadCloser, *domain.FsEntry, error)
	// HistoryByPath 条目版本历史（entry_history，按时间倒序）
	HistoryByPath(ctx context.Context, uid int64, vaultName, path string) (*domain.FsEntry, []domain.EntryHistoryItem, error)
	// RestoreFromHash 把历史版本内容写回当前（恢复 = 对旧 hash 提交 modify）
	RestoreFromHash(ctx context.Context, uid int64, vaultName, path, hash, client string) (*domain.FsEntry, error)
	// CurrentManifest 当前快照（Web 树渲染/备份/Git 导出的数据源）
	CurrentManifest(ctx context.Context, uid int64, vaultName string) (*domain.Manifest, error)

	// Write 写入/新建（内容寻址落 blob → 提交 add|modify）。
	// 返回条目最终状态；同路径存在即 modify（保身份），否则 add（服务器分配 UUID）。
	Write(ctx context.Context, uid int64, vaultName, path string, content []byte, isNote bool, client string) (*domain.FsEntry, error)
	// Delete 删除（墓碑）——分享撤销由提交副作用监听统一处理
	Delete(ctx context.Context, uid int64, vaultName, path, client string) error
	// RestoreFromTombstone 回收站恢复：墓碑按原 id 复活（add-with-ID 走复活分支，历史延续）
	RestoreFromTombstone(ctx context.Context, uid int64, vaultName, path, client string) (*domain.FsEntry, error)
	// RestoreBatch 批量回收站恢复（按路径）：逐条走 RestoreFromTombstone 语义，
	// 单条失败不影响其余；返回成功路径与失败明细（path → 错误文本）。
	RestoreBatch(ctx context.Context, uid int64, vaultName string, paths []string, client string) (okPaths []string, failed map[string]string)
	// Move 移动/重命名（id 不变，历史不断链）
	Move(ctx context.Context, uid int64, vaultName, oldPath, newPath, client string) (*domain.FsEntry, error)
	// ApplyChanges 批量提交复合变更（子树移动/删除；单条与 Move/Delete 语义一致）
	ApplyChanges(ctx context.Context, uid int64, vaultName string, changes []reconcile.Change, client string) error
}

type contentV3Service struct {
	fsRepo      domain.FsEntryRepository
	manifest    domain.VaultManifestRepository
	histRepo    domain.EntryHistoryRepository
	blobs       domain.BlobStore
	vaultSvc    VaultResolver
	sync        SyncV3Service
	broadcaster ManifestBroadcaster
	logger      *zap.Logger
}

// NewContentV3Service 创建门面实例；broadcaster 可为 nil（无 WS 上下文的测试环境）
func NewContentV3Service(
	fsRepo domain.FsEntryRepository,
	manifest domain.VaultManifestRepository,
	histRepo domain.EntryHistoryRepository,
	blobs domain.BlobStore,
	vaultSvc VaultResolver,
	sync SyncV3Service,
	broadcaster ManifestBroadcaster,
	logger *zap.Logger,
) ContentV3Service {
	return &contentV3Service{
		fsRepo:      fsRepo,
		manifest:    manifest,
		histRepo:    histRepo,
		blobs:       blobs,
		vaultSvc:    vaultSvc,
		sync:        sync,
		broadcaster: broadcaster,
		logger:      logger,
	}
}

// ==================== 读 ====================

func (s *contentV3Service) resolveVault(ctx context.Context, uid int64, vaultName string) (*domain.Vault, error) {
	v, err := s.vaultSvc.GetOrCreate(ctx, uid, vaultName)
	if err != nil {
		return nil, code.ErrorV3SyncPlanFailed.WithDetails("vault resolve: " + err.Error())
	}
	return v, nil
}

func (s *contentV3Service) GetEntryByPath(ctx context.Context, uid int64, vaultName, path string) (*domain.FsEntry, error) {
	v, err := s.resolveVault(ctx, uid, vaultName)
	if err != nil {
		return nil, err
	}
	e, err := s.fsRepo.GetLiveByPath(ctx, path, v.ID, uid)
	if err != nil {
		if errors.Is(err, domain.ErrEntryNotFound) {
			return nil, code.ErrorV3EntryNotFound.WithPath(path)
		}
		return nil, err
	}
	return e, nil
}

func (s *contentV3Service) ListEntries(ctx context.Context, uid int64, vaultName, prefix string, isNote *bool, afterPath string, limit int) ([]*domain.FsEntry, error) {
	v, err := s.resolveVault(ctx, uid, vaultName)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	return s.fsRepo.ListLiveByPrefix(ctx, v.ID, prefix, isNote, afterPath, limit, uid)
}

func (s *contentV3Service) ReadEntry(ctx context.Context, uid int64, vaultName, path string) (*domain.FsEntry, []byte, error) {
	e, err := s.GetEntryByPath(ctx, uid, vaultName, path)
	if err != nil {
		return nil, nil, err
	}
	data, err := s.blobs.BlobReadAll(uid, e.BlobHash)
	if err != nil {
		return e, nil, code.ErrorV3BlobNotFound.WithDetails(err.Error())
	}
	return e, data, nil
}

func (s *contentV3Service) OpenEntry(ctx context.Context, uid int64, vaultName, path string) (io.ReadCloser, *domain.FsEntry, error) {
	e, err := s.GetEntryByPath(ctx, uid, vaultName, path)
	if err != nil {
		return nil, nil, err
	}
	rc, err := s.blobs.BlobOpen(uid, e.BlobHash)
	if err != nil {
		return nil, e, code.ErrorV3BlobNotFound.WithDetails(err.Error())
	}
	return rc, e, nil
}

func (s *contentV3Service) ReadEntryBlob(uid int64, blobHash string) ([]byte, error) {
	data, err := s.blobs.BlobReadAll(uid, blobHash)
	if err != nil {
		return nil, code.ErrorV3BlobNotFound.WithDetails(err.Error())
	}
	return data, nil
}

func (s *contentV3Service) HistoryByPath(ctx context.Context, uid int64, vaultName, path string) (*domain.FsEntry, []domain.EntryHistoryItem, error) {
	e, err := s.GetEntryByPath(ctx, uid, vaultName, path)
	if err != nil {
		return nil, nil, err
	}
	items, err := s.histRepo.ListByEntry(ctx, e.ID, uid)
	if err != nil {
		return e, nil, err
	}
	return e, items, nil
}

func (s *contentV3Service) RestoreFromHash(ctx context.Context, uid int64, vaultName, path, hash, client string) (*domain.FsEntry, error) {
	e, err := s.GetEntryByPath(ctx, uid, vaultName, path)
	if err != nil {
		return nil, err
	}
	if !s.blobs.BlobExists(uid, hash) {
		return nil, code.ErrorV3BlobNotFound.WithDetails("history blob missing: " + hash)
	}
	if err := s.commit(ctx, uid, vaultName, []reconcile.Change{{
		Op: "modify",
		Item: domain.ManifestItem{
			ID: e.ID, Path: e.Path, BlobHash: hash, IsNote: e.IsNote,
			Size: 0, Mtime: nowMillis(), Ctime: e.Ctime,
		},
	}}, client); err != nil {
		return nil, err
	}
	return s.GetEntryByPath(ctx, uid, vaultName, path)
}

func (s *contentV3Service) CurrentManifest(ctx context.Context, uid int64, vaultName string) (*domain.Manifest, error) {
	v, err := s.resolveVault(ctx, uid, vaultName)
	if err != nil {
		return nil, err
	}
	return s.manifest.Current(ctx, v.ID, uid)
}

// ==================== 写 ====================

func (s *contentV3Service) Write(ctx context.Context, uid int64, vaultName, path string, content []byte, isNote bool, client string) (*domain.FsEntry, error) {
	v, err := s.resolveVault(ctx, uid, vaultName)
	if err != nil {
		return nil, err
	}
	hash, err := s.blobs.BlobStoreFromBytes(uid, content)
	if err != nil {
		return nil, code.ErrorV3CommitFailed.WithDetails("blob store: " + err.Error())
	}
	mtime := nowMillis()
	existing, err := s.fsRepo.GetLiveByPath(ctx, path, v.ID, uid)
	if err != nil && !errors.Is(err, domain.ErrEntryNotFound) {
		return nil, err
	}
	op := "add"
	item := domain.ManifestItem{Path: path, BlobHash: hash, IsNote: isNote, Size: int64(len(content)), Mtime: mtime, Ctime: mtime}
	if existing != nil {
		op = "modify"
		item.ID = existing.ID
		item.Ctime = existing.Ctime // 身份延续
		item.IsNote = existing.IsNote
	}
	if err := s.commit(ctx, uid, vaultName, []reconcile.Change{{Op: op, Item: item}}, client); err != nil {
		return nil, err
	}
	out, err := s.fsRepo.GetLiveByPath(ctx, path, v.ID, uid)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *contentV3Service) Delete(ctx context.Context, uid int64, vaultName, path, client string) error {
	e, err := s.GetEntryByPath(ctx, uid, vaultName, path)
	if err != nil {
		return err
	}
	return s.commit(ctx, uid, vaultName, []reconcile.Change{{
		Op:   "delete",
		Item: domain.ManifestItem{ID: e.ID, Path: e.Path, IsNote: e.IsNote, BlobHash: e.BlobHash, Size: e.Size, Mtime: nowMillis(), Ctime: e.Ctime},
	}}, client)
}

// RestoreFromTombstone 回收站恢复：按路径在墓碑中定位，提交 add-with-ID（复活分支）。
// 墓碑缺失或已是活跃条目时返回 548。
func (s *contentV3Service) RestoreFromTombstone(ctx context.Context, uid int64, vaultName, path, client string) (*domain.FsEntry, error) {
	v, err := s.resolveVault(ctx, uid, vaultName)
	if err != nil {
		return nil, err
	}
	// 已是活跃条目：直接返回（幂等恢复）
	if live, err := s.fsRepo.GetLiveByPath(ctx, path, v.ID, uid); err == nil && live != nil {
		return live, nil
	}
	tombs, err := s.fsRepo.ListDeleted(ctx, v.ID, uid)
	if err != nil {
		return nil, err
	}
	var tomb *domain.FsEntry
	for _, t := range tombs {
		if t.Path == path {
			tomb = t
			break
		}
	}
	if tomb == nil {
		return nil, code.ErrorV3EntryNotFound.WithPath(path)
	}
	if err := s.commit(ctx, uid, vaultName, []reconcile.Change{{
		Op: "add", // 同 id 重新上报 → changeWriter 复活分支（Restore + UpdateContent）
		Item: domain.ManifestItem{
			ID: tomb.ID, Path: tomb.Path, BlobHash: tomb.BlobHash, IsNote: tomb.IsNote,
			Size: tomb.Size, Mtime: nowMillis(), Ctime: tomb.Ctime,
		},
	}}, client); err != nil {
		return nil, err
	}
	return s.fsRepo.GetLiveByPath(ctx, path, v.ID, uid)
}

// RestoreBatch 批量回收站恢复：逐条独立恢复（幂等），单条失败（墓碑缺失/路径
// 已被新文件占用/存储故障）只记录不中断。每条成功恢复都走完整提交管线
// （epoch 推进 + 广播 + 副作用），客户端下一轮对账自动拉回。
// WebGUI「一键恢复」的后端；21 文件级事故恢复从逐条操作变成一次调用。
func (s *contentV3Service) RestoreBatch(ctx context.Context, uid int64, vaultName string, paths []string, client string) ([]string, map[string]string) {
	ok := make([]string, 0, len(paths))
	failed := make(map[string]string)
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, err := s.RestoreFromTombstone(ctx, uid, vaultName, p, client); err != nil {
			failed[p] = err.Error()
			continue
		}
		ok = append(ok, p)
	}
	return ok, failed
}

func (s *contentV3Service) Move(ctx context.Context, uid int64, vaultName, oldPath, newPath, client string) (*domain.FsEntry, error) {
	e, err := s.GetEntryByPath(ctx, uid, vaultName, oldPath)
	if err != nil {
		return nil, err
	}
	if err := s.commit(ctx, uid, vaultName, []reconcile.Change{{
		Op:      "move",
		OldPath: oldPath,
		Item: domain.ManifestItem{
			ID: e.ID, Path: newPath, BlobHash: e.BlobHash, IsNote: e.IsNote,
			Size: e.Size, Mtime: nowMillis(), Ctime: e.Ctime,
		},
	}}, client); err != nil {
		return nil, err
	}
	v, err := s.resolveVault(ctx, uid, vaultName)
	if err != nil {
		return nil, err
	}
	return s.fsRepo.GetLiveByPath(ctx, newPath, v.ID, uid)
}

// ApplyChanges 批量提交（子树级复合操作：目录移动/删除）。广播与副作用与单条写入一致。
func (s *contentV3Service) ApplyChanges(ctx context.Context, uid int64, vaultName string, changes []reconcile.Change, client string) error {
	if len(changes) == 0 {
		return nil
	}
	return s.commit(ctx, uid, vaultName, changes, client)
}

// commit 走 v3 提交管线（乐观锁 + 广播 + 副作用）。服务器侧写入与并发客户端提交竞争
// epoch：542 时重读当前 epoch 重试（净变更幂等，重试安全）。
func (s *contentV3Service) commit(ctx context.Context, uid int64, vaultName string, changes []reconcile.Change, client string) error {
	const retries = 5
	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		v, err := s.resolveVault(ctx, uid, vaultName)
		if err != nil {
			return err
		}
		cur, err := s.manifest.Current(ctx, v.ID, uid)
		if err != nil {
			return code.ErrorV3CommitFailed.WithDetails("manifest current: " + err.Error())
		}
		base := int64(0)
		if cur != nil {
			base = cur.Epoch
		}
		ack, _, err := s.sync.Commit(ctx, uid, vaultName, &dto.V3ManifestCommitRequest{
			Vault: vaultName, BaseEpoch: base, Changes: changes,
		}, client)
		if err != nil {
			if c, ok := err.(*code.Code); ok && c.Code() == code.ErrorV3EpochConflict.Code() {
				lastErr = err
				continue // 服务器被并发提交推进：重读 epoch 再试
			}
			return err
		}
		s.broadcast(uid, vaultName, ack.NewEpoch, changesToNotifyOps(changes))
		return nil
	}
	return lastErr
}

// changesToNotifyOps 提示性 ops：add/modify→pull、delete→delete、move→move
func changesToNotifyOps(changes []reconcile.Change) []reconcile.Op {
	ops := make([]reconcile.Op, 0, len(changes))
	for _, ch := range changes {
		kind := reconcile.OpKind(ch.Op)
		if ch.Op == "add" || ch.Op == "modify" {
			kind = reconcile.OpPull // 客户端语义：这条路径有新内容可拉
		}
		ops = append(ops, reconcile.Op{Kind: kind, From: ch.OldPath, Item: ch.Item})
	}
	return ops
}

func (s *contentV3Service) broadcast(uid int64, vault string, newEpoch int64, ops []reconcile.Op) {
	if s.broadcaster == nil || len(ops) == 0 {
		return
	}
	s.broadcaster.BroadcastManifest(uid, &dto.V3NotifyManifestMessage{Vault: vault, NewEpoch: newEpoch, Ops: ops})
}

func nowMillis() int64 { return time.Now().UnixMilli() }
