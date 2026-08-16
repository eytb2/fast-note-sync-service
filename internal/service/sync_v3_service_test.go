// Package service: WS v3 双客户端收敛矩阵测试（P2 验收）。
// 用两个内存模拟客户端走真实服务路径：SyncPlan → 上传 BlobNeed → 应用 Ops/冲突（server-wins）
// → Commit（409 自动重试），断言多端与服务器清单最终一致。
// 场景覆盖 git-sync-redesign.md §5 P2 行：move/delete/冲突/并发提交/离线重连。
package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// fakeVaultResolver v3 服务只依赖 vault 名 → ID 解析
type fakeVaultResolver struct{}

func (fakeVaultResolver) GetOrCreate(_ context.Context, uid int64, name string) (*domain.Vault, error) {
	return &domain.Vault{ID: 1, UID: uid, Name: name}, nil
}

// setupV3TestEnv 搭建 SQLite 临时工作区与真实 v3 服务栈
func setupV3TestEnv(t *testing.T) (svc SyncV3Service, d *dao.Dao, fsRepo domain.FsEntryRepository, manifestRepo domain.VaultManifestRepository, cleanup func()) {
	tempDir, err := os.MkdirTemp("", "fns-v3-test-*")
	require.NoError(t, err)

	origWd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(tempDir))
	require.NoError(t, os.MkdirAll(filepath.Join("storage", "database"), 0755))

	dbPath := filepath.Join("storage", "database", "db.sqlite3")
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	require.NoError(t, err)

	dbCfg := &config.DatabaseConfig{
		Type:             "sqlite",
		Path:             dbPath,
		EnableWriteQueue: util.Ptr(false),
	}
	d = dao.New(db, context.Background(),
		dao.WithConfig(dbCfg),
		dao.WithUserDatabaseConfig(dbCfg),
		dao.WithLogger(zap.NewNop()),
	)

	fsRepo = dao.NewFsEntryRepository(d)
	manifestRepo = dao.NewVaultManifestRepository(d)
	svc = NewSyncV3Service(fsRepo, manifestRepo, dao.NewEntryHistoryRepository(d), d, fakeVaultResolver{}, zap.NewNop())

	cleanup = func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
		_ = os.Chdir(origWd)
		_ = os.RemoveAll(tempDir)
	}
	return
}

// ==================== 模拟客户端 ====================

const simUID int64 = 1
const simVault = "test-vault"

// simClient 内存本地树 + 基线，模拟真实插件的 baseline_store 行为
type simClient struct {
	name      string
	svc       SyncV3Service
	blobs     *dao.Dao // 传输层替身：客户端上传/下载直接读写 blob store
	files     map[string]string
	ids       map[string]string
	tombs     map[string]bool
	baseEpoch int64
	// syncedContent 最近一次成功同步后的内容快照（fuzz 谱系记录用：
	// 只有“内容与服务器一致时发起的改名”才可断言哈希推断保 id）
	syncedContent map[string]string
	lastPlan      *dto.V3SyncPlanMessage // 诊断：最近一轮计划（convergeN 失败时打印）
}

func newSimClient(name string, svc SyncV3Service, d *dao.Dao) *simClient {
	return &simClient{
		name:  name,
		svc:   svc,
		blobs: d,
		files: map[string]string{},
		ids:   map[string]string{},
		tombs: map[string]bool{}, syncedContent: map[string]string{},
	}
}

func (c *simClient) write(path, content string) { c.files[path] = content }

// snapshotSynced 记录“本地树 == 服务器@baseEpoch”的内容视图（提交成功或纯拉平后）
func (c *simClient) snapshotSynced() {
	c.syncedContent = make(map[string]string, len(c.files))
	for p, v := range c.files {
		c.syncedContent[p] = v
	}
}

func (c *simClient) remove(path string) {
	delete(c.files, path)
	delete(c.ids, path)
}

func (c *simClient) rename(from, to string) {
	c.files[to] = c.files[from]
	if id, ok := c.ids[from]; ok {
		c.ids[to] = id
	}
	delete(c.files, from)
	delete(c.ids, from)
}

func (c *simClient) manifest() []domain.ManifestItem {
	paths := make([]string, 0, len(c.files))
	for p := range c.files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	items := make([]domain.ManifestItem, 0, len(paths))
	for _, p := range paths {
		content := c.files[p]
		items = append(items, domain.ManifestItem{
			ID: c.ids[p], Path: p, BlobHash: util.SHA256Bytes([]byte(content)),
			IsNote: strings.HasSuffix(p, ".md"),
			Size:   int64(len(content)), Mtime: 1, Ctime: 1,
		})
	}
	return items
}

func (c *simClient) tombstones() []reconcile.Tombstone {
	out := make([]reconcile.Tombstone, 0, len(c.tombs))
	for p := range c.tombs {
		out = append(out, reconcile.Tombstone{Path: p})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func (c *simClient) readBlob(t *testing.T, hash string) string {
	data, err := c.blobs.BlobReadAll(simUID, hash)
	require.NoError(t, err)
	return string(data)
}

// syncRound 一轮对账：拉平（Ops/冲突）→ 上传 → 提交。返回本轮是否有动作（未静止）。
func (c *simClient) syncRound(t *testing.T, ctx context.Context) bool {
	plan, needs, err := c.svc.SyncPlan(ctx, simUID, simVault, &dto.V3SyncRequest{
		Vault: simVault, BaseEpoch: c.baseEpoch,
		Manifest: c.manifest(), Tombstones: c.tombstones(),
	})
	require.NoError(t, err)
	c.lastPlan = plan // 诊断用：收敛失败时打印各端计划
	busy := len(needs) > 0 || len(plan.Ops) > 0 || len(plan.Conflicts) > 0 || len(plan.Expected) > 0

	// 1) 上传服务器缺的 blob（走 BlobNeed 清单）
	c.upload(t, needs)

	// 2) 应用服务器操作（move 必须先于本端提交，防止路径抖动）
	for _, op := range plan.Ops {
		switch op.Kind {
		case reconcile.OpPull:
			c.files[op.Item.Path] = c.readBlob(t, op.Item.BlobHash)
			c.ids[op.Item.Path] = op.Item.ID
			delete(c.tombs, op.Item.Path)
		case reconcile.OpMove:
			c.rename(op.From, op.Item.Path)
			if op.Item.ID != "" {
				c.ids[op.Item.Path] = op.Item.ID
			}
		case reconcile.OpDelete:
			c.remove(op.Item.Path)
			c.tombs[op.Item.Path] = true
		default:
			t.Fatalf("未知 op %s", op.Kind)
		}
	}

	// 3) 冲突：server-wins 档（沿用四档策略之一）
	for _, cf := range plan.Conflicts {
		c.files[cf.Path] = c.readBlob(t, cf.ServerHash)
		if cf.ID != "" {
			c.ids[cf.Path] = cf.ID
		}
	}

	// 4) 提交（epoch 冲突 → 本轮作废，外层重跑）
	if len(plan.Expected) > 0 {
		ack, _, err := c.svc.Commit(ctx, simUID, simVault, &dto.V3ManifestCommitRequest{
			BaseEpoch: plan.ServerEpoch, Changes: plan.Expected,
		}, c.name)
		if err != nil {
			if cErr, ok := err.(*code.Code); ok && cErr.Code() == code.ErrorV3EpochConflict.Code() {
				t.Logf("[commit] %s 409 plan.base=%d plan.server=%d expected=%d",
					c.name, plan.BaseEpoch, plan.ServerEpoch, len(plan.Expected))
				return true // 409：重新对账
			}
			t.Fatalf("%s commit 失败: %v", c.name, err)
		}
		t.Logf("[commit] %s ok %d→%d items=%d changes=%v",
			c.name, plan.ServerEpoch, ack.NewEpoch, len(ack.Items), describeChanges(plan.Expected))
		if commitObserver != nil {
			commitObserver(c.name, plan.Expected)
		}
		c.baseEpoch = ack.NewEpoch
		for _, it := range ack.Items {
			c.ids[it.Path] = it.ID
		}
		c.snapshotSynced()
	} else {
		// 无本地变更但应用了 Ops/冲突（server-wins）：本地树已对齐 ServerEpoch，基线随之推进
		c.baseEpoch = plan.ServerEpoch
		c.snapshotSynced()
	}
	return busy
}

// upload 按 BlobNeed 清单把本地内容写入 blob store（真实客户端走分块上传，这里直写等价）
func (c *simClient) upload(t *testing.T, needs []dto.V3BlobNeedMessage) {
	t.Helper()
	for _, n := range needs {
		content, ok := c.files[n.Path]
		require.True(t, ok, "%s 本地缺少待上传文件 %s", c.name, n.Path)
		h, err := c.blobs.BlobStoreFromBytes(simUID, []byte(content))
		require.NoError(t, err)
		require.Equal(t, n.Hash, h, "%s 上传哈希不符 %s", c.name, n.Path)
	}
}

// converge 轮询所有客户端直至静止，并断言多端与服务器三方一致
func converge(t *testing.T, ctx context.Context, svc SyncV3Service, manifestRepo domain.VaultManifestRepository, clients ...*simClient) map[string]string {
	return convergeN(t, ctx, svc, manifestRepo, 12, clients...)
}

// convergeDebugRows 收敛失败时的行级转储钩子（fuzz 注入 fsRepo 视图；nil 则跳过）
var convergeDebugRows func(t *testing.T, ctx context.Context, hot map[string]bool)

// commitObserver 每次成功提交后回调（fuzz 用于谱系污染追踪；nil 则跳过）
var commitObserver func(client string, changes []reconcile.Change)

// convergeN 带轮数上限与诊断的 converge：超限时打印各端最后一轮计划再失败
func convergeN(t *testing.T, ctx context.Context, svc SyncV3Service, manifestRepo domain.VaultManifestRepository, limit int, clients ...*simClient) map[string]string {
	settled := 0
	for round := 0; round < limit; round++ {
		busy := false
		for _, c := range clients {
			if c.syncRound(t, ctx) {
				busy = true
			}
		}
		if !busy {
			settled = round
			break
		}
		if round >= limit-4 { // 尾轮追踪：观察服务器 epoch 是否仍在推进
			if cur, err := manifestRepo.Current(ctx, 1, simUID); err == nil && cur != nil {
				var disagree []string
				for _, c := range clients {
					for p := range c.files {
						disagree = append(disagree, fmt.Sprintf("%s@%s", c.name, p))
					}
					break
				}
				_ = disagree
				t.Logf("[尾轮%d] server epoch=%d items=%d", round, manifestEpoch(cur), len(cur.Items))
			}
		}
		if round == limit-1 {
			// 三视图细查：对争议路径打印 local/server/base 的 id+hash（定位不收敛根因）
			serverSnapshot := serverState(t, ctx, manifestRepo)
			hot := map[string]bool{}
			for _, c := range clients {
				for p, lc := range c.files {
					if s, ok := serverSnapshot[p]; !ok || s != lc {
						hot[p] = true
					}
				}
			}
			if cur, err := manifestRepo.Current(ctx, 1, simUID); err == nil && cur != nil {
				srvByPath := map[string]domain.ManifestItem{}
				for _, it := range cur.Items {
					srvByPath[it.Path] = it
				}
				for p := range hot {
					si := srvByPath[p]
					t.Logf("[细查] path=%s server.id=%s server.hash=%s", p, si.ID, shortHash(si.BlobHash))
					for _, c := range clients {
						lc, hasL := c.files[p]
						var baseItem domain.ManifestItem
						hasB := false
						if c.baseEpoch > 0 {
							if b, err := manifestRepo.GetByEpoch(ctx, c.baseEpoch, 1, simUID); err == nil && b != nil {
								for _, it := range b.Items {
									if it.Path == p {
										baseItem, hasB = it, true
									}
								}
							}
						}
						t.Logf("[细查]   %s local=%q(%t) localID=%s base(%t) id=%s hash=%s tomb=%t",
							c.name, lc, hasL, c.ids[p], hasB, baseItem.ID, shortHash(baseItem.BlobHash), c.tombs[p])
					}
				}
			}
			for _, c := range clients {
				if c.lastPlan == nil {
					continue
				}
				var opsKinds []string
				for _, op := range c.lastPlan.Ops {
					opsKinds = append(opsKinds, string(op.Kind))
				}
				var expOps []string
				for _, ch := range c.lastPlan.Expected {
					expOps = append(expOps, ch.Op+":"+ch.Item.Path)
				}
				t.Logf("[未收敛] %s epoch=%d files=%d ops=%v expected=%v conflicts=%d",
					c.name, c.baseEpoch, len(c.files), opsKinds, expOps, len(c.lastPlan.Conflicts))
			}
			server := serverState(t, ctx, manifestRepo)
			for p, s := range server {
				for _, c := range clients {
					if lc, ok := c.files[p]; !ok || lc != s {
						t.Logf("[未收敛] %s 与服务器不一致 @%s server=%q local=%q", c.name, p, s, c.files[p])
					}
				}
			}
			for _, c := range clients {
				for p, lc := range c.files {
					if s, ok := server[p]; !ok || s != lc {
						t.Logf("[未收敛] 服务器与 %s 不一致 @%s local=%q server=%q", c.name, p, lc, server[p])
					}
				}
			}
			if convergeDebugRows != nil {
				convergeDebugRows(t, ctx, hot)
			}
			t.Fatalf("%d 轮未收敛", limit)
		}
	}
	t.Logf("converge depth=%d", settled)

	server := serverState(t, ctx, manifestRepo)
	first := clients[0].files
	for _, c := range clients[1:] {
		assert.Equal(t, first, c.files, "%s 与 %s 本地树不一致", clients[0].name, c.name)
	}
	assert.Equal(t, server, first, "服务器清单与客户端不一致")
	return first
}

// serverState 服务器当前清单（path → content）
func serverState(t *testing.T, ctx context.Context, manifestRepo domain.VaultManifestRepository) map[string]string {
	cur, err := manifestRepo.Current(ctx, 1, simUID)
	require.NoError(t, err)
	out := map[string]string{}
	if cur == nil {
		return out
	}
	for _, it := range cur.Items {
		// 重新计算内容（不经 blobs 直接读，保持断言独立性）
		p := filepath.Join("storage", "vault", fmt.Sprintf("u_%d", simUID), "blob", it.BlobHash[:2], it.BlobHash)
		data, err := os.ReadFile(p)
		require.NoError(t, err, "服务器清单引用的 blob 缺失: %s", it.Path)
		out[it.Path] = string(data)
	}
	return out
}

// seedSync 先让一个客户端把初始文件同步上去，再让所有客户端对齐
func seedSync(t *testing.T, ctx context.Context, svc SyncV3Service, manifestRepo domain.VaultManifestRepository, seed map[string]string, clients ...*simClient) {
	for p, c := range seed {
		clients[0].write(p, c)
	}
	converge(t, ctx, svc, manifestRepo, clients...)
}

// ==================== 场景矩阵 ====================

func TestV3Converge_BasicAddPull(t *testing.T) {
	svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()

	a := newSimClient("A", svc, d)
	b := newSimClient("B", svc, d)

	a.write("notes/a.md", "# hello")
	a.write("assets/pic.png", "\x89PNG-fake")
	a.write(".obsidian/app.json", `{"theme":"dark"}`)

	final := converge(t, ctx, svc, manifestRepo, a, b)
	assert.Equal(t, "# hello", final["notes/a.md"])
	assert.Equal(t, "\x89PNG-fake", final["assets/pic.png"])
	assert.Equal(t, `{"theme":"dark"}`, final[".obsidian/app.json"])
	// B 拿到了服务器分配的 id（后续 move 检测依赖）
	assert.NotEmpty(t, b.ids["notes/a.md"])
	assert.Equal(t, a.ids["notes/a.md"], b.ids["notes/a.md"])
}

func TestV3Converge_MoveById(t *testing.T) {
	svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()

	a := newSimClient("A", svc, d)
	b := newSimClient("B", svc, d)
	seedSync(t, ctx, svc, manifestRepo, map[string]string{"a.md": "v1"}, a, b)
	idBefore := a.ids["a.md"]

	a.rename("a.md", "dir/moved.md")

	final := converge(t, ctx, svc, manifestRepo, a, b)
	assert.Equal(t, "v1", final["dir/moved.md"])
	assert.NotContains(t, final, "a.md")
	// 身份不断链：同一 entry id 跨越移动
	assert.Equal(t, idBefore, a.ids["dir/moved.md"])
	assert.Equal(t, idBefore, b.ids["dir/moved.md"])
}

func TestV3Converge_Delete(t *testing.T) {
	svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()

	a := newSimClient("A", svc, d)
	b := newSimClient("B", svc, d)
	seedSync(t, ctx, svc, manifestRepo, map[string]string{"a.md": "v1", "b.md": "keep"}, a, b)

	a.remove("a.md")

	final := converge(t, ctx, svc, manifestRepo, a, b)
	assert.NotContains(t, final, "a.md")
	assert.Equal(t, "keep", final["b.md"])
	assert.True(t, b.tombs["a.md"], "B 应用删除后应记录墓碑（防复活）")
}

func TestV3Converge_ModifyModifyConflict(t *testing.T) {
	svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()

	a := newSimClient("A", svc, d)
	b := newSimClient("B", svc, d)
	seedSync(t, ctx, svc, manifestRepo, map[string]string{"a.md": "v1"}, a, b)

	// 双方离线各改一版
	a.write("a.md", "v2-by-A")
	b.write("a.md", "v3-by-B")

	// server-wins：先提交者（A）胜出，B 采信服务器版本
	final := converge(t, ctx, svc, manifestRepo, a, b)
	assert.Equal(t, "v2-by-A", final["a.md"])
}

func TestV3Converge_ConcurrentCommit(t *testing.T) {
	svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()

	a := newSimClient("A", svc, d)
	b := newSimClient("B", svc, d)
	seedSync(t, ctx, svc, manifestRepo, map[string]string{"base.md": "0"}, a, b)
	require.Equal(t, a.baseEpoch, b.baseEpoch)

	a.write("x.md", "from-A")
	b.write("y.md", "from-B")

	// 真实并发：双方在对方提交前各自取得计划（同一 ServerEpoch）
	req := func(c *simClient) *dto.V3SyncRequest {
		return &dto.V3SyncRequest{Vault: simVault, BaseEpoch: c.baseEpoch, Manifest: c.manifest(), Tombstones: c.tombstones()}
	}
	planA, needsA, err := a.svc.SyncPlan(ctx, simUID, simVault, req(a))
	require.NoError(t, err)
	planB, needsB, err := b.svc.SyncPlan(ctx, simUID, simVault, req(b))
	require.NoError(t, err)
	require.Equal(t, planA.ServerEpoch, planB.ServerEpoch)
	a.upload(t, needsA)
	b.upload(t, needsB)

	// 同一 BaseEpoch 双提交：后者必须拿到 409（乐观锁生效）
	_, _, err = a.svc.Commit(ctx, simUID, simVault, &dto.V3ManifestCommitRequest{BaseEpoch: planA.ServerEpoch, Changes: planA.Expected}, "A")
	require.NoError(t, err)
	_, _, err = b.svc.Commit(ctx, simUID, simVault, &dto.V3ManifestCommitRequest{BaseEpoch: planB.ServerEpoch, Changes: planB.Expected}, "B")
	require.Error(t, err)
	cErr, ok := err.(*code.Code)
	require.True(t, ok)
	assert.Equal(t, code.ErrorV3EpochConflict.Code(), cErr.Code())

	// 409 后重新对账，双方内容都保留
	final := converge(t, ctx, svc, manifestRepo, a, b)
	assert.Equal(t, "from-A", final["x.md"])
	assert.Equal(t, "from-B", final["y.md"])
	assert.Equal(t, "0", final["base.md"])
}

func TestV3Converge_OfflineMatrix(t *testing.T) {
	svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()

	a := newSimClient("A", svc, d)
	b := newSimClient("B", svc, d)
	seedSync(t, ctx, svc, manifestRepo, map[string]string{
		"a.md": "a1", "b.md": "b1", "c.md": "c1",
	}, a, b)

	// A 离线：移动 + 删除 + 修改 + 新增
	a.rename("a.md", "moved/a.md")
	a.remove("b.md")
	a.write("c.md", "c2-by-A")
	a.write("d.md", "new-A")

	// B 离线：在旧路径改 a.md（与 A 的移动撞车）+ 新增
	b.write("a.md", "a2-by-B")
	b.write("e.md", "new-B")

	// server-wins 下 B 在旧路径的修改让位于 A 的移动
	final := converge(t, ctx, svc, manifestRepo, a, b)
	assert.Equal(t, "a1", final["moved/a.md"], "B 的旧路径修改被 server-wins 覆盖")
	assert.NotContains(t, final, "a.md")
	assert.NotContains(t, final, "b.md")
	assert.Equal(t, "c2-by-A", final["c.md"])
	assert.Equal(t, "new-A", final["d.md"])
	assert.Equal(t, "new-B", final["e.md"])
}

func TestV3Commit_BlobMissing(t *testing.T) {
	svc, _, _, _, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()

	_, _, err := svc.Commit(ctx, simUID, simVault, &dto.V3ManifestCommitRequest{
		BaseEpoch: 0,
		Changes: []reconcile.Change{{
			Op:   "add",
			Item: domain.ManifestItem{Path: "x.md", BlobHash: util.SHA256Bytes([]byte("never uploaded")), IsNote: true, Size: 13},
		}},
	}, "test")
	require.Error(t, err)
	cErr, ok := err.(*code.Code)
	require.True(t, ok)
	assert.Equal(t, code.ErrorV3BlobMissing.Code(), cErr.Code())
}

// describeChanges 压缩展示变更列表（诊断用）
func describeChanges(changes []reconcile.Change) string {
	if len(changes) > 6 {
		return fmt.Sprintf("%d 项(%s...)", len(changes), changes[0].Op+":"+changes[0].Item.Path)
	}
	parts := make([]string, 0, len(changes))
	for _, ch := range changes {
		p := ch.Op + ":" + ch.Item.Path
		if ch.OldPath != "" {
			p += "(" + ch.OldPath + ")"
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, " ")
}

// shortHash 诊断用哈希缩写
func shortHash(h string) string {
	if len(h) > 10 {
		return h[:10]
	}
	return h
}
