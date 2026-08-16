package service

import (
	"context"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// captureBroadcaster ManifestBroadcaster 替身：记录广播消息供断言
type captureBroadcaster struct {
	msgs []*dto.V3NotifyManifestMessage
}

func (b *captureBroadcaster) BroadcastManifest(uid int64, msg *dto.V3NotifyManifestMessage) {
	b.msgs = append(b.msgs, msg)
}

func newContentV3(t *testing.T, svc SyncV3Service, d *dao.Dao, fsRepo domain.FsEntryRepository, manifestRepo domain.VaultManifestRepository) (ContentV3Service, *captureBroadcaster) {
	t.Helper()
	bc := &captureBroadcaster{}
	return NewContentV3Service(fsRepo, manifestRepo, dao.NewEntryHistoryRepository(d), d, fakeVaultResolver{}, svc, bc, zap.NewNop()), bc
}

// ==================== 读基础：Write(add) → 落清单 → 读回 ====================

func TestContentV3_WriteReadRoundtrip(t *testing.T) {
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()
	content, _ := newContentV3(t, svc, d, fsRepo, manifestRepo)

	e, err := content.Write(ctx, simUID, simVault, "a.md", []byte("hello"), true, "rest-test")
	require.NoError(t, err)
	assert.NotEmpty(t, e.ID, "add 时服务器应分配 UUID")
	assert.Equal(t, "a.md", e.Path)
	assert.True(t, e.IsNote)

	// ReadEntry 往返
	e2, data, err := content.ReadEntry(ctx, simUID, simVault, "a.md")
	require.NoError(t, err)
	assert.Equal(t, e.ID, e2.ID)
	assert.Equal(t, "hello", string(data))

	// 落进快照
	cur, err := content.CurrentManifest(ctx, simUID, simVault)
	require.NoError(t, err)
	require.Len(t, cur.Items, 1)
	assert.Equal(t, "a.md", cur.Items[0].Path)

	// 不存在路径 → 548
	_, err = content.GetEntryByPath(ctx, simUID, simVault, "missing.md")
	require.Error(t, err)
	c, ok := err.(*code.Code)
	require.True(t, ok, "应返回 code 错误, got %T", err)
	assert.Equal(t, code.ErrorV3EntryNotFound.Code(), c.Code())
}

// ==================== Write(modify) 保身份 ====================

func TestContentV3_WriteModifyKeepsIdentity(t *testing.T) {
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()
	content, _ := newContentV3(t, svc, d, fsRepo, manifestRepo)

	e1, err := content.Write(ctx, simUID, simVault, "a.md", []byte("v1"), true, "rest-test")
	require.NoError(t, err)
	e2, err := content.Write(ctx, simUID, simVault, "a.md", []byte("v2"), true, "rest-test")
	require.NoError(t, err)
	assert.Equal(t, e1.ID, e2.ID, "modify 不换身份")
	assert.Equal(t, e1.Ctime, e2.Ctime, "modify 保 Ctime")
	assert.GreaterOrEqual(t, e2.Mtime, e1.Mtime)

	_, data, err := content.ReadEntry(ctx, simUID, simVault, "a.md")
	require.NoError(t, err)
	assert.Equal(t, "v2", string(data))

	// 历史应有两个版本（首提交 + modify 各一）
	_, hist, err := content.HistoryByPath(ctx, simUID, simVault, "a.md")
	require.NoError(t, err)
	assert.Len(t, hist, 2)
}

// ==================== Move：身份不变、历史不断链 ====================

func TestContentV3_MoveKeepsIdentity(t *testing.T) {
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()
	content, _ := newContentV3(t, svc, d, fsRepo, manifestRepo)

	e1, err := content.Write(ctx, simUID, simVault, "old.md", []byte("v1"), true, "rest-test")
	require.NoError(t, err)
	_, err = content.Write(ctx, simUID, simVault, "old.md", []byte("v2"), true, "rest-test")
	require.NoError(t, err)

	e2, err := content.Move(ctx, simUID, simVault, "old.md", "new.md", "rest-test")
	require.NoError(t, err)
	assert.Equal(t, e1.ID, e2.ID)
	assert.Equal(t, "new.md", e2.Path)

	_, hist, err := content.HistoryByPath(ctx, simUID, simVault, "new.md")
	require.NoError(t, err)
	assert.Len(t, hist, 2, "移动后历史沿同一 entry id 延续")

	_, err = content.GetEntryByPath(ctx, simUID, simVault, "old.md")
	require.Error(t, err)
}

// ==================== Delete → 548；清单收缩 ====================

func TestContentV3_Delete(t *testing.T) {
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()
	content, _ := newContentV3(t, svc, d, fsRepo, manifestRepo)

	_, err := content.Write(ctx, simUID, simVault, "a.md", []byte("x"), true, "rest-test")
	require.NoError(t, err)
	require.NoError(t, content.Delete(ctx, simUID, simVault, "a.md", "rest-test"))

	_, err = content.GetEntryByPath(ctx, simUID, simVault, "a.md")
	require.Error(t, err)
	c, ok := err.(*code.Code)
	require.True(t, ok)
	assert.Equal(t, code.ErrorV3EntryNotFound.Code(), c.Code())

	cur, err := content.CurrentManifest(ctx, simUID, simVault)
	require.NoError(t, err)
	assert.Empty(t, cur.Items)

	// 再删一次 → 548（幂等重放按不存在处理）
	err = content.Delete(ctx, simUID, simVault, "a.md", "rest-test")
	require.Error(t, err)
}

// ==================== ListEntries：前缀 / isNote / keyset 分页 ====================

func TestContentV3_ListEntries(t *testing.T) {
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()
	content, _ := newContentV3(t, svc, d, fsRepo, manifestRepo)

	seed := map[string]bool{ // path → isNote
		"dir/a.md":  true,
		"dir/b.png": false,
		"dir/c.md":  true,
		"top.md":    true,
	}
	for p, isNote := range seed {
		_, err := content.Write(ctx, simUID, simVault, p, []byte(p), isNote, "rest-test")
		require.NoError(t, err)
	}

	// 前缀全量
	all, err := content.ListEntries(ctx, simUID, simVault, "dir/", nil, "", 100)
	require.NoError(t, err)
	assert.Len(t, all, 3)

	// isNote 过滤
	yes := true
	notes, err := content.ListEntries(ctx, simUID, simVault, "dir/", &yes, "", 100)
	require.NoError(t, err)
	assert.Len(t, notes, 2)
	no := false
	files, err := content.ListEntries(ctx, simUID, simVault, "dir/", &no, "", 100)
	require.NoError(t, err)
	assert.Len(t, files, 1)
	assert.Equal(t, "dir/b.png", files[0].Path)

	// keyset 分页：limit=2 → 第二页 afterPath = 第一页末
	page1, err := content.ListEntries(ctx, simUID, simVault, "dir/", nil, "", 2)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, "dir/a.md", page1[0].Path)
	page2, err := content.ListEntries(ctx, simUID, simVault, "dir/", nil, page1[1].Path, 2)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "dir/c.md", page2[0].Path)
}

// ==================== RestoreFromHash：旧内容写回当前 ====================

func TestContentV3_RestoreFromHash(t *testing.T) {
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()
	content, _ := newContentV3(t, svc, d, fsRepo, manifestRepo)

	_, err := content.Write(ctx, simUID, simVault, "a.md", []byte("v1"), true, "rest-test")
	require.NoError(t, err)
	e2, err := content.Write(ctx, simUID, simVault, "a.md", []byte("v2"), true, "rest-test")
	require.NoError(t, err)

	_, hist, err := content.HistoryByPath(ctx, simUID, simVault, "a.md")
	require.NoError(t, err)
	require.Len(t, hist, 2)
	// 历史倒序：第 0 条最新（v2），第 1 条最旧（v1）
	oldHash := hist[1].BlobHash

	e3, err := content.RestoreFromHash(ctx, simUID, simVault, "a.md", oldHash, "rest-restore")
	require.NoError(t, err)
	assert.Equal(t, e2.ID, e3.ID, "恢复不改身份")

	_, data, err := content.ReadEntry(ctx, simUID, simVault, "a.md")
	require.NoError(t, err)
	assert.Equal(t, "v1", string(data))

	// 恢复也追加历史
	_, hist2, err := content.HistoryByPath(ctx, simUID, simVault, "a.md")
	require.NoError(t, err)
	assert.Len(t, hist2, 3)
}

// ==================== 验收项：门面写入 → v3 客户端可见（epoch 推进 + 广播 + 可拉取） ====================

func TestContentV3_FacadeWriteVisibleToV3Client(t *testing.T) {
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()
	content, bc := newContentV3(t, svc, d, fsRepo, manifestRepo)

	// 先由客户端造基线
	cl := newSimClient("cli", svc, d)
	cl.write("seed.md", "seed")
	converge(t, ctx, svc, manifestRepo, cl)

	// 服务器侧 REST/MCP 写入
	before := cl.baseEpoch
	_, err := content.Write(ctx, simUID, simVault, "from-rest.md", []byte("rest-content"), true, "rest-test")
	require.NoError(t, err)

	// 广播已发出
	require.NotEmpty(t, bc.msgs, "门面写入应触发 NotifyManifest 广播")
	last := bc.msgs[len(bc.msgs)-1]
	assert.Equal(t, simVault, last.Vault)
	assert.Greater(t, last.NewEpoch, before, "epoch 应推进")
	require.NotEmpty(t, last.Ops)

	// 在线客户端一轮同步即可见
	busy := cl.syncRound(t, ctx)
	require.True(t, busy, "门面写入后客户端应有动作")
	assert.Equal(t, "rest-content", cl.files["from-rest.md"])
	// 收敛后三方一致
	final := converge(t, ctx, svc, manifestRepo, cl)
	assert.Equal(t, "rest-content", final["from-rest.md"])
}

// ==================== 门面与客户端并发写入（epoch 竞争由重试兜底） ====================

func TestContentV3_ConcurrentWithClient(t *testing.T) {
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()
	content, _ := newContentV3(t, svc, d, fsRepo, manifestRepo)

	cl := newSimClient("cli", svc, d)
	cl.write("a.md", "client-a")
	converge(t, ctx, svc, manifestRepo, cl)

	// 客户端本地改 a.md、门面写 b.md，交错提交
	cl.write("a.md", "client-a2")
	_, err := content.Write(ctx, simUID, simVault, "b.md", []byte("rest-b"), true, "rest-test")
	require.NoError(t, err)

	final := converge(t, ctx, svc, manifestRepo, cl)
	assert.Equal(t, "client-a2", final["a.md"])
	assert.Equal(t, "rest-b", final["b.md"])

	// 门面删除 → 客户端同步后本地也消失
	require.NoError(t, content.Delete(ctx, simUID, simVault, "b.md", "rest-test"))
	converge(t, ctx, svc, manifestRepo, cl)
	assert.NotContains(t, cl.files, "b.md")
}
