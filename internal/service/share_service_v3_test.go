// Package service: 分享链路的 v3 集成测试（P5 功能回接）。
// 覆盖：生成（含嵌入附件授权）→ 验证 → 笔记渲染（引用改写）→ 附件零拷贝下载 →
// 条目删除触发撤销 → 重命名不迁移（条目 UUID 稳定）。
package service

import (
	"context"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newShareV3 搭建真实仓储栈上的分享服务（vault/user_share 均落 SQLite）
func newShareV3(t *testing.T) (ShareService, SyncV3Service, ContentV3Service, *dao.Dao) {
	t.Helper()
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	vaultRepo := dao.NewVaultRepository(d)
	// 触发 vault 表的懒迁移（ExecuteWrite 不走 OnceInit AutoMigrate）
	_, _ = vaultRepo.GetByName(ctx, simVault, simUID)
	if _, err := vaultRepo.Create(ctx, &domain.Vault{UID: simUID, Name: simVault}, simUID); err != nil {
		t.Fatalf("create vault: %v", err)
	}

	tm := pkgapp.NewTokenManager(pkgapp.TokenConfig{
		SecretKey:     "test-secret-key-32-bytes-aaaaaaaaa",
		ShareTokenKey: "test-share-key-32-bytes-bbbbbbbbb",
		ShareExpiry:   time.Hour, // 默认 0 会让 token 立刻过期（跨秒即失效，测试偶发挂）
	})
	share := NewShareService(
		dao.NewUserShareRepository(d), tm, vaultRepo, fsRepo, d,
		zap.NewNop(), &ServiceConfig{},
	)
	content, _ := newContentV3(t, svc, d, fsRepo, manifestRepo)
	return share, svc, content, d
}

// TestShareV3_FullChain 生成 → 验证 → 渲染 → 附件授权 → 下载
func TestShareV3_FullChain(t *testing.T) {
	share, _, content, d := newShareV3(t)
	ctx := context.Background()

	// 笔记 + 嵌入附件（相对路径引用）
	note, err := content.Write(ctx, simUID, simVault, "notes/pic-note.md", []byte("# t\n![[assets/pic.png]]"), true, "rest")
	require.NoError(t, err)
	pic, err := content.Write(ctx, simUID, simVault, "notes/assets/pic.png", []byte("\x89PNG-fake"), false, "rest")
	require.NoError(t, err)
	_ = pic

	res, err := share.ShareGenerate(ctx, simUID, simVault, "notes/pic-note.md", "hash-ignored", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "note", res.Type)
	assert.Equal(t, note.ID, res.EntryID)
	assert.NotEmpty(t, res.Token)

	// VerifyShare：主资源 + 附件都在授权列表
	entity, err := share.VerifyShare(ctx, res.Token, note.ID, "note", "")
	require.NoError(t, err)
	assert.Contains(t, entity.Resources["file"], pic.ID, "嵌入附件应被授权")

	// 渲染：嵌入引用改写为分享 API URL（携带附件 UUID）
	dto, err := share.GetSharedNote(ctx, res.Token, note.ID, "")
	require.NoError(t, err)
	assert.Equal(t, note.ID, dto.EntryID)
	assert.Contains(t, dto.Content, "/api/share/file?id="+pic.ID)
	assert.Contains(t, dto.Content, "share_token="+res.Token)

	// 附件零拷贝元数据（物理 blob 路径 + etag=blob hash）
	savePath, ctype, mtime, etag, fname, err := share.GetSharedFileInfo(ctx, res.Token, pic.ID, "")
	require.NoError(t, err)
	assert.Equal(t, "image/png", ctype)
	assert.Equal(t, pic.BlobHash, etag)
	assert.Equal(t, "pic.png", fname)
	assert.NotEmpty(t, savePath)
	assert.NotZero(t, mtime)

	// 附件内容读取
	body, _, _, _, _, err := share.GetSharedFile(ctx, res.Token, pic.ID, "")
	require.NoError(t, err)
	assert.Equal(t, "\x89PNG-fake", string(body))
	_ = d
}

// TestShareV3_RevokeOnDelete 条目删除 → 分享自动撤销
func TestShareV3_RevokeOnDelete(t *testing.T) {
	share, _, content, _ := newShareV3(t)
	ctx := context.Background()

	note, err := content.Write(ctx, simUID, simVault, "shared.md", []byte("v1"), true, "rest")
	require.NoError(t, err)
	res, err := share.ShareGenerate(ctx, simUID, simVault, "shared.md", "", "", 0)
	require.NoError(t, err)

	// 删除条目 → 副作用撤销（RevokeV3Entries 由提交监听触发；这里直接验证实现）
	share.RevokeV3Entries(&CommitEvent{UID: simUID, VaultID: 1, Vault: simVault}, []string{note.ID})

	_, err = share.VerifyShare(ctx, res.Token, note.ID, "note", "")
	require.Error(t, err, "条目删除后分享应被撤销")
}

// TestShareV3_RenameKeepsShare 重命名不改 ID：分享查询与验证沿用原 token
func TestShareV3_RenameKeepsShare(t *testing.T) {
	share, _, content, _ := newShareV3(t)
	ctx := context.Background()

	_, err := content.Write(ctx, simUID, simVault, "old-name.md", []byte("v1"), true, "rest")
	require.NoError(t, err)
	res, err := share.ShareGenerate(ctx, simUID, simVault, "old-name.md", "", "", 0)
	require.NoError(t, err)

	moved, err := content.Move(ctx, simUID, simVault, "old-name.md", "dir/new-name.md", "rest")
	require.NoError(t, err)

	// 旧路径的分享记录仍指向同一 entry → 新路径查询复用，验证不受影响
	got, err := share.GetShareByPath(ctx, simUID, simVault, "dir/new-name.md")
	require.NoError(t, err)
	assert.Equal(t, moved.ID, got.ResIDV3)
	_, err = share.VerifyShare(ctx, res.Token, moved.ID, "note", "")
	require.NoError(t, err, "重命名不应使分享失效（entry ID 稳定）")
}
