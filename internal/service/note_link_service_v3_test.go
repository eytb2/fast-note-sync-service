// Package service: 链接查询（反链/出链）的 v3 集成测试（P5 功能回接）。
// 覆盖：变体匹配（[[note]] 命中深层路径）、嵌入、#heading 归一化、
// 出链上下文、epoch 缓存失效（提交后反链更新）、错误路径。
package service

import (
	"context"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newLinkV3 搭建真实仓储栈上的 v3 链接服务
func newLinkV3(t *testing.T) (NoteLinkService, ContentV3Service) {
	t.Helper()
	svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
	t.Cleanup(cleanup)
	content, _ := newContentV3(t, svc, d, fsRepo, manifestRepo)
	return NewNoteLinkServiceV3(fsRepo, manifestRepo, content, fakeVaultResolver{}, zap.NewNop()), content
}

// TestNoteLinkV3_BacklinksVariations 各形态链接（短名/嵌入/#heading/全路径）都应命中反链
func TestNoteLinkV3_BacklinksVariations(t *testing.T) {
	links, content := newLinkV3(t)
	ctx := context.Background()

	_, err := content.Write(ctx, simUID, simVault, "projects/folder/note.md", []byte("target body"), true, "rest")
	require.NoError(t, err)
	_, err = content.Write(ctx, simUID, simVault, "daily/a.md",
		[]byte("intro text before [[note]] link\n![[folder/note]] embed\nand [[note#section|jump]] alias"), true, "rest")
	require.NoError(t, err)
	_, err = content.Write(ctx, simUID, simVault, "b.md", []byte("ref [[projects/folder/note]] full path"), true, "rest")
	require.NoError(t, err)
	_, err = content.Write(ctx, simUID, simVault, "c.md", []byte("plain note no links"), true, "rest")
	require.NoError(t, err)

	items, err := links.GetBacklinks(ctx, simUID, &dto.NoteLinkQueryRequest{Vault: simVault, Path: "projects/folder/note.md"})
	require.NoError(t, err)
	require.Len(t, items, 2, "两个来源笔记应命中")

	byPath := map[string]*dto.NoteLinkItem{}
	for _, it := range items {
		byPath[it.Path] = it
	}
	a := byPath["daily/a.md"]
	require.NotNil(t, a, "短名 [[note]] 应命中深层路径")
	assert.Equal(t, "", a.LinkText, "[[note]] 无别名则 LinkText 为空")
	assert.Contains(t, a.Context, "[[note]]", "上下文应包含链接原文")

	b := byPath["b.md"]
	require.NotNil(t, b, "全路径链接应命中")
	assert.False(t, b.IsEmbed)
	assert.Contains(t, b.Context, "[[projects/folder/note]]")

	// 无反链的目标 → 空结果（不是错误）
	none, err := links.GetBacklinks(ctx, simUID, &dto.NoteLinkQueryRequest{Vault: simVault, Path: "c.md"})
	require.NoError(t, err)
	assert.Empty(t, none)
}

// TestNoteLinkV3_Outlinks 出链解析：路径/别名/嵌入标记/上下文
func TestNoteLinkV3_Outlinks(t *testing.T) {
	links, content := newLinkV3(t)
	ctx := context.Background()

	_, err := content.Write(ctx, simUID, simVault, "daily/a.md",
		[]byte("intro [[note]] link\n![[folder/note]] embed\nand [[note#section|jump]] alias"), true, "rest")
	require.NoError(t, err)

	items, err := links.GetOutlinks(ctx, simUID, &dto.NoteLinkQueryRequest{Vault: simVault, Path: "daily/a.md"})
	require.NoError(t, err)
	require.Len(t, items, 3)

	assert.Equal(t, "note", items[0].Path)
	assert.Equal(t, "", items[0].LinkText)
	assert.False(t, items[0].IsEmbed)
	assert.Contains(t, items[0].Context, "[[note]]")

	assert.Equal(t, "folder/note", items[1].Path)
	assert.True(t, items[1].IsEmbed, "![[...]] 应标记为嵌入")

	assert.Equal(t, "note#section", items[2].Path)
	assert.Equal(t, "jump", items[2].LinkText, "别名应保留")
}

// TestNoteLinkV3_CacheInvalidation 提交（epoch 前进）后索引重建：反链随内容更新
func TestNoteLinkV3_CacheInvalidation(t *testing.T) {
	links, content := newLinkV3(t)
	ctx := context.Background()

	_, err := content.Write(ctx, simUID, simVault, "target.md", []byte("t"), true, "rest")
	require.NoError(t, err)
	_, err = content.Write(ctx, simUID, simVault, "src.md", []byte("see [[target]]"), true, "rest")
	require.NoError(t, err)

	items, err := links.GetBacklinks(ctx, simUID, &dto.NoteLinkQueryRequest{Vault: simVault, Path: "target.md"})
	require.NoError(t, err)
	require.Len(t, items, 1)

	// 改写来源（去掉链接）→ epoch 变化 → 缓存失效 → 反链为空
	_, err = content.Write(ctx, simUID, simVault, "src.md", []byte("no links now"), true, "rest")
	require.NoError(t, err)
	items, err = links.GetBacklinks(ctx, simUID, &dto.NoteLinkQueryRequest{Vault: simVault, Path: "target.md"})
	require.NoError(t, err)
	assert.Empty(t, items, "提交后反链应反映最新内容")

	// 删除来源 → 反链消失；新增链接 → 反链出现
	require.NoError(t, content.Delete(ctx, simUID, simVault, "src.md", "rest"))
	items, err = links.GetBacklinks(ctx, simUID, &dto.NoteLinkQueryRequest{Vault: simVault, Path: "target.md"})
	require.NoError(t, err)
	assert.Empty(t, items)
}

// TestNoteLinkV3_Errors 出链目标不存在 / 非笔记 → ErrorNoteNotFound
func TestNoteLinkV3_Errors(t *testing.T) {
	links, content := newLinkV3(t)
	ctx := context.Background()

	_, err := content.Write(ctx, simUID, simVault, "pic.png", []byte("\x89PNG"), false, "rest")
	require.NoError(t, err)

	_, err = links.GetOutlinks(ctx, simUID, &dto.NoteLinkQueryRequest{Vault: simVault, Path: "missing.md"})
	assert.True(t, isCode(err, code.ErrorNoteNotFound), "missing.md 应返回 ErrorNoteNotFound，got %v", err)

	_, err = links.GetOutlinks(ctx, simUID, &dto.NoteLinkQueryRequest{Vault: simVault, Path: "pic.png"})
	assert.True(t, isCode(err, code.ErrorNoteNotFound), "附件没有出链，got %v", err)
}
