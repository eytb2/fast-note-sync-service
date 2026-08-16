// Package service: P6 边缘场景——CLI 无名事件改名的哈希推断、大小写翻转重命名、
// 并发提交压测（多 goroutine 同时 Commit → 409 重试 → 全部落盘收敛）。
package service

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// planFor 以客户端当前状态构造一次对账请求（只读服务器，不应用）
func planFor(t *testing.T, ctx context.Context, c *simClient) (*dto.V3SyncPlanMessage, []dto.V3BlobNeedMessage) {
	t.Helper()
	plan, needs, err := c.svc.SyncPlan(ctx, simUID, simVault, &dto.V3SyncRequest{
		Vault: simVault, BaseEpoch: c.baseEpoch,
		Manifest: c.manifest(), Tombstones: c.tombstones(),
	})
	require.NoError(t, err)
	return plan, needs
}

// TestSyncV3_CLIRenameHashInference CLI 式改名（删旧路径 + 新路径同内容，无 move 事件、
// 无 id 提示）：服务器两轮 move 检测的第二轮（同哈希推断）必须把它识别为 move，
// 其他客户端收到 OpMove 而非 删除+新增，条目 UUID 谱系保持。
func TestSyncV3_CLIRenameHashInference(t *testing.T) {
	svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()
	a := newSimClient("A", svc, d)
	b := newSimClient("B", svc, d)

	a.write("docs/old-name.md", "# stable content")
	a.syncRound(t, ctx)
	converge(t, ctx, svc, manifestRepo, a, b)
	origID := a.ids["docs/old-name.md"]
	require.NotEmpty(t, origID)

	// CLI 改名：等价于 remove+write，不携带任何 id
	a.remove("docs/old-name.md")
	a.write("docs/new-name.md", "# stable content")
	a.syncRound(t, ctx)

	// B 视角：必须是一个 OpMove（保 id），且不得伴随对旧路径的 OpDelete
	plan, _ := planFor(t, ctx, b)
	var moves, deletes, pulls int
	var moveID, moveFrom, moveTo string
	for _, op := range plan.Ops {
		switch op.Kind {
		case reconcile.OpMove:
			moves++
			moveFrom, moveTo, moveID = op.From, op.Item.Path, op.Item.ID
		case reconcile.OpDelete:
			deletes++
		case reconcile.OpPull:
			pulls++
		}
	}
	require.Equal(t, 1, moves, "同哈希删除+新增应被推断为一次 move，实际 ops=%+v", plan.Ops)
	assert.Equal(t, "docs/old-name.md", moveFrom)
	assert.Equal(t, "docs/new-name.md", moveTo)
	assert.Equal(t, origID, moveID, "哈希推断的 move 必须保住原条目 UUID")
	assert.Zero(t, deletes, "move 已涵盖旧路径消失，不应再发 OpDelete")
	assert.Zero(t, pulls, "内容未变，不应重新拉取 blob")

	// 全端收敛 + 谱系断言
	converge(t, ctx, svc, manifestRepo, a, b)
	assert.Equal(t, origID, b.ids["docs/new-name.md"])
	assert.NotContains(t, b.ids, "docs/old-name.md")
}

// TestSyncV3_CaseRename 大小写翻转重命名（note.md → Note.md）：
// 路径仅大小写不同，不得出现 同路径一半删除一半新增 的抖动；
// GUI 式（id 随行）与 CLI 式（哈希推断）都要收敛且保谱系。
func TestSyncV3_CaseRename(t *testing.T) {
	t.Run("gui-id", func(t *testing.T) {
		svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
		defer cleanup()
		ctx := context.Background()
		a := newSimClient("A", svc, d)
		b := newSimClient("B", svc, d)

		a.write("journal/note.md", "case body")
		a.syncRound(t, ctx)
		converge(t, ctx, svc, manifestRepo, a, b)
		origID := a.ids["journal/note.md"]
		require.NotEmpty(t, origID)

		a.rename("journal/note.md", "journal/Note.md")
		a.syncRound(t, ctx)

		plan, _ := planFor(t, ctx, b)
		require.Len(t, plan.Ops, 1, "大小写改名应且仅应产生一个 op: %+v", plan.Ops)
		require.Equal(t, reconcile.OpMove, plan.Ops[0].Kind)
		assert.Equal(t, "journal/note.md", plan.Ops[0].From)
		assert.Equal(t, "journal/Note.md", plan.Ops[0].Item.Path)
		assert.Equal(t, origID, plan.Ops[0].Item.ID)

		converge(t, ctx, svc, manifestRepo, a, b)
		assert.Equal(t, origID, b.ids["journal/Note.md"])
		assert.NotContains(t, b.files, "journal/note.md")
	})

	t.Run("cli-hash", func(t *testing.T) {
		svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
		defer cleanup()
		ctx := context.Background()
		a := newSimClient("A", svc, d)
		b := newSimClient("B", svc, d)

		a.write("wiki/Page.md", "page v1")
		a.syncRound(t, ctx)
		converge(t, ctx, svc, manifestRepo, a, b)
		origID := a.ids["wiki/Page.md"]
		require.NotEmpty(t, origID)

		// CLI 式大小写改名：删 Page.md + 增 page.md（同内容）
		a.remove("wiki/Page.md")
		a.write("wiki/page.md", "page v1")
		a.syncRound(t, ctx)

		plan, _ := planFor(t, ctx, b)
		require.NotEmpty(t, plan.Ops, "服务器必须对大小写改哈希名产生 op")
		assert.Equal(t, reconcile.OpMove, plan.Ops[0].Kind, "应为 move 而非删+增: %+v", plan.Ops)
		assert.Equal(t, origID, plan.Ops[0].Item.ID)

		converge(t, ctx, svc, manifestRepo, a, b)
		assert.Equal(t, origID, b.ids["wiki/page.md"])
	})
}

// TestSyncV3_ConcurrentCommitStress 并发提交压测：8 个客户端同时提交互不相干的
// 新文件（同一 vault），409 乐观锁冲突必须由重试吸收，最终全部文件在服务器落盘、
// 所有客户端收敛一致。
func TestSyncV3_ConcurrentCommitStress(t *testing.T) {
	svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()

	// 基线：A 建一个公共文件并全员拉平
	a := newSimClient("A", svc, d)
	a.write("base/seed.md", "base")
	a.syncRound(t, ctx)

	const n = 8
	clients := make([]*simClient, n)
	for i := 0; i < n; i++ {
		clients[i] = newSimClient(fmt.Sprintf("W%d", i), svc, d)
	}
	converge(t, ctx, svc, manifestRepo, append([]*simClient{a}, clients...)...)

	// 各写一个独占文件后并发出同步轮（模拟同时 Commit）
	for i, c := range clients {
		c.write(fmt.Sprintf("conc/w%d.md", i), fmt.Sprintf("payload %d", i))
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, c := range clients {
		wg.Add(1)
		go func(c *simClient) {
			defer wg.Done()
			<-start
			for round := 0; round < 20; round++ { // 409 由重试吸收
				if !c.syncRound(t, ctx) {
					break
				}
			}
		}(c)
	}
	close(start)
	wg.Wait()

	converge(t, ctx, svc, manifestRepo, append([]*simClient{a}, clients...)...)
	for i, c := range clients {
		p := fmt.Sprintf("conc/w%d.md", i)
		assert.Contains(t, c.files, p, "并发提交后 %s 丢失", p)
		assert.NotEmpty(t, c.ids[p])
	}
}
