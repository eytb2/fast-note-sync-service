// Package service: P6 规模测试——vault 内 5 万条目：
// 大批量提交耗时、全新客户端全量对账耗时、单文件增量对账必须仍是小操作。
package service

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSyncV3_Scale50k 5 万文件规模下的提交/全量对账/增量对账
func TestSyncV3_Scale50k(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过规模测试")
	}
	svc, d, _, manifestRepo, cleanup := setupV3TestEnv(t)
	defer cleanup()
	ctx := context.Background()

	const n = 50000
	content := func(i int) string { return fmt.Sprintf("scale-file-%d\n", i) }

	// 1) 一次性提交 50k 新增（blob 预先入库，等价客户端分块秒传完成后的 Commit）
	changes := make([]reconcile.Change, 0, n)
	items := make([]domain.ManifestItem, 0, n) // 客户端视角的完整清单（提交后回填 id）
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("scale/dir%02d/file%06d.md", i%50, i)
		body := content(i)
		if _, err := d.BlobStoreFromBytes(simUID, []byte(body)); err != nil {
			t.Fatalf("blob %d: %v", i, err)
		}
		changes = append(changes, reconcile.Change{Op: "add", Item: domain.ManifestItem{
			Path: p, BlobHash: util.SHA256Bytes([]byte(body)),
			IsNote: strings.HasSuffix(p, ".md"), Size: int64(len(body)), Mtime: 1, Ctime: 1,
		}})
		items = append(items, domain.ManifestItem{Path: p, BlobHash: util.SHA256Bytes([]byte(body)),
			IsNote: strings.HasSuffix(p, ".md"), Size: int64(len(body)), Mtime: 1, Ctime: 1})
	}

	t0 := time.Now()
	plan, needs, err := svc.SyncPlan(ctx, simUID, simVault, &dto.V3SyncRequest{
		Vault: simVault, Manifest: items, Tombstones: []reconcile.Tombstone{},
	})
	require.NoError(t, err)
	require.Empty(t, needs)
	require.Len(t, plan.Expected, n, "空清单对账应产生 50k 待提交变更")
	t.Logf("SyncPlan(空→50k Expected): %s", time.Since(t0))

	t1 := time.Now()
	ack, _, err := svc.Commit(ctx, simUID, simVault, &dto.V3ManifestCommitRequest{BaseEpoch: plan.ServerEpoch, Changes: plan.Expected}, "scale")
	require.NoError(t, err)
	require.Len(t, ack.Items, n)
	commitDur := time.Since(t1)
	t.Logf("Commit(50k adds): %s (%.0f 条/秒)", commitDur, float64(n)/commitDur.Seconds())
	assert.Less(t, commitDur, 120*time.Second, "5 万条目单事务提交超出预算")

	idByPath := make(map[string]string, n)
	for _, it := range ack.Items {
		idByPath[it.Path] = it.ID
	}
	for i := range items {
		items[i].ID = idByPath[items[i].Path]
	}

	// 2a) 全新客户端（空清单）全量对账：必须给出 50k 个 pull，且不产生任何上传/冲突
	fresh := newSimClient("fresh", svc, d)
	t2 := time.Now()
	fplan, fneeds, err := fresh.svc.SyncPlan(ctx, simUID, simVault, &dto.V3SyncRequest{
		Vault: simVault, Tombstones: []reconcile.Tombstone{},
	})
	require.NoError(t, err)
	fullDur := time.Since(t2)
	t.Logf("SyncPlan(空客户端→50k pull): %s ops=%d", fullDur, len(fplan.Ops))
	assert.Less(t, fullDur, 120*time.Second, "5 万条目全量对账超出预算")
	assert.Len(t, fplan.Ops, n)
	assert.Empty(t, fplan.Conflicts)
	assert.Empty(t, fneeds)

	// 2b) 稳态对账：客户端已持有全部 50k（含 id 与哈希一致）→ 零动作且快速
	t2b := time.Now()
	splan, sneeds, err := svc.SyncPlan(ctx, simUID, simVault, &dto.V3SyncRequest{
		Vault: simVault, BaseEpoch: ack.NewEpoch, Manifest: items, Tombstones: []reconcile.Tombstone{},
	})
	require.NoError(t, err)
	steadyDur := time.Since(t2b)
	t.Logf("SyncPlan(50k 稳态 no-op): %s ops=%d expected=%d needs=%d", steadyDur, len(splan.Ops), len(splan.Expected), len(sneeds))
	assert.Empty(t, splan.Ops)
	assert.Empty(t, splan.Expected)
	assert.Empty(t, splan.Conflicts)
	assert.Empty(t, sneeds)

	// 3) 单文件增量：50k 清单只改 1 个文件 → 1 个 modify；blob 已在库（秒传）则 0 上传需求
	k := 12345
	items[k].BlobHash = util.SHA256Bytes([]byte("modified-body"))
	items[k].Size = int64(len("modified-body"))
	if _, err := d.BlobStoreFromBytes(simUID, []byte("modified-body")); err != nil {
		t.Fatalf("blob modify: %v", err)
	}
	t3 := time.Now()
	iplan, ineads, err := svc.SyncPlan(ctx, simUID, simVault, &dto.V3SyncRequest{
		Vault: simVault, BaseEpoch: ack.NewEpoch, Manifest: items, Tombstones: []reconcile.Tombstone{},
	})
	require.NoError(t, err)
	incrDur := time.Since(t3)
	t.Logf("SyncPlan(50k 增量改 1): %s needs=%d expected=%d ops=%d", incrDur, len(ineads), len(iplan.Expected), len(iplan.Ops))
	assert.Less(t, incrDur, 60*time.Second, "增量对账不应随 vault 规模线性劣化到此程度")
	require.Empty(t, ineads, "blob 已入库应秒传（无上传需求）")
	require.Len(t, iplan.Expected, 1)
	assert.Equal(t, "modify", iplan.Expected[0].Op)
	assert.Equal(t, items[k].Path, iplan.Expected[0].Item.Path)

	// 服务器侧终态：5 万活跃条目
	server := serverState(t, ctx, manifestRepo)
	assert.Len(t, server, n, "服务器活跃条目应恰为 5 万")
}
