package service

import (
	"context"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// fakeSideEffectDeps 捕获各副作用出口的调用
type fakeSideEffectDeps struct {
	logs      []fakeLogCall
	backupHit int
	gitHit    int
	revoked   []string
}

type fakeLogCall struct {
	uid, vaultID int64
	typ, action  string
	path         string
	size         int64
}

type stubSyncLog struct{ f *fakeSideEffectDeps }

func (s *stubSyncLog) Log(uid, vaultID int64, logType domain.SyncLogType, action domain.SyncLogAction,
	changedFields, path, pathHash, clientType, clientName, clientVersion string, size int64) {
	s.f.logs = append(s.f.logs, fakeLogCall{uid: uid, vaultID: vaultID,
		typ: string(logType), action: string(action), path: path, size: size})
}
func (s *stubSyncLog) List(ctx context.Context, uid int64, vaultID int64, logType, action string, page, pageSize int) ([]*dto.SyncLogDTO, int64, error) {
	return nil, 0, nil
}
func (s *stubSyncLog) CleanupByTime(ctx context.Context, cutoffTime int64) error { return nil }
func (s *stubSyncLog) Shutdown(ctx context.Context) error                        { return nil }

type stubBackup struct {
	BackupService // 嵌入接口：仅 NotifyUpdated 会被监听调用，其余方法 nil 即可
	f             *fakeSideEffectDeps
}

func (s *stubBackup) NotifyUpdated(uid int64) { s.f.backupHit++ }

type stubGit struct {
	GitSyncService
	f *fakeSideEffectDeps
}

func (s *stubGit) NotifyUpdated(uid int64, vaultID int64) { s.f.gitHit++ }

type stubRevoker struct{ f *fakeSideEffectDeps }

func (s *stubRevoker) RevokeV3Entries(ev *CommitEvent, deletedIDs []string) {
	s.f.revoked = append(s.f.revoked, deletedIDs...)
}

func TestV3SideEffects_OnCommit(t *testing.T) {
	f := &fakeSideEffectDeps{}
	l := NewV3SideEffects(&stubSyncLog{f: f}, &stubBackup{f: f}, &stubGit{f: f}, nil, nil, &stubRevoker{f: f}, zap.NewNop())

	ev := &CommitEvent{
		UID: 7, VaultID: 3, Vault: "v", NewEpoch: 12, Client: "obsidian/pc-1",
		Changes: []reconcile.Change{
			{Op: "add", Item: domain.ManifestItem{ID: "uuid-a", Path: "a.md", BlobHash: "h1", IsNote: true, Size: 10}},
			{Op: "modify", Item: domain.ManifestItem{ID: "uuid-b", Path: "b.md", BlobHash: "h2", IsNote: true, Size: 20}},
			{Op: "move", OldPath: "c.md", Item: domain.ManifestItem{ID: "uuid-c", Path: "c2.md", IsNote: true}},
			{Op: "delete", Item: domain.ManifestItem{ID: "uuid-d", Path: "d.png", IsNote: false}},
		},
	}
	l.OnCommit(ev)

	// 逐条日志：类型/动作/路径
	assert.Len(t, f.logs, 4)
	assert.Equal(t, fakeLogCall{uid: 7, vaultID: 3, typ: "note", action: "create", path: "a.md", size: 10}, f.logs[0])
	assert.Equal(t, fakeLogCall{uid: 7, vaultID: 3, typ: "note", action: "modify", path: "b.md", size: 20}, f.logs[1])
	assert.Equal(t, fakeLogCall{uid: 7, vaultID: 3, typ: "note", action: "rename", path: "c2.md", size: 0}, f.logs[2])
	assert.Equal(t, fakeLogCall{uid: 7, vaultID: 3, typ: "file", action: "soft_delete", path: "d.png", size: 0}, f.logs[3])

	// 删除 → 分享撤销收到条目 id
	assert.Equal(t, []string{"uuid-d"}, f.revoked)

	// 备份/Git 各触发一次
	assert.Equal(t, 1, f.backupHit)
	assert.Equal(t, 1, f.gitHit)
}

func TestV3SideEffects_SplitClientTag(t *testing.T) {
	ct, cn, cv := splitClientTag("obsidian/pc-1")
	assert.Equal(t, "obsidian", ct)
	assert.Equal(t, "pc-1", cn)
	assert.Empty(t, cv)

	// v3ClientTag 现在带版本段："type/name/version"
	ct, cn, cv = splitClientTag("obsidian/pc-1/1.2.3")
	assert.Equal(t, "obsidian", ct)
	assert.Equal(t, "pc-1", cn)
	assert.Equal(t, "1.2.3", cv)

	ct, cn, cv = splitClientTag("mcp")
	assert.Equal(t, "mcp", ct)
	assert.Empty(t, cn)
	assert.Empty(t, cv)
}
