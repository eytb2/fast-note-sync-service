package dao

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// setupFsTestEnv 搭建 SQLite 临时工作区（与 file_repository_test 同模式），返回新数据层四仓储。
func setupFsTestEnv(t *testing.T) (fsRepo domain.FsEntryRepository, manifestRepo domain.VaultManifestRepository,
	histRepo domain.EntryHistoryRepository, idMap domain.FsIdMapRepository, d *Dao, cleanup func()) {

	tempDir, err := os.MkdirTemp("", "fns-fsrepo-test-*")
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
	d = New(db, context.Background(),
		WithConfig(dbCfg),
		WithUserDatabaseConfig(dbCfg),
		WithLogger(zap.NewNop()),
	)

	fsRepo = NewFsEntryRepository(d)
	manifestRepo = NewVaultManifestRepository(d)
	histRepo = NewEntryHistoryRepository(d)
	idMap = NewFsIdMapRepository(d)

	cleanup = func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
		_ = os.Chdir(origWd)
		_ = os.RemoveAll(tempDir)
	}
	return
}

const testUID int64 = 1
const testVault int64 = 1

func TestBlobStoreDedup(t *testing.T) {
	_, _, _, _, d, cleanup := setupFsTestEnv(t)
	defer cleanup()

	h1, err := d.BlobStoreFromBytes(testUID, []byte("hello world"))
	require.NoError(t, err)
	assert.Equal(t, util.SHA256Bytes([]byte("hello world")), h1)
	assert.True(t, d.BlobExists(testUID, h1))

	// Same content written again: same hash, no error (instant upload/dedup)
	h2, err := d.BlobStoreFromBytes(testUID, []byte("hello world"))
	require.NoError(t, err)
	assert.Equal(t, h1, h2)

	data, err := d.BlobReadAll(testUID, h1)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(data))

	// Streaming write + source remains unchanged
	srcPath := filepath.Join(d.BlobTempDir(), "src.bin")
	require.NoError(t, os.MkdirAll(d.BlobTempDir(), 0755))
	require.NoError(t, os.WriteFile(srcPath, []byte(strings.Repeat("x", 3<<20)), 0644))
	fp, err := os.Open(srcPath)
	require.NoError(t, err)
	h3, err := d.BlobStoreFromReader(testUID, fp)
	require.NoError(t, err)
	require.NoError(t, fp.Close())
	assert.Equal(t, util.SHA256Bytes([]byte(strings.Repeat("x", 3<<20))), h3)
	_, err = os.Stat(srcPath) // Source file is kept as-is
	assert.NoError(t, err)
}

func TestFsEntryCRUDAndMoveKeepsIdentity(t *testing.T) {
	fsRepo, _, _, _, _, cleanup := setupFsTestEnv(t)
	defer cleanup()
	ctx := context.Background()

	created, err := fsRepo.Create(ctx, &domain.FsEntry{
		ID: "id-a", VaultID: testVault, IsNote: true,
		Path: "dir/note.md", BlobHash: "hash1", Size: 10, Ctime: 1, Mtime: 2,
	}, testUID)
	require.NoError(t, err)
	assert.Equal(t, "id-a", created.ID)

	got, err := fsRepo.GetLiveByPath(ctx, "dir/note.md", testVault, testUID)
	require.NoError(t, err)
	assert.Equal(t, "id-a", got.ID)

	// Moving only changes the path; identity stays unchanged
	require.NoError(t, fsRepo.MovePath(ctx, "id-a", "new/deep/path.md", 3, testUID))
	got, err = fsRepo.GetByID(ctx, "id-a", testUID)
	require.NoError(t, err)
	assert.Equal(t, "new/deep/path.md", got.Path)

	_, err = fsRepo.GetLiveByPath(ctx, "dir/note.md", testVault, testUID)
	assert.ErrorIs(t, err, domain.ErrEntryNotFound)

	// Delete tombstone → not in active list; restore returns to active
	require.NoError(t, fsRepo.MarkDeleted(ctx, "id-a", testUID))
	live, err := fsRepo.ListLive(ctx, testVault, testUID)
	require.NoError(t, err)
	assert.Empty(t, live)
	require.NoError(t, fsRepo.Restore(ctx, "id-a", testUID))
	live, err = fsRepo.ListLive(ctx, testVault, testUID)
	require.NoError(t, err)
	assert.Len(t, live, 1)
}

func TestManifestCommitOptimistic(t *testing.T) {
	_, manifestRepo, _, _, _, cleanup := setupFsTestEnv(t)
	defer cleanup()
	ctx := context.Background()

	// Empty vault first commit (baseEpoch=0)
	e1, err := manifestRepo.CommitOptimistic(ctx, testVault, testUID, 0, func(store domain.FsEntryStore) ([]domain.ManifestItem, error) {
		require.NoError(t, store.Create(&domain.FsEntry{
			ID: "id-1", VaultID: testVault, IsNote: true, Path: "a.md", BlobHash: "h1", Size: 1,
		}))
		return []domain.ManifestItem{{ID: "id-1", Path: "a.md", BlobHash: "h1", IsNote: true}}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), e1)

	// The second commit with a stale baseEpoch must conflict
	_, err = manifestRepo.CommitOptimistic(ctx, testVault, testUID, 0, func(domain.FsEntryStore) ([]domain.ManifestItem, error) {
		t.Fatal("apply should not run on conflict")
		return nil, nil
	})
	assert.ErrorIs(t, err, domain.ErrEpochConflict)

	// Committing with the correct baseEpoch succeeds, and we can read the historical snapshot by epoch
	e2, err := manifestRepo.CommitOptimistic(ctx, testVault, testUID, e1, func(store domain.FsEntryStore) ([]domain.ManifestItem, error) {
		require.NoError(t, store.MovePath("id-1", "moved.md", 9))
		// Inside the transaction, read via the store (an external connection cannot see uncommitted changes)
		e, err := store.GetByID("id-1")
		if err != nil {
			return nil, err
		}
		return []domain.ManifestItem{e.ToManifestItem()}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), e2)

	base, err := manifestRepo.GetByEpoch(ctx, e1, testVault, testUID)
	require.NoError(t, err)
	require.Len(t, base.Items, 1)
	assert.Equal(t, "a.md", base.Items[0].Path)

	cur, err := manifestRepo.Current(ctx, testVault, testUID)
	require.NoError(t, err)
	require.Len(t, cur.Items, 1)
	assert.Equal(t, "moved.md", cur.Items[0].Path)
	assert.Equal(t, "id-1", cur.Items[0].ID, "moved entry id must be stable")
}

func TestIdMapIdempotent(t *testing.T) {
	_, _, _, idMap, _, cleanup := setupFsTestEnv(t)
	defer cleanup()
	ctx := context.Background()

	_, ok, err := idMap.Get(ctx, "note", 42, testUID)
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, idMap.Put(ctx, "note", 42, "uuid-x", testUID))
	got, ok, err := idMap.Get(ctx, "note", 42, testUID)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "uuid-x", got)
}
