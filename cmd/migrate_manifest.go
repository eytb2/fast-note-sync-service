// migrate-manifest: one-shot migration of existing protocol data to the snapshot (manifest) data layer.
// Design doc §4: only read old tables, only write new tables; idempotent (re-runs skip rows that already have a mapping via fs_id_map).
// migrate-manifest: 存量旧协议数据一次性平迁到快照（manifest）数据层。
// 设计文档 §4：只读旧表、只写新表；幂等（重跑时经 fs_id_map 跳过已映射行）。
package cmd

import (
	"context"
	"fmt"
	"os"

	internalApp "github.com/haierkeys/fast-note-sync-service/internal/app"
	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/fileurl"
	"github.com/haierkeys/fast-note-sync-service/pkg/logger"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func init() {
	var configPath string
	var dryRun bool

	var migrateCmd = &cobra.Command{
		Use:   "migrate-manifest [-c config_file] [--dry-run]",
		Short: "Migrate legacy note/file/setting data to the snapshot (fs_entry/manifest) layer",
		Run: func(cmd *cobra.Command, args []string) {
			if configPath == "" {
				if fileurl.IsExist("config/config-dev.yaml") {
					configPath = "config/config-dev.yaml"
				} else if fileurl.IsExist("config.yaml") {
					configPath = "config.yaml"
				} else {
					configPath = "config/config.yaml"
				}
			}

			appConfig, configRealpath, err := internalApp.LoadConfig(configPath)
			if err != nil {
				bootstrapLogger.Error("failed to load config", zap.Error(err))
				os.Exit(1)
			}
			bootstrapLogger.Info("loading config", zap.String("path", configRealpath))

			lg, err := logger.NewLogger(logger.Config{
				Level:      appConfig.Log.Level,
				File:       appConfig.Log.File,
				Production: appConfig.Log.Production,
			})
			if err != nil {
				bootstrapLogger.Error("failed to init logger", zap.Error(err))
				os.Exit(1)
			}

			dbConfig := appConfig.Database
			dbConfig.RunMode = appConfig.Server.RunMode
			db, err := dao.NewEngine(dbConfig, lg)
			if err != nil {
				bootstrapLogger.Error("failed to init database", zap.Error(err))
				os.Exit(1)
			}

			ctx := context.Background()
			daoObj := dao.New(db, ctx, dao.WithConfig(&dbConfig), dao.WithLogger(lg))

			// Make sure the temp directory for blob streaming already exists
			// 确保 blob 流式写入的临时目录存在
			if err := os.MkdirAll(daoObj.BlobTempDir(), 0755); err != nil {
				bootstrapLogger.Error("failed to init temp dir", zap.Error(err))
				os.Exit(1)
			}

			uids, err := daoObj.GetAllUserUIDs()
			if err != nil {
				bootstrapLogger.Error("failed to list users", zap.Error(err))
				os.Exit(1)
			}

			m := &manifestMigrator{dao: daoObj, log: lg, ctx: ctx, dryRun: dryRun}
			var total migrated
			for _, uid := range uids {
				t, err := m.migrateUser(uid)
				if err != nil {
					bootstrapLogger.Error("migration failed", zap.Int64("uid", uid), zap.Error(err))
					os.Exit(1)
				}
				total.add(t)
			}

			bootstrapLogger.Info("migration done",
				zap.Int64("entries", total.entries),
				zap.Int64("skipped", total.skipped),
				zap.Int64("history", total.history),
				zap.Int64("manifests", total.manifests),
				zap.Bool("dryRun", dryRun))
		},
	}

	rootCmd.AddCommand(migrateCmd)
	fs := migrateCmd.Flags()
	fs.StringVarP(&configPath, "config", "c", "", "config file path (default: config/config.yaml)")
	fs.BoolVar(&dryRun, "dry-run", false, "only report what would be migrated, do not write")
}

type migrated struct {
	entries   int64
	skipped   int64
	history   int64
	manifests int64
}

func (t *migrated) add(o migrated) {
	t.entries += o.entries
	t.skipped += o.skipped
	t.history += o.history
	t.manifests += o.manifests
}

type manifestMigrator struct {
	dao    *dao.Dao
	log    *zap.Logger
	ctx    context.Context
	dryRun bool
}

func (m *manifestMigrator) migrateUser(uid int64) (migrated, error) {
	var out migrated

	vaultDB := m.dao.ResolveDB(fmt.Sprintf("user_vault_%d", uid))
	var vaults []model.Vault
	if err := vaultDB.WithContext(m.ctx).Where("is_deleted = 0").Find(&vaults).Error; err != nil {
		return out, fmt.Errorf("list vaults: %w", err)
	}
	if len(vaults) == 0 {
		return out, nil
	}

	fsRepo := dao.NewFsEntryRepository(m.dao)
	manifestRepo := dao.NewVaultManifestRepository(m.dao)
	histRepo := dao.NewEntryHistoryRepository(m.dao)
	idMap := dao.NewFsIdMapRepository(m.dao)

	// ---------- notes ----------
	noteDB := m.dao.ResolveDB(fmt.Sprintf("user_%d", uid))
	var notes []model.Note
	if err := noteDB.WithContext(m.ctx).Find(&notes).Error; err != nil {
		return out, fmt.Errorf("list notes: %w", err)
	}
	for i := range notes {
		n := &notes[i]
		if _, ok, _ := idMap.Get(m.ctx, "note", n.ID, uid); ok {
			out.skipped++
			continue
		}
		entryID, err := m.migrateNote(uid, n, fsRepo, idMap)
		if err != nil {
			return out, fmt.Errorf("note %d (%s): %w", n.ID, n.Path, err)
		}
		out.entries++
		if entryID == "" { // dry-run
			continue
		}
		// note_history attached to the stable UUID
		histDB := m.dao.ResolveDB(fmt.Sprintf("user_note_history_%d", uid))
		var hists []model.NoteHistory
		if err := histDB.WithContext(m.ctx).Where("note_id = ?", n.ID).Order("id ASC").Find(&hists).Error; err != nil {
			return out, fmt.Errorf("list history for note %d: %w", n.ID, err)
		}
		for _, h := range hists {
			if h.Content == "" {
				continue
			}
			blobHash, err := m.dao.BlobStoreFromBytes(uid, []byte(h.Content))
			if err != nil {
				return out, fmt.Errorf("history blob for note %d v%d: %w", n.ID, h.Version, err)
			}
			if err := histRepo.Append(m.ctx, entryID, n.VaultID, blobHash, int64(len(h.Content)), h.Version, h.ClientName, uid); err != nil {
				return out, fmt.Errorf("append history for note %d: %w", n.ID, err)
			}
			out.history++
		}
	}

	// ---------- files (attachments) ----------
	fileDB := m.dao.ResolveDB(fmt.Sprintf("user_file_%d", uid))
	var files []model.File
	if err := fileDB.WithContext(m.ctx).Find(&files).Error; err != nil {
		return out, fmt.Errorf("list files: %w", err)
	}
	for i := range files {
		f := &files[i]
		if _, ok, _ := idMap.Get(m.ctx, "file", f.ID, uid); ok {
			out.skipped++
			continue
		}
		if _, err := m.migrateFile(uid, f, fsRepo, idMap); err != nil {
			return out, fmt.Errorf("file %d (%s): %w", f.ID, f.Path, err)
		}
		out.entries++
	}

	// ---------- settings (.obsidian configs) ----------
	setDB := m.dao.ResolveDB(fmt.Sprintf("user_setting_%d", uid))
	var settings []model.Setting
	if err := setDB.WithContext(m.ctx).Find(&settings).Error; err != nil {
		return out, fmt.Errorf("list settings: %w", err)
	}
	for i := range settings {
		s := &settings[i]
		if _, ok, _ := idMap.Get(m.ctx, "setting", s.ID, uid); ok {
			out.skipped++
			continue
		}
		if _, err := m.migrateSetting(uid, s, fsRepo, idMap); err != nil {
			return out, fmt.Errorf("setting %d (%s): %w", s.ID, s.Path, err)
		}
		out.entries++
	}

	// ---------- first-version manifest per vault ----------
	for _, v := range vaults {
		cur, err := manifestRepo.Current(m.ctx, v.ID, uid)
		if err != nil {
			return out, fmt.Errorf("manifest current for vault %d: %w", v.ID, err)
		}
		if cur != nil {
			continue
		}
		entries, err := fsRepo.ListLive(m.ctx, v.ID, uid)
		if err != nil {
			return out, fmt.Errorf("list live entries vault %d: %w", v.ID, err)
		}
		if m.dryRun {
			out.manifests++
			continue
		}
		items := make([]domain.ManifestItem, 0, len(entries))
		for _, e := range entries {
			items = append(items, e.ToManifestItem())
		}
		if _, err := manifestRepo.CommitOptimistic(m.ctx, v.ID, uid, 0, func(domain.FsEntryStore) ([]domain.ManifestItem, error) {
			return items, nil
		}); err != nil {
			return out, fmt.Errorf("commit manifest vault %d: %w", v.ID, err)
		}
		out.manifests++
	}

	return out, nil
}

// readNoteContent prefers on-disk content.txt, falling back to the DB Content column (legacy rows)
// readNoteContent 优先读磁盘 content.txt，缺失则回退 DB Content 列（旧数据行）
func (m *manifestMigrator) readNoteContent(uid int64, n *model.Note) ([]byte, bool) {
	folder := m.dao.GetNoteFolderPath(uid, n.ID)
	if content, exists, err := m.dao.LoadContentFromFile(folder, "content.txt"); err == nil && exists {
		return []byte(content), true
	}
	if n.Content != "" {
		return []byte(n.Content), true
	}
	return nil, false
}

func (m *manifestMigrator) migrateNote(uid int64, n *model.Note, fsRepo domain.FsEntryRepository, idMap domain.FsIdMapRepository) (string, error) {
	content, hasContent := m.readNoteContent(uid, n)
	deleted := n.Action == "delete"

	blobHash := ""
	var size int64
	if hasContent {
		if m.dryRun {
			blobHash = "dry-run"
			size = int64(len(content))
		} else {
			h, err := m.dao.BlobStoreFromBytes(uid, content)
			if err != nil {
				return "", err
			}
			blobHash, size = h, int64(len(content))
		}
	} else if !deleted {
		// Active row with no readable content: this shouldn't happen, abort and have the user investigate first
		// 活跃行却读不到内容：不应发生，中止让用户先排查
		return "", fmt.Errorf("active note has no readable content")
	}

	entryID := ""
	if !m.dryRun {
		entryID = uuid.NewString()
	}
	entry := &domain.FsEntry{
		ID: entryID, VaultID: n.VaultID, IsNote: true,
		Path: n.Path, BlobHash: blobHash, Size: size,
		Ctime: n.Ctime, Mtime: n.Mtime, Deleted: deleted,
	}
	if !m.dryRun {
		if _, err := fsRepo.Create(m.ctx, entry, uid); err != nil {
			return "", err
		}
		if err := idMap.Put(m.ctx, "note", n.ID, entryID, uid); err != nil {
			return "", err
		}
	}
	return entryID, nil
}

func (m *manifestMigrator) migrateFile(uid int64, f *model.File, fsRepo domain.FsEntryRepository, idMap domain.FsIdMapRepository) (string, error) {
	deleted := f.Action == "delete"
	blobHash := ""
	var size int64

	srcPath := f.SavePath
	if srcPath == "" {
		srcPath = m.dao.GetFileFolderPath(uid, f.ID) + "/file.dat"
	}
	if fileurl.IsExist(srcPath) {
		if m.dryRun {
			blobHash = "dry-run"
			size = f.Size
		} else {
			fp, err := os.Open(srcPath)
			if err != nil {
				return "", err
			}
			h, err := m.dao.BlobStoreFromReader(uid, fp)
			_ = fp.Close()
			if err != nil {
				return "", err
			}
			blobHash, size = h, f.Size
		}
	} else if !deleted {
		return "", fmt.Errorf("active file blob missing: %s", srcPath)
	}

	entryID := ""
	if !m.dryRun {
		entryID = uuid.NewString()
	}
	entry := &domain.FsEntry{
		ID: entryID, VaultID: f.VaultID, IsNote: false,
		Path: f.Path, BlobHash: blobHash, Size: size,
		Ctime: f.Ctime, Mtime: f.Mtime, Deleted: deleted,
	}
	if !m.dryRun {
		if _, err := fsRepo.Create(m.ctx, entry, uid); err != nil {
			return "", err
		}
		if err := idMap.Put(m.ctx, "file", f.ID, entryID, uid); err != nil {
			return "", err
		}
	}
	return entryID, nil
}

func (m *manifestMigrator) migrateSetting(uid int64, s *model.Setting, fsRepo domain.FsEntryRepository, idMap domain.FsIdMapRepository) (string, error) {
	deleted := s.Action == "delete"
	content := s.Content
	if content == "" {
		folder := m.dao.GetSettingFolderPath(uid, s.ID)
		if c, exists, err := m.dao.LoadContentFromFile(folder, "content.txt"); err == nil && exists {
			content = c
		}
	}
	if content == "" && !deleted {
		return "", fmt.Errorf("active setting has no content")
	}

	blobHash := ""
	var size int64
	if content != "" {
		if m.dryRun {
			blobHash = "dry-run"
			size = int64(len(content))
		} else {
			h, err := m.dao.BlobStoreFromBytes(uid, []byte(content))
			if err != nil {
				return "", err
			}
			blobHash, size = h, int64(len(content))
		}
	}

	entryID := ""
	if !m.dryRun {
		entryID = uuid.NewString()
	}
	entry := &domain.FsEntry{
		ID: entryID, VaultID: s.VaultID, IsNote: true, // 配置也是文本文件
		Path: s.Path, BlobHash: blobHash, Size: size,
		Ctime: s.Ctime, Mtime: s.Mtime, Deleted: deleted,
	}
	if !m.dryRun {
		if _, err := fsRepo.Create(m.ctx, entry, uid); err != nil {
			return "", err
		}
		if err := idMap.Put(m.ctx, "setting", s.ID, entryID, uid); err != nil {
			return "", err
		}
	}
	return entryID, nil
}
