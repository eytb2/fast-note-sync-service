// Package dao: git 式快照同步（WS v3）的仓储实现。
// fs_entry / vault_manifest / entry_history / fs_id_map 四张表。
// 注意：新模型没有 gorm-gen 生成的 query DSL，这里直接使用 *gorm.DB。
package dao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"gorm.io/gorm"
)

// ==================== fs_entry ====================

type fsEntryRepository struct {
	dao *Dao
}

func NewFsEntryRepository(d *Dao) domain.FsEntryRepository {
	return &fsEntryRepository{dao: d}
}

func (r *fsEntryRepository) GetKey(uid int64) string {
	return "user_" + strconv.FormatInt(uid, 10)
}

func init() {
	RegisterModel(ModelConfig{
		Name: "FsEntry",
		RepoFactory: func(d *Dao) daoDBCustomKey {
			return NewFsEntryRepository(d).(daoDBCustomKey)
		},
	})
}

// migrate triggers table creation/initialization via the once-init mechanism, returning the raw DB connection
// migrate 通过 once-init 机制触发表初始化，并返回原始 DB 连接
func (r *fsEntryRepository) migrate(uid int64) *gorm.DB {
	key := r.GetKey(uid)
	_ = r.dao.QueryWithOnceInit(func(g *gorm.DB) {
		model.AutoMigrate(g, "FsEntry")
	}, key+"#fs_entry", key)
	return r.dao.ResolveDB(key)
}

func fsEntryToDomain(m *model.FsEntry) *domain.FsEntry {
	if m == nil {
		return nil
	}
	return &domain.FsEntry{
		ID:         m.ID,
		VaultID:    m.VaultID,
		IsNote:     m.IsNote,
		Path:       m.Path,
		BlobHash:   m.BlobHash,
		Size:       m.Size,
		Ctime:      m.Ctime,
		Mtime:      m.Mtime,
		Deleted:    m.Deleted,
		DeletedAt:  m.DeletedAt,
		ClientName: m.ClientName,
		CreatedAt:  time.Time(m.CreatedAt),
		UpdatedAt:  time.Time(m.UpdatedAt),
	}
}

func (r *fsEntryRepository) GetByID(ctx context.Context, entryID string, uid int64) (*domain.FsEntry, error) {
	var m model.FsEntry
	err := r.migrate(uid).WithContext(ctx).Where("id = ?", entryID).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrEntryNotFound
	}
	return fsEntryToDomain(&m), err
}

func (r *fsEntryRepository) GetLiveByPath(ctx context.Context, path string, vaultID, uid int64) (*domain.FsEntry, error) {
	var m model.FsEntry
	err := r.migrate(uid).WithContext(ctx).
		Where("vault_id = ? AND path = ? AND deleted = false", vaultID, path).
		First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrEntryNotFound
	}
	return fsEntryToDomain(&m), err
}

func (r *fsEntryRepository) ListLive(ctx context.Context, vaultID, uid int64) ([]*domain.FsEntry, error) {
	var rows []model.FsEntry
	err := r.migrate(uid).WithContext(ctx).
		Where("vault_id = ? AND deleted = false", vaultID).
		Order("path ASC").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]*domain.FsEntry, 0, len(rows))
	for i := range rows {
		out = append(out, fsEntryToDomain(&rows[i]))
	}
	return out, nil
}

// StatsLive vault 活跃条目聚合（v3 口径：未删除条目按 is_note 分组计数求和）
func (r *fsEntryRepository) StatsLive(ctx context.Context, vaultID, uid int64) (noteCount, noteSize, fileCount, fileSize int64, err error) {
	type aggRow struct {
		IsNote bool
		Cnt    int64
		Sz     int64
	}
	var rows []aggRow
	err = r.migrate(uid).WithContext(ctx).Model(&model.FsEntry{}).
		Select("is_note, COUNT(*) AS cnt, COALESCE(SUM(size), 0) AS sz").
		Where("vault_id = ? AND deleted = false", vaultID).
		Group("is_note").Scan(&rows).Error
	if err != nil {
		return 0, 0, 0, 0, err
	}
	for _, row := range rows {
		if row.IsNote {
			noteCount, noteSize = row.Cnt, row.Sz
		} else {
			fileCount, fileSize = row.Cnt, row.Sz
		}
	}
	return noteCount, noteSize, fileCount, fileSize, nil
}

func (r *fsEntryRepository) ListLiveByPrefix(ctx context.Context, vaultID int64, prefix string, isNote *bool, afterPath string, limit int, uid int64) ([]*domain.FsEntry, error) {
	q := r.migrate(uid).WithContext(ctx).
		Where("vault_id = ? AND deleted = false", vaultID)
	if prefix != "" {
		q = q.Where("path LIKE ?", prefix+"%")
	}
	if isNote != nil {
		q = q.Where("is_note = ?", *isNote)
	}
	if afterPath != "" {
		q = q.Where("path > ?", afterPath)
	}
	var rows []model.FsEntry
	err := q.Order("path ASC").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]*domain.FsEntry, 0, len(rows))
	for i := range rows {
		out = append(out, fsEntryToDomain(&rows[i]))
	}
	return out, nil
}

// ListDeleted 列出 vault 全部墓碑（回收站视图；按删除时间倒序）
func (r *fsEntryRepository) ListDeleted(ctx context.Context, vaultID, uid int64) ([]*domain.FsEntry, error) {
	var rows []model.FsEntry
	err := r.migrate(uid).WithContext(ctx).
		Where("vault_id = ? AND deleted = true", vaultID).
		Order("deleted_at DESC").
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]*domain.FsEntry, 0, len(rows))
	for i := range rows {
		out = append(out, fsEntryToDomain(&rows[i]))
	}
	return out, nil
}

func (r *fsEntryRepository) ListExpiredDeleted(ctx context.Context, vaultID, uid int64, before int64) ([]*domain.FsEntry, error) {
	var rows []model.FsEntry
	err := r.migrate(uid).WithContext(ctx).
		Where("vault_id = ? AND deleted = true AND deleted_at > 0 AND deleted_at < ?", vaultID, before).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]*domain.FsEntry, 0, len(rows))
	for i := range rows {
		out = append(out, fsEntryToDomain(&rows[i]))
	}
	return out, nil
}

func (r *fsEntryRepository) Create(ctx context.Context, e *domain.FsEntry, uid int64) (*domain.FsEntry, error) {
	_ = r.migrate(uid)
	m := &model.FsEntry{
		ID:         e.ID,
		VaultID:    e.VaultID,
		IsNote:     e.IsNote,
		Path:       e.Path,
		BlobHash:   e.BlobHash,
		Size:       e.Size,
		Ctime:      e.Ctime,
		Mtime:      e.Mtime,
		ClientName: e.ClientName,
		CreatedAt:  timex.Time(time.Now()),
		UpdatedAt:  timex.Time(time.Now()),
	}
	err := r.dao.ExecuteWriteWithRetry(ctx, uid, r, func(db *gorm.DB) error {
		return db.Create(m).Error
	})
	return fsEntryToDomain(m), err
}

func (r *fsEntryRepository) UpdateContent(ctx context.Context, entryID, blobHash string, size, mtime int64, client string, uid int64) error {
	_ = r.migrate(uid)
	return r.dao.ExecuteWriteWithRetry(ctx, uid, r, func(db *gorm.DB) error {
		res := db.Model(&model.FsEntry{}).Where("id = ?", entryID).Updates(map[string]interface{}{
			"blob_hash":   blobHash,
			"size":        size,
			"mtime":       mtime,
			"client_name": client,
			"updated_at":  time.Now(),
		})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return domain.ErrEntryNotFound
		}
		return nil
	})
}

// MovePath moves/renames: only the path of the same row changes; identity and history chain remain intact.
// MovePath 移动/重命名：同一条行只改路径，身份与历史不断链。
func (r *fsEntryRepository) MovePath(ctx context.Context, entryID, newPath string, mtime int64, uid int64) error {
	_ = r.migrate(uid)
	return r.dao.ExecuteWriteWithRetry(ctx, uid, r, func(db *gorm.DB) error {
		res := db.Model(&model.FsEntry{}).Where("id = ? AND deleted = false", entryID).Updates(map[string]interface{}{
			"path":       newPath,
			"mtime":      mtime,
			"updated_at": time.Now(),
		})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return domain.ErrEntryNotFound
		}
		return nil
	})
}

func (r *fsEntryRepository) MarkDeleted(ctx context.Context, entryID string, uid int64) error {
	_ = r.migrate(uid)
	return r.dao.ExecuteWriteWithRetry(ctx, uid, r, func(db *gorm.DB) error {
		res := db.Model(&model.FsEntry{}).Where("id = ? AND deleted = false", entryID).Updates(map[string]interface{}{
			"deleted":    true,
			"deleted_at": time.Now().UnixMilli(),
			"updated_at": time.Now(),
		})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return domain.ErrEntryNotFound
		}
		return nil
	})
}

func (r *fsEntryRepository) Restore(ctx context.Context, entryID string, uid int64) error {
	_ = r.migrate(uid)
	return r.dao.ExecuteWriteWithRetry(ctx, uid, r, func(db *gorm.DB) error {
		res := db.Model(&model.FsEntry{}).Where("id = ? AND deleted = true", entryID).Updates(map[string]interface{}{
			"deleted":    false,
			"deleted_at": 0,
			"updated_at": time.Now(),
		})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return domain.ErrEntryNotFound
		}
		return nil
	})
}

func (r *fsEntryRepository) Purge(ctx context.Context, entryID string, uid int64) error {
	_ = r.migrate(uid)
	return r.dao.ExecuteWriteWithRetry(ctx, uid, r, func(db *gorm.DB) error {
		return db.Where("id = ?", entryID).Delete(&model.FsEntry{}).Error
	})
}

// ==================== vault_manifest ====================

type vaultManifestRepository struct {
	dao *Dao
}

func NewVaultManifestRepository(d *Dao) domain.VaultManifestRepository {
	return &vaultManifestRepository{dao: d}
}

func (r *vaultManifestRepository) GetKey(uid int64) string {
	return "user_" + strconv.FormatInt(uid, 10)
}

func init() {
	RegisterModel(ModelConfig{
		Name: "VaultManifest",
		RepoFactory: func(d *Dao) daoDBCustomKey {
			return NewVaultManifestRepository(d).(daoDBCustomKey)
		},
	})
}

func (r *vaultManifestRepository) migrate(uid int64) *gorm.DB {
	key := r.GetKey(uid)
	_ = r.dao.QueryWithOnceInit(func(g *gorm.DB) {
		model.AutoMigrate(g, "VaultManifest")
		model.AutoMigrate(g, "FsEntry")
		// commit 事务（CommitOptimistic 的 apply）会在同连接里写 entry_history，必须一并建表
		model.AutoMigrate(g, "EntryHistory")
	}, key+"#vault_manifest", key)
	return r.dao.ResolveDB(key)
}

func serializeTree(items []domain.ManifestItem) (string, error) {
	b, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseTree(s string) ([]domain.ManifestItem, error) {
	var items []domain.ManifestItem
	if s == "" {
		return items, nil
	}
	if err := json.Unmarshal([]byte(s), &items); err != nil {
		return nil, fmt.Errorf("manifest tree parse: %w", err)
	}
	return items, nil
}

func (r *vaultManifestRepository) rowToDomain(m *model.VaultManifest) (*domain.Manifest, error) {
	items, err := parseTree(m.Tree)
	if err != nil {
		return nil, err
	}
	return &domain.Manifest{
		Epoch:    m.ID,
		VaultID:  m.VaultID,
		Items:    items,
		CreateAt: time.Time(m.CreatedAt),
	}, nil
}

func (r *vaultManifestRepository) Current(ctx context.Context, vaultID, uid int64) (*domain.Manifest, error) {
	var m model.VaultManifest
	err := r.migrate(uid).WithContext(ctx).
		Where("vault_id = ?", vaultID).
		Order("id DESC").First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return r.rowToDomain(&m)
}

func (r *vaultManifestRepository) GetByEpoch(ctx context.Context, epoch, vaultID, uid int64) (*domain.Manifest, error) {
	var m model.VaultManifest
	err := r.migrate(uid).WithContext(ctx).
		Where("vault_id = ? AND id = ?", vaultID, epoch).
		First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return r.rowToDomain(&m)
}

// fsEntryStoreTx implementation of the domain.FsEntryStore bound inside a transaction
// fsEntryStoreTx 事务内绑定的 domain.FsEntryStore 实现
type fsEntryStoreTx struct {
	tx *gorm.DB
}

func (s *fsEntryStoreTx) GetByID(entryID string) (*domain.FsEntry, error) {
	var m model.FsEntry
	err := s.tx.Where("id = ?", entryID).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrEntryNotFound
	}
	return fsEntryToDomain(&m), err
}

func (s *fsEntryStoreTx) GetLiveByPath(path string, vaultID int64) (*domain.FsEntry, error) {
	var m model.FsEntry
	err := s.tx.Where("vault_id = ? AND path = ? AND deleted = false", vaultID, path).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrEntryNotFound
	}
	return fsEntryToDomain(&m), err
}

func (s *fsEntryStoreTx) Create(e *domain.FsEntry) error {
	m := &model.FsEntry{
		ID: e.ID, VaultID: e.VaultID, IsNote: e.IsNote, Path: e.Path,
		BlobHash: e.BlobHash, Size: e.Size, Ctime: e.Ctime, Mtime: e.Mtime,
		ClientName: e.ClientName,
		CreatedAt:  timex.Time(time.Now()), UpdatedAt: timex.Time(time.Now()),
	}
	return s.tx.Create(m).Error
}

func (s *fsEntryStoreTx) UpdateContent(entryID, blobHash string, size, mtime int64, client string) error {
	return s.tx.Model(&model.FsEntry{}).Where("id = ?", entryID).Updates(map[string]interface{}{
		"blob_hash": blobHash, "size": size, "mtime": mtime, "client_name": client,
		"updated_at": time.Now(),
	}).Error
}

// MovePath 同事务内改路径（同行只改 path，身份与历史不断链）
func (s *fsEntryStoreTx) MovePath(entryID, newPath string, mtime int64) error {
	return s.tx.Model(&model.FsEntry{}).Where("id = ?", entryID).Updates(map[string]interface{}{
		"path": newPath, "mtime": mtime, "updated_at": time.Now(),
	}).Error
}

func (s *fsEntryStoreTx) MarkDeleted(entryID string) error {
	return s.tx.Model(&model.FsEntry{}).Where("id = ?", entryID).Updates(map[string]interface{}{
		"deleted": true, "deleted_at": time.Now().UnixMilli(), "updated_at": time.Now(),
	}).Error
}

func (s *fsEntryStoreTx) Restore(entryID string) error {
	return s.tx.Model(&model.FsEntry{}).Where("id = ?", entryID).Updates(map[string]interface{}{
		"deleted": false, "deleted_at": 0, "updated_at": time.Now(),
	}).Error
}

func (s *fsEntryStoreTx) ListLive(vaultID int64) ([]*domain.FsEntry, error) {
	var rows []model.FsEntry
	err := s.tx.Where("vault_id = ? AND deleted = false", vaultID).Order("path ASC").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]*domain.FsEntry, 0, len(rows))
	for i := range rows {
		out = append(out, fsEntryToDomain(&rows[i]))
	}
	return out, nil
}

func (s *fsEntryStoreTx) AppendHistory(entryID string, blobHash string, size int64, client string) error {
	return s.tx.Create(&model.EntryHistory{
		EntryID: entryID, BlobHash: blobHash, Size: size, Version: time.Now().UnixMilli(), Client: client,
		CreatedAt: timex.Time(time.Now()),
	}).Error
}

func (s *fsEntryStoreTx) ListAll(vaultID int64) ([]*domain.FsEntry, error) {
	var rows []model.FsEntry
	err := s.tx.Where("vault_id = ?", vaultID).Order("path ASC").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]*domain.FsEntry, 0, len(rows))
	for i := range rows {
		out = append(out, fsEntryToDomain(&rows[i]))
	}
	return out, nil
}

// 批量插入的每批行数（sqlite 变量上限与语句规模折中）
const fsBatchSize = 500

func (s *fsEntryStoreTx) CreateBatch(entries []*domain.FsEntry) error {
	if len(entries) == 0 {
		return nil
	}
	now := timex.Time(time.Now())
	rows := make([]model.FsEntry, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, model.FsEntry{
			ID: e.ID, VaultID: e.VaultID, IsNote: e.IsNote, Path: e.Path,
			BlobHash: e.BlobHash, Size: e.Size, Ctime: e.Ctime, Mtime: e.Mtime,
			ClientName: e.ClientName,
			CreatedAt:  now, UpdatedAt: now,
		})
	}
	return s.tx.CreateInBatches(&rows, fsBatchSize).Error
}

func (s *fsEntryStoreTx) AppendHistoryBatch(items []domain.HistoryAppend) error {
	if len(items) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	nowTs := timex.Time(time.Now())
	rows := make([]model.EntryHistory, 0, len(items))
	for i, it := range items {
		// 同批毫秒戳 + 序号偏移，保证同条目多版本间的排序稳定（单条路径是纯 UnixMilli）
		rows = append(rows, model.EntryHistory{
			EntryID: it.EntryID, BlobHash: it.BlobHash, Size: it.Size,
			Version: now + int64(i), Client: it.Client,
			CreatedAt: nowTs,
		})
	}
	return s.tx.CreateInBatches(&rows, fsBatchSize).Error
}

// CommitOptimistic optimistic-lock atomic commit:
// 1) a transaction locks and reads the current epoch; 2) if it doesn't match baseEpoch, return ErrEpochConflict;
// 3) apply mutates fs_entry in the same transaction and returns the new snapshot; 4) write the manifest row (the auto-increment ID is the new epoch).
// CommitOptimistic 乐观锁原子提交：
// 1) 事务内读取当前 epoch；2) 与 baseEpoch 不符则返回 ErrEpochConflict；
// 3) apply 在同一事务内改动 fs_entry 并返回新快照；4) 写 manifest 行（自增 ID 即新 epoch）。
func (r *vaultManifestRepository) CommitOptimistic(ctx context.Context, vaultID, uid int64, baseEpoch int64, apply func(store domain.FsEntryStore) ([]domain.ManifestItem, error)) (int64, error) {
	_ = r.migrate(uid)
	var newEpoch int64
	err := r.dao.ExecuteWriteWithRetry(ctx, uid, r, func(db *gorm.DB) error {
		return db.Transaction(func(tx *gorm.DB) error {
			var cur model.VaultManifest
			err := tx.Where("vault_id = ?", vaultID).Order("id DESC").First(&cur).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if baseEpoch != 0 {
					return domain.ErrEpochConflict
				}
			} else if err != nil {
				return err
			} else if cur.ID != baseEpoch {
				return domain.ErrEpochConflict
			}

			items, err := apply(&fsEntryStoreTx{tx: tx})
			if err != nil {
				return err
			}
			tree, err := serializeTree(items)
			if err != nil {
				return err
			}
			row := &model.VaultManifest{VaultID: vaultID, Tree: tree, CreatedAt: timex.Time(time.Now())}
			if err := tx.Create(row).Error; err != nil {
				return err
			}
			newEpoch = row.ID
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return newEpoch, nil
}

// ==================== entry_history ====================

type entryHistoryRepository struct {
	dao *Dao
}

func NewEntryHistoryRepository(d *Dao) domain.EntryHistoryRepository {
	return &entryHistoryRepository{dao: d}
}

func (r *entryHistoryRepository) GetKey(uid int64) string {
	return "user_" + strconv.FormatInt(uid, 10)
}

func init() {
	RegisterModel(ModelConfig{
		Name: "EntryHistory",
		RepoFactory: func(d *Dao) daoDBCustomKey {
			return NewEntryHistoryRepository(d).(daoDBCustomKey)
		},
	})
}

func (r *entryHistoryRepository) migrate(uid int64) *gorm.DB {
	key := r.GetKey(uid)
	_ = r.dao.QueryWithOnceInit(func(g *gorm.DB) {
		model.AutoMigrate(g, "EntryHistory")
	}, key+"#entry_history", key)
	return r.dao.ResolveDB(key)
}

func (r *entryHistoryRepository) Append(ctx context.Context, entryID string, vaultID int64, blobHash string, size, version int64, client string, uid int64) error {
	_ = r.migrate(uid)
	return r.dao.ExecuteWriteWithRetry(ctx, uid, r, func(db *gorm.DB) error {
		return db.Create(&model.EntryHistory{
			EntryID: entryID, VaultID: vaultID, BlobHash: blobHash,
			Size: size, Version: version, Client: client,
			CreatedAt: timex.Time(time.Now()),
		}).Error
	})
}

func (r *entryHistoryRepository) ListByEntry(ctx context.Context, entryID string, uid int64) ([]domain.EntryHistoryItem, error) {
	var rows []model.EntryHistory
	err := r.migrate(uid).WithContext(ctx).
		Where("entry_id = ?", entryID).Order("id DESC").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]domain.EntryHistoryItem, 0, len(rows))
	for _, m := range rows {
		out = append(out, domain.EntryHistoryItem{
			ID: m.ID, EntryID: m.EntryID, VaultID: m.VaultID, BlobHash: m.BlobHash,
			Size: m.Size, Version: m.Version, Client: m.Client, CreatedAt: time.Time(m.CreatedAt),
		})
	}
	return out, nil
}

// GetByID 按历史行 ID 取单条（REST /note/history 详情）
func (r *entryHistoryRepository) GetByID(ctx context.Context, id, uid int64) (*domain.EntryHistoryItem, error) {
	var row model.EntryHistory
	err := r.migrate(uid).WithContext(ctx).Where("id = ?", id).First(&row).Error
	if err != nil {
		return nil, err
	}
	return &domain.EntryHistoryItem{
		ID: row.ID, EntryID: row.EntryID, VaultID: row.VaultID, BlobHash: row.BlobHash,
		Size: row.Size, Version: row.Version, Client: row.Client, CreatedAt: time.Time(row.CreatedAt),
	}, nil
}

// ==================== fs_id_map（迁移映射） ====================

type fsIdMapRepository struct {
	dao *Dao
}

func NewFsIdMapRepository(d *Dao) domain.FsIdMapRepository {
	return &fsIdMapRepository{dao: d}
}

func (r *fsIdMapRepository) GetKey(uid int64) string {
	return "user_" + strconv.FormatInt(uid, 10)
}

func init() {
	RegisterModel(ModelConfig{
		Name: "FsIdMap",
		RepoFactory: func(d *Dao) daoDBCustomKey {
			return NewFsIdMapRepository(d).(daoDBCustomKey)
		},
	})
}

func (r *fsIdMapRepository) migrate(uid int64) *gorm.DB {
	key := r.GetKey(uid)
	_ = r.dao.QueryWithOnceInit(func(g *gorm.DB) {
		model.AutoMigrate(g, "FsIdMap")
	}, key+"#fs_id_map", key)
	return r.dao.ResolveDB(key)
}

func (r *fsIdMapRepository) Get(ctx context.Context, oldType string, oldID int64, uid int64) (string, bool, error) {
	var m model.FsIdMap
	err := r.migrate(uid).WithContext(ctx).
		Where("old_type = ? AND old_id = ?", oldType, oldID).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return m.EntryID, true, nil
}

func (r *fsIdMapRepository) Put(ctx context.Context, oldType string, oldID int64, entryID string, uid int64) error {
	_ = r.migrate(uid)
	return r.dao.ExecuteWriteWithRetry(ctx, uid, r, func(db *gorm.DB) error {
		return db.Create(&model.FsIdMap{
			OldType: oldType, OldID: oldID, EntryID: entryID,
			CreatedAt: timex.Time(time.Now()),
		}).Error
	})
}
