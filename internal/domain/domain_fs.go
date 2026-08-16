// Package domain: git 式快照同步（WS v3）的领域类型与仓储接口。
package domain

import (
	"context"
	"io"
	"time"
)

// ManifestItem 清单（快照）中的一项：路径 + 文件身份 + 内容哈希。
// 客户端与服务器对账时交换的就是这份结构的集合。
type ManifestItem struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	BlobHash string `json:"hash"`
	IsNote   bool   `json:"isNote"`
	Size     int64  `json:"size"`
	Mtime    int64  `json:"mtime"`
	Ctime    int64  `json:"ctime"`
}

// Manifest vault 的一次版本化快照。Epoch 即 manifest 表的自增 ID。
type Manifest struct {
	Epoch    int64          `json:"epoch"`
	VaultID  int64          `json:"vaultId"`
	Items    []ManifestItem `json:"items"`
	CreateAt time.Time      `json:"createAt"`
}

// FsEntry vault 内一个文件的登记行（笔记/附件/配置统一）。
type FsEntry struct {
	ID         string
	VaultID    int64
	IsNote     bool
	Path       string
	BlobHash   string
	Size       int64
	Ctime      int64
	Mtime      int64
	Deleted    bool
	DeletedAt  int64
	ClientName string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ToManifestItem 转为清单条目
// ToManifestItem converts to a manifest item
func (e *FsEntry) ToManifestItem() ManifestItem {
	return ManifestItem{
		ID:       e.ID,
		Path:     e.Path,
		BlobHash: e.BlobHash,
		IsNote:   e.IsNote,
		Size:     e.Size,
		Mtime:    e.Mtime,
		Ctime:    e.Ctime,
	}
}

// ErrEpochConflict optimistic-lock conflict: baseEpoch is stale (another client has already committed a newer epoch)
// ErrEpochConflict 乐观锁冲突：baseEpoch 已过期（已有其他客户端提交了更新的 epoch）
var ErrEpochConflict = errFS("epoch conflict")

// ErrEntryNotFound file entry does not exist
// ErrEntryNotFound 文件条目不存在
var ErrEntryNotFound = errFS("entry not found")

type errFS string

func (e errFS) Error() string { return string(e) }

// FsEntryRepository 文件登记表仓储
type FsEntryRepository interface {
	GetKey(uid int64) string

	// GetByID 按 UUID 获取（含墓碑）
	GetByID(ctx context.Context, entryID string, uid int64) (*FsEntry, error)
	// GetLiveByPath 按路径获取活跃条目
	GetLiveByPath(ctx context.Context, path string, vaultID, uid int64) (*FsEntry, error)
	// ListLive 列出 vault 全部活跃条目
	ListLive(ctx context.Context, vaultID, uid int64) ([]*FsEntry, error)
	// ListLiveByPrefix 前缀列举活跃条目（REST/MCP 树与目录内容）。
	// isNote 双态过滤（nil=全部）；afterPath="" 从头，否则取 path 严格大于它的行（keyset 分页）。
	ListLiveByPrefix(ctx context.Context, vaultID int64, prefix string, isNote *bool, afterPath string, limit int, uid int64) ([]*FsEntry, error)
	// StatsLive 活跃条目聚合统计（笔记/附件的数量与总大小；vault 表计数器自 v3 起不再增量维护，展示层现算）
	StatsLive(ctx context.Context, vaultID, uid int64) (noteCount, noteSize, fileCount, fileSize int64, err error)
	// ListDeleted 列出全部墓碑（回收站视图；按删除时间倒序）
	ListDeleted(ctx context.Context, vaultID, uid int64) ([]*FsEntry, error)
	// ListExpiredDeleted 列出超过保留期的墓碑（物理清除用）
	ListExpiredDeleted(ctx context.Context, vaultID, uid int64, before int64) ([]*FsEntry, error)

	Create(ctx context.Context, entry *FsEntry, uid int64) (*FsEntry, error)
	// UpdateContent 更新内容引用（blob_hash/size/mtime）
	UpdateContent(ctx context.Context, entryID string, blobHash string, size, mtime int64, client string, uid int64) error
	// MovePath 移动/重命名：同一条行只改路径（身份与历史不断链）
	MovePath(ctx context.Context, entryID, newPath string, mtime int64, uid int64) error
	// MarkDeleted 置墓碑（回收站语义）
	MarkDeleted(ctx context.Context, entryID string, uid int64) error
	// Restore 从回收站恢复
	Restore(ctx context.Context, entryID string, uid int64) error
	// Purge 物理删除行
	Purge(ctx context.Context, entryID string, uid int64) error
}

// FsEntryStore 事务内绑定的操作子集（CommitOptimistic 的 apply 回调参数）
type FsEntryStore interface {
	GetByID(entryID string) (*FsEntry, error)
	GetLiveByPath(path string, vaultID int64) (*FsEntry, error)
	Create(entry *FsEntry) error
	UpdateContent(entryID, blobHash string, size, mtime int64, client string) error
	MovePath(entryID, newPath string, mtime int64) error
	MarkDeleted(entryID string) error
	// Restore 墓碑复活（同 id 条目重新上报/移动到新路径）
	Restore(entryID string) error
	// ListLive 列出 vault 全部活跃条目（apply 返回值 = 新快照的来源）
	ListLive(vaultID int64) ([]*FsEntry, error)
	// AppendHistory 在同一事务内追加版本历史（失败不阻断提交语义上可接受，但保持原子更简单）
	AppendHistory(entryID string, blobHash string, size int64, client string) error
	// ListAll 列出 vault 全部条目（含墓碑）——大清单提交的内存快照索引用（复活判定需要墓碑）
	ListAll(vaultID int64) ([]*FsEntry, error)
	// CreateBatch 批量新建（万级首灌时逐条 INSERT 会拖垮事务）
	CreateBatch(entries []*FsEntry) error
	// AppendHistoryBatch 批量追加历史（同 CreateBatch 动机）
	AppendHistoryBatch(items []HistoryAppend) error
}

// HistoryAppend AppendHistory 的批量形态（字段一一对应）
type HistoryAppend struct {
	EntryID  string
	BlobHash string
	Size     int64
	Client   string
}

// VaultManifestRepository 清单仓储
type VaultManifestRepository interface {
	GetKey(uid int64) string

	// Current 最新快照（vault 为空时返回 nil, nil）
	Current(ctx context.Context, vaultID, uid int64) (*Manifest, error)
	// GetByEpoch 指定 epoch 的快照（客户端基线）
	GetByEpoch(ctx context.Context, epoch, vaultID, uid int64) (*Manifest, error)
	// CommitOptimistic 乐观锁提交：当前 epoch != baseEpoch 则返回 ErrEpochConflict。
	// apply 在与 FsEntry 写操作同一事务内执行，返回要落盘的新快照条目。
	CommitOptimistic(ctx context.Context, vaultID, uid int64, baseEpoch int64, apply func(store FsEntryStore) ([]ManifestItem, error)) (newEpoch int64, err error)
}

// EntryHistoryRepository 版本历史仓储
type EntryHistoryRepository interface {
	GetKey(uid int64) string
	Append(ctx context.Context, entryID string, vaultID int64, blobHash string, size, version int64, client string, uid int64) error
	ListByEntry(ctx context.Context, entryID string, uid int64) ([]EntryHistoryItem, error)
	// GetByID 按历史行 ID 取单条（REST 历史详情）
	GetByID(ctx context.Context, id, uid int64) (*EntryHistoryItem, error)
}

// EntryHistoryItem 历史版本条目
type EntryHistoryItem struct {
	ID        int64
	EntryID   string
	VaultID   int64
	BlobHash  string
	Size      int64
	Version   int64
	Client    string
	CreatedAt time.Time
}

// FsIdMapRepository 迁移映射仓储（幂等依据）
type FsIdMapRepository interface {
	GetKey(uid int64) string
	Get(ctx context.Context, oldType string, oldID int64, uid int64) (entryID string, ok bool, err error)
	Put(ctx context.Context, oldType string, oldID int64, entryID string, uid int64) error
}

// BlobStore 内容寻址 blob 存取（*dao.Dao 直接满足该接口）
type BlobStore interface {
	BlobExists(uid int64, hash string) bool
	BlobStoreFromTemp(uid int64, tempPath, expectedHash string) error
	// BlobStoreFromBytes 写入字节并返回内容哈希（服务器侧写入路径：REST/MCP/历史恢复）
	BlobStoreFromBytes(uid int64, data []byte) (string, error)
	BlobOpen(uid int64, hash string) (io.ReadCloser, error)
	BlobReadAll(uid int64, hash string) ([]byte, error)
	BlobSize(uid int64, hash string) (int64, bool)
}
