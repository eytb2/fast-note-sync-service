// Package model: fs_entry 属于 git 式快照同步（WS v3）的新数据层，手写维护，非 gorm-gen 生成。

package model

import "github.com/haierkeys/fast-note-sync-service/pkg/timex"

const TableNameFsEntry = "fs_entry"

// FsEntry vault 内一个文件的登记行（笔记/附件/配置统一）。
// 身份 = ID（UUID，终身不变）；path 只是当前位置属性。
// 重命名/移动 = UPDATE path，行与 blob_hash 不变，历史与引用不断链。
type FsEntry struct {
	ID        string `gorm:"column:id;primaryKey;type:varchar(36)" json:"id" form:"id"`
	VaultID   int64  `gorm:"column:vault_id;not null;index:idx_fs_entry_vault_live,priority:1;index:idx_fs_entry_vault_deleted,priority:1;default:0" json:"vaultId" form:"vaultId"`
	IsNote    bool   `gorm:"column:is_note;not null;default:false" json:"isNote" form:"isNote"`
	Path      string `gorm:"column:path;type:varchar(1024);not null;index:idx_fs_entry_vault_live,priority:2;default:''" json:"path" form:"path"`
	BlobHash  string `gorm:"column:blob_hash;type:varchar(64);not null;index;default:''" json:"blobHash" form:"blobHash"`
	Size      int64  `gorm:"column:size;not null;default:0" json:"size" form:"size"`
	Ctime     int64  `gorm:"column:ctime;not null;default:0" json:"ctime" form:"ctime"`
	Mtime     int64  `gorm:"column:mtime;not null;default:0" json:"mtime" form:"mtime"`
	Deleted   bool   `gorm:"column:deleted;not null;index:idx_fs_entry_vault_deleted,priority:2;default:false" json:"deleted" form:"deleted"`
	DeletedAt int64  `gorm:"column:deleted_at;not null;default:0" json:"deletedAt" form:"deletedAt"`
	// 客户端信息仅作审计展示，不参与一致性
	ClientName string     `gorm:"column:client_name;type:varchar(255);default:''" json:"clientName" form:"clientName"`
	CreatedAt  timex.Time `gorm:"column:created_at;default:NULL;autoCreateTime:false" json:"createdAt" form:"createdAt"`
	UpdatedAt  timex.Time `gorm:"column:updated_at;default:NULL;autoUpdateTime:false" json:"updatedAt" form:"updatedAt"`
}

func (*FsEntry) TableName() string {
	return TableNameFsEntry
}
