// Package model: entry_history / id_map 属于 git 式快照同步（WS v3）的新数据层，手写维护，非 gorm-gen 生成。

package model

import "github.com/haierkeys/fast-note-sync-service/pkg/timex"

const TableNameEntryHistory = "entry_history"

// EntryHistory 挂在稳定 FsEntry.ID 上的版本历史（原 note_history 的后继）。
type EntryHistory struct {
	ID        int64      `gorm:"column:id;primaryKey" json:"id" form:"id"`
	EntryID   string     `gorm:"column:entry_id;type:varchar(36);not null;index;default:''" json:"entryId" form:"entryId"`
	VaultID   int64      `gorm:"column:vault_id;not null;default:0" json:"vaultId" form:"vaultId"`
	BlobHash  string     `gorm:"column:blob_hash;type:varchar(64);not null;default:''" json:"blobHash" form:"blobHash"`
	Size      int64      `gorm:"column:size;not null;default:0" json:"size" form:"size"`
	Version   int64      `gorm:"column:version;not null;default:0" json:"version" form:"version"`
	Client    string     `gorm:"column:client;type:varchar(255);default:''" json:"client" form:"client"`
	CreatedAt timex.Time `gorm:"column:created_at;default:NULL;autoCreateTime:false" json:"createdAt" form:"createdAt"`
}

func (*EntryHistory) TableName() string {
	return TableNameEntryHistory
}

const TableNameIdMap = "fs_id_map"

// FsIdMap 旧协议行 → 新 fs_entry 的映射，平迁幂等依据（见设计文档 §4）。
type FsIdMap struct {
	ID        int64      `gorm:"column:id;primaryKey" json:"id" form:"id"`
	OldType   string     `gorm:"column:old_type;type:varchar(16);not null;index:idx_fs_id_map_old,priority:1;default:''" json:"oldType" form:"oldType"`
	OldID     int64      `gorm:"column:old_id;not null;index:idx_fs_id_map_old,priority:2;default:0" json:"oldId" form:"oldId"`
	EntryID   string     `gorm:"column:entry_id;type:varchar(36);not null;index;default:''" json:"entryId" form:"entryId"`
	CreatedAt timex.Time `gorm:"column:created_at;default:NULL;autoCreateTime:false" json:"createdAt" form:"createdAt"`
}

func (*FsIdMap) TableName() string {
	return TableNameIdMap
}
