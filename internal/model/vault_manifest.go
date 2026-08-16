// Package model: vault_manifest 属于 git 式快照同步（WS v3）的新数据层，手写维护，非 gorm-gen 生成。

package model

import "github.com/haierkeys/fast-note-sync-service/pkg/timex"

const TableNameVaultManifest = "vault_manifest"

// VaultManifest vault 的版本化快照（权威状态）。ID 单调递增 = epoch，
// 客户端只记住 epoch，对账时服务器 diff 两个 epoch 的快照。
type VaultManifest struct {
	ID        int64      `gorm:"column:id;primaryKey" json:"id" form:"id"`
	VaultID   int64      `gorm:"column:vault_id;not null;index;default:0" json:"vaultId" form:"vaultId"`
	Tree      string     `gorm:"column:tree;type:text;not null" json:"tree" form:"tree"`
	CreatedAt timex.Time `gorm:"column:created_at;default:NULL;autoCreateTime:false" json:"createdAt" form:"createdAt"`
}

func (*VaultManifest) TableName() string {
	return TableNameVaultManifest
}
