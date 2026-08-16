// Package dto: WS v3 git 式快照同步的消息体。
// 清单条目/墓碑/scope 直接以类型别名复用 domain 与 reconcile 的定义——
// 线上 JSON 形状只有这一份定义，客户端（插件）按同样字段实现。
package dto

import (
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
)

// V3ManifestItem 清单条目（path + 文件身份 + 内容哈希）
type V3ManifestItem = domain.ManifestItem

// V3Tombstone 客户端墓碑
type V3Tombstone = reconcile.Tombstone

// V3Scope 客户端声明范围（sparse 对账）
type V3Scope = reconcile.Scope

// V3SyncRequest C→S 对账请求（git-sync-redesign.md §2）
type V3SyncRequest struct {
	Vault      string           `json:"vault" form:"vault" binding:"required"`
	BaseEpoch  int64            `json:"baseEpoch"`
	Manifest   []V3ManifestItem `json:"manifest"`
	Tombstones []V3Tombstone    `json:"tombstones"`
	Scope      *V3Scope         `json:"scope,omitempty"`
}

// V3SyncPlanMessage S→C 对账计划
type V3SyncPlanMessage struct {
	Vault       string               `json:"vault"`
	ServerEpoch int64                `json:"serverEpoch"`
	BaseEpoch   int64                `json:"baseEpoch"`
	Ops         []reconcile.Op       `json:"ops"`       // 客户端待应用操作（pull/move/delete）
	Conflicts   []reconcile.Conflict `json:"conflicts"` // 冲突项（笔记三路合并/冲突副本）
	Expected    []reconcile.Change   `json:"expected"`  // 服务器认可的提交素材（add/modify/delete/move）
}

// V3BlobNeedMessage S→C 服务器缺失的 blob（客户端走分块上传）
type V3BlobNeedMessage struct {
	Vault string `json:"vault"`
	Path  string `json:"path"`
	Hash  string `json:"hash"`
	Size  int64  `json:"size"`
}

// V3BlobPageMessage S→C 内联 blob 内容（笔记文本；超限附件走 V3BlobDownload 分块）
type V3BlobPageMessage struct {
	Vault   string `json:"vault"`
	Path    string `json:"path"`
	Hash    string `json:"hash"`
	Size    int64  `json:"size"`
	IsNote  bool   `json:"isNote"`
	Content string `json:"content"` // utf-8 文本；超限走分块
}

// V3ManifestCommitRequest C→S 清单提交
type V3ManifestCommitRequest struct {
	Vault     string             `json:"vault" form:"vault" binding:"required"`
	BaseEpoch int64              `json:"baseEpoch"`
	Changes   []reconcile.Change `json:"changes"`
}

// V3CommitAckItem 提交后客户端应记入基线的身份映射（add 分配的 UUID / move 的最终路径）
type V3CommitAckItem struct {
	Path string `json:"path"`
	ID   string `json:"id"`
}

// V3ManifestCommitAckMessage S→C 提交确认
type V3ManifestCommitAckMessage struct {
	Vault    string            `json:"vault"`
	NewEpoch int64             `json:"newEpoch"`
	Items    []V3CommitAckItem `json:"items"` // 本次提交涉及条目的最终 (path, id)
}

// V3EpochConflictData epoch 冲突时的错误附帶数据（客户端据此重新对账）
type V3EpochConflictData struct {
	CurrentEpoch int64 `json:"currentEpoch"`
}

// V3NotifyManifestMessage S→C 实时通知（在线设备纯优化，可丢；客户端收到后重新对账）
type V3NotifyManifestMessage struct {
	Vault    string         `json:"vault"`
	NewEpoch int64          `json:"newEpoch"`
	Ops      []reconcile.Op `json:"ops"` // 自上一版清单的两方 diff（提示性，不保证完整）
}

// V3BlobUploadOpenRequest C→S 打开 blob 上传会话
type V3BlobUploadOpenRequest struct {
	Vault string `json:"vault" form:"vault" binding:"required"`
	Hash  string `json:"hash" binding:"required"`
	Size  int64  `json:"size" binding:"gte=0"`
}

// V3BlobUploadOpenMessage S→C 会话参数（Exists=true 表示服务器已有该 blob，秒传）
type V3BlobUploadOpenMessage struct {
	Vault       string `json:"vault"`
	Hash        string `json:"hash"`
	SessionID   string `json:"sessionId"`
	ChunkSize   int64  `json:"chunkSize"`
	TotalChunks int64  `json:"totalChunks"`
	Exists      bool   `json:"exists"`
}

// V3BlobUploadAckMessage S→C 分块上传完成确认
type V3BlobUploadAckMessage struct {
	Vault string `json:"vault"`
	Hash  string `json:"hash"`
	Size  int64  `json:"size"`
}

// V3BlobDownloadRequest C→S 拉取一个 blob 分块
type V3BlobDownloadRequest struct {
	Vault      string `json:"vault" form:"vault" binding:"required"`
	Hash       string `json:"hash" binding:"required"`
	ChunkIndex uint32 `json:"chunkIndex"`
}

// V3BlobChunkMessage S→C 单分块响应（base64）
type V3BlobChunkMessage struct {
	Vault       string `json:"vault"`
	Hash        string `json:"hash"`
	ChunkIndex  uint32 `json:"chunkIndex"`
	TotalChunks int64  `json:"totalChunks"`
	ChunkSize   int64  `json:"chunkSize"`
	Size        int64  `json:"size"` // blob 总大小
	Data        string `json:"data"` // base64
}
