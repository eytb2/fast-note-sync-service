package websocket_router

// WebSocketMsgType WebSocket Binary message type
// WebSocket 二进制消息类型
type WebSocketMsgType = string

// VaultBlobMsgType v3 blob chunk upload message
// v3 blob 分块上传消息（帧格式：36B 会话 ID + 4B 分块序号 + 数据）
const VaultBlobMsgType WebSocketMsgType = "01"

// WebSocketReceiveAction WebSocket text receive action type
// WebSocket 文本接收动作类型
type WebSocketReceiveAction = string

// WebSocketSendAction WebSocket text send action type
// WebSocket 文本发送动作类型
type WebSocketSendAction = string

const (
	// ClientReceiveInfo client info action
	// ClientReceiveInfo 客户端信息接收动作
	ClientReceiveInfo WebSocketReceiveAction = "ClientInfo"
	// ClientReceiveAuth client authorization action
	// ClientReceiveAuth 客户端鉴权接收动作
	ClientReceiveAuth WebSocketReceiveAction = "Authorization"

	// ClientInfo client info ack action
	// ClientInfo 客户端信息确认发送动作
	ClientInfo WebSocketSendAction = "ClientInfo"
)

const (
	// ---------------- WS v3 Snapshot Sync（git 式快照同步） ----------------

	// V3ReceiveSync reconcile request: client sends local manifest + tombstones + baseline epoch
	// V3ReceiveSync 对账请求：客户端上报本地清单 + 墓碑 + 基线 epoch
	V3ReceiveSync WebSocketReceiveAction = "V3Sync"
	// V3ReceiveCommit manifest commit: atomically apply changes (optimistic lock on baseEpoch)
	// V3ReceiveCommit 清单提交：原子应用变更（baseEpoch 乐观锁）
	V3ReceiveCommit WebSocketReceiveAction = "V3Commit"
	// V3ReceiveBlobUploadOpen open a blob chunk-upload session (instant upload if blob exists)
	// V3ReceiveBlobUploadOpen 打开 blob 分块上传会话（已存在则秒传）
	V3ReceiveBlobUploadOpen WebSocketReceiveAction = "V3BlobUpload"
	// V3ReceiveBlobDownload fetch one chunk of a blob by content hash
	// V3ReceiveBlobDownload 按内容哈希拉取一个 blob 分块
	V3ReceiveBlobDownload WebSocketReceiveAction = "V3BlobDownload"
)

const (
	// ---------------- WS v3 Snapshot Sync send ----------------

	// V3SyncPlan reconcile plan: ops for the client, conflicts, expected commit set
	// V3SyncPlan 对账计划：客户端待应用操作、冲突项、期望提交集
	V3SyncPlan WebSocketSendAction = "V3SyncPlan"
	// V3BlobNeed blob the server lacks; client should upload it in chunks
	// V3BlobNeed 服务器缺失的 blob，客户端应分块上传
	V3BlobNeed WebSocketSendAction = "V3BlobNeed"
	// V3BlobPage inline blob content for the client (note text; chunked for large attachments)
	// V3BlobPage 内联 blob 内容（笔记文本；超限走分块下载）
	V3BlobPage WebSocketSendAction = "V3BlobPage"
	// V3CommitAck manifest commit ack with the new epoch
	// V3CommitAck 清单提交确认，携带新 epoch
	V3CommitAck WebSocketSendAction = "V3CommitAck"
	// V3NotifyManifest real-time notify to other online devices (advisory, may be dropped)
	// V3NotifyManifest 其他在线设备的实时通知（纯优化，可丢）
	V3NotifyManifest WebSocketSendAction = "V3NotifyManifest"
	// V3BlobUploadOpenAck blob upload session opened (or instant-upload if exists)
	// V3BlobUploadOpenAck blob 上传会话已打开（已存在则秒传）
	V3BlobUploadOpenAck WebSocketSendAction = "V3BlobUploadOpenAck"
	// V3BlobUploadAck blob chunk upload complete ack (hash verified, stored)
	// V3BlobUploadAck blob 上传完成确认（哈希校验通过并已入库）
	V3BlobUploadAck WebSocketSendAction = "V3BlobUploadAck"
	// V3BlobChunk one chunk of a blob (base64) in response to V3BlobDownload
	// V3BlobChunk 对 V3BlobDownload 的单分块响应（base64）
	V3BlobChunk WebSocketSendAction = "V3BlobChunk"

	// ---------------- Share ----------------

	// ShareSyncRefresh notify clients to refresh share state
	// ShareSyncRefresh 通知客户端刷新分享状态
	ShareSyncRefresh WebSocketSendAction = "ShareSyncRefresh"
)
