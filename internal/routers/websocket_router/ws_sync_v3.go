// websocket_router: WS v3 git 式快照同步处理器。
// 动作集见 git-sync-redesign.md §2：V3Sync（对账）→ V3SyncPlan + V3BlobNeed + V3BlobPage（笔记内联）；
// V3Commit（提交）→ V3CommitAck + V3NotifyManifest（广播，纯优化）；
// V3BlobUpload（会话/秒传）+ "01" 二进制分块 → V3BlobUploadAck；V3BlobDownload → V3BlobChunk。
package websocket_router

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/haierkeys/fast-note-sync-service/internal/app"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/convert"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"go.uber.org/zap"
)

// SyncV3WSHandler WS v3 快照同步处理器
type SyncV3WSHandler struct {
	*WSHandler
}

// NewSyncV3WSHandler creates SyncV3WSHandler instance
// NewSyncV3WSHandler 创建 SyncV3WSHandler 实例
func NewSyncV3WSHandler(a *app.App) *SyncV3WSHandler {
	return &SyncV3WSHandler{WSHandler: NewWSHandler(a)}
}

// v3InlineNoteMaxBytes 笔记文本内联上限（超过则客户端走 V3BlobDownload 分块）
const v3InlineNoteMaxBytes = 512 * 1024

// v3UploadSessionTimeoutDefault blob 上传会话默认超时
const v3UploadSessionTimeoutDefault = 20 * time.Minute

// clientTag 历史记录用的客户端标识
func v3ClientTag(c *pkgapp.WebsocketClient) string {
	return c.ClientType() + "/" + c.ClientName()
}

// getChunkSizeFromConfig 从注入的配置获取分片大小, 默认为 512KB
func getChunkSizeFromConfig(cfg *app.AppConfig) int64 {
	return util.ParseSize(cfg.App.FileChunkSize, 1024*512)
}

// UserInfo 握手后加载用户信息（UseUserVerify 回调）
// UserInfo loads user info after handshake (UseUserVerify callback)
func (h *WSHandler) UserInfo(c *pkgapp.WebsocketClient, uid int64) (*pkgapp.UserSelectEntity, error) {
	ctx := c.Context()
	user, err := h.App.UserService.GetInfo(ctx, uid)

	var userEntity *pkgapp.UserSelectEntity
	if user != nil {
		userEntity = convert.StructAssign(user, &pkgapp.UserSelectEntity{}).(*pkgapp.UserSelectEntity)
	}

	return userEntity, err
}

// Sync 处理 V3Sync 对账请求：BlobNeed ×N → 笔记内联 BlobPage ×N → SyncPlan（终结帧）
func (h *SyncV3WSHandler) Sync(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage) {
	params := &dto.V3SyncRequest{}
	valid, errs := c.BindAndValidWithAction(msg.Type, msg.Data, params)
	if !valid {
		h.respondErrorWithData(c, code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()), errs, errs.MapsToString(), "websocket_router.v3.Sync.BindAndValid", msg)
		return
	}

	ctx := c.Context()
	plan, needs, err := h.App.SyncV3Service.SyncPlan(ctx, c.User.UID, params.Vault, params)
	if err != nil {
		h.respondError(c, code.ErrorV3SyncPlanFailed, err, "websocket_router.v3.Sync.SyncPlan", msg)
		return
	}

	// needs/pages 先推、SyncPlan 最后推：plan 即"响应终结帧"。
	// 客户端见到 plan 立即 settle——消除纯防抖 settle 的竞态（大清单冷启动时
	// plan 与 needs 帧间隔可超过防抖窗，客户端会以 needs=[] 提前结算 →
	// 跳过上传直接 commit → 服务端 545 blob missing）。
	for i := range needs {
		c.ToResponse(code.Success.WithData(needs[i]).WithVault(params.Vault), string(V3BlobNeed))
	}

	// 笔记文本内联（超限/读失败走分块下载，不阻断对账）
	for i := range plan.Ops {
		op := &plan.Ops[i]
		if op.Kind != reconcile.OpPull || !op.Item.IsNote || op.Item.BlobHash == "" {
			continue
		}
		content, ok, err := h.App.SyncV3Service.ReadBlobInline(ctx, c.User.UID, op.Item.BlobHash, v3InlineNoteMaxBytes)
		if err != nil {
			h.logWarn(c, "websocket_router.v3.Sync.ReadBlobInline", zap.String("hash", op.Item.BlobHash), zap.Error(err))
			continue
		}
		if !ok {
			continue
		}
		c.ToResponse(code.Success.WithData(dto.V3BlobPageMessage{
			Vault: params.Vault, Path: op.Item.Path, Hash: op.Item.BlobHash,
			Size: op.Item.Size, IsNote: true, Content: string(content),
		}).WithVault(params.Vault), string(V3BlobPage))
	}

	c.ToResponse(code.Success.WithData(plan).WithVault(params.Vault), string(V3SyncPlan))
}

// Commit 处理 V3Commit 清单提交：ack + 广播 NotifyManifest（排除自己，纯优化可丢）
func (h *SyncV3WSHandler) Commit(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage) {
	params := &dto.V3ManifestCommitRequest{}
	valid, errs := c.BindAndValidWithAction(msg.Type, msg.Data, params)
	if !valid {
		h.respondErrorWithData(c, code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()), errs, errs.MapsToString(), "websocket_router.v3.Commit.BindAndValid", msg)
		return
	}

	ack, notifyOps, err := h.App.SyncV3Service.Commit(c.Context(), c.User.UID, params.Vault, params, v3ClientTag(c))
	if err != nil {
		// epoch 冲突等以 *code.Code 形式返回（附带 currentEpoch），其余包装为提交失败
		h.respondError(c, code.ErrorV3CommitFailed, err, "websocket_router.v3.Commit.Commit", msg)
		return
	}

	c.ToResponse(code.Success.WithData(ack).WithVault(params.Vault), string(V3CommitAck))
	c.BroadcastResponse(code.Success.WithData(dto.V3NotifyManifestMessage{
		Vault: params.Vault, NewEpoch: ack.NewEpoch, Ops: notifyOps,
	}).WithVault(params.Vault), true, V3NotifyManifest)
}

// BlobUploadOpen 处理 V3BlobUpload：blob 已存在 → 秒传；否则开分块上传会话
func (h *SyncV3WSHandler) BlobUploadOpen(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage) {
	params := &dto.V3BlobUploadOpenRequest{}
	valid, errs := c.BindAndValidWithAction(msg.Type, msg.Data, params)
	if !valid {
		h.respondErrorWithData(c, code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()), errs, errs.MapsToString(), "websocket_router.v3.BlobUploadOpen.BindAndValid", msg)
		return
	}

	uid := c.User.ID // 会话表以字符串 uid 索引（与旧协议一致）

	// 秒传
	if h.App.SyncV3Service.BlobExists(c.User.UID, params.Hash) {
		c.ToResponse(code.Success.WithData(dto.V3BlobUploadOpenMessage{
			Vault: params.Vault, Hash: params.Hash, Exists: true,
		}).WithVault(params.Vault), string(V3BlobUploadOpenAck))
		return
	}

	// 空文件（0 字节）无分块可发：TotalChunks=0 的会话永远凑不齐「全部到齐」判定，
	// 客户端会等 ack 到超时（生产首灌实测：仅一个 0 字节 md 卡死整轮）。
	// 直接落空 blob（同一 Finalize 哈希校验路径，谎报 size=0 会被拒）并以秒传回包。
	if params.Size == 0 {
		tempDir0 := h.App.Config().App.TempPath
		if tempDir0 == "" {
			tempDir0 = "storage/temp"
		}
		if err := os.MkdirAll(filepath.Join(tempDir0, "blob"), 0754); err != nil {
			h.respondError(c, code.ErrorV3CommitFailed, err, "websocket_router.v3.BlobUploadOpen.EmptyBlob.MkdirAll", msg)
			return
		}
		emptyTemp := filepath.Join(tempDir0, "blob", uuid.New().String()+".tmp")
		if err := os.WriteFile(emptyTemp, nil, 0644); err != nil {
			h.respondError(c, code.ErrorV3CommitFailed, err, "websocket_router.v3.BlobUploadOpen.EmptyBlob.Write", msg)
			return
		}
		if err := h.App.SyncV3Service.FinalizeBlobUpload(c.Context(), c.User.UID, params.Hash, emptyTemp); err != nil {
			_ = os.Remove(emptyTemp)
			h.respondError(c, code.ErrorV3BlobHashInvalid, err, "websocket_router.v3.BlobUploadOpen.EmptyBlob.Finalize", msg)
			return
		}
		c.ToResponse(code.Success.WithData(dto.V3BlobUploadOpenMessage{
			Vault: params.Vault, Hash: params.Hash, Exists: true,
		}).WithVault(params.Vault), string(V3BlobUploadOpenAck))
		return
	}

	// 同哈希活跃会话复用（断线重传场景）
	if existing := c.Server.GetSessionByPathHash(uid, params.Hash); existing != nil {
		if session, ok := existing.(*BlobUploadChunkSession); ok && session.Size == params.Size {
			c.ToResponse(code.Success.WithData(dto.V3BlobUploadOpenMessage{
				Vault: params.Vault, Hash: params.Hash,
				SessionID: session.ID, ChunkSize: session.ChunkSize, TotalChunks: session.TotalChunks,
			}).WithVault(params.Vault), string(V3BlobUploadOpenAck))
			return
		}
	}
	c.Server.CleanSessionsByPathHash(uid, params.Hash)

	cfg := h.App.Config()
	tempDir := cfg.App.TempPath
	if tempDir == "" {
		tempDir = "storage/temp"
	}
	if err := os.MkdirAll(filepath.Join(tempDir, "blob"), 0754); err != nil {
		h.respondError(c, code.ErrorV3CommitFailed, err, "websocket_router.v3.BlobUploadOpen.MkdirAll", msg)
		return
	}
	tempPath := filepath.Join(tempDir, "blob", uuid.New().String()+".tmp")

	chunkSize := getChunkSizeFromConfig(cfg)
	session := &BlobUploadChunkSession{
		ID:             uuid.New().String(),
		Vault:          params.Vault,
		Hash:           params.Hash,
		Size:           params.Size,
		ChunkSize:      chunkSize,
		TotalChunks:    util.Ceil(params.Size, chunkSize),
		SavePath:       tempPath,
		CreatedAt:      time.Now(),
		uploadedChunks: make(map[uint32]struct{}),
	}

	// 超时回收
	timeout := v3UploadSessionTimeoutDefault
	if cfg.App.UploadSessionTimeout != "" && cfg.App.UploadSessionTimeout != "0" {
		if d, err := util.ParseDuration(cfg.App.UploadSessionTimeout); err == nil {
			timeout = d
		}
	}
	sessionID := session.ID
	timer := time.AfterFunc(timeout, func() {
		if cur := c.Server.GetSession(uid, sessionID); cur != nil {
			if bs, ok := cur.(*BlobUploadChunkSession); ok {
				bs.Cleanup()
			}
			c.Server.RemoveSession(uid, sessionID)
		}
	})
	session.CancelFunc = func() { timer.Stop() }

	c.Server.SetSession(uid, session.ID, session)

	c.ToResponse(code.Success.WithData(dto.V3BlobUploadOpenMessage{
		Vault: params.Vault, Hash: params.Hash,
		SessionID: session.ID, ChunkSize: session.ChunkSize, TotalChunks: session.TotalChunks,
	}).WithVault(params.Vault), string(V3BlobUploadOpenAck))
}

// BlobUploadChunkBinary 处理 "01" 二进制分块（帧格式沿用 "00"：36B 会话 ID + 4B 序号 + 数据）
func (h *SyncV3WSHandler) BlobUploadChunkBinary(c *pkgapp.WebsocketClient, data []byte) {
	select {
	case <-c.Context().Done():
		return
	default:
	}

	if len(data) < 40 {
		h.logError(c, "websocket_router.v3.BlobUploadChunkBinary", fmt.Errorf("invalid data length: %d", len(data)))
		return
	}

	sessionID := string(data[:36])
	chunkIndex := binary.BigEndian.Uint32(data[36:40])
	chunkData := data[40:]

	raw := c.Server.GetSession(c.User.ID, sessionID)
	if raw == nil {
		c.ToResponse(code.ErrorV3SessionNotFound.WithData(map[string]string{"sessionID": sessionID}))
		return
	}
	session, ok := raw.(*BlobUploadChunkSession)
	if !ok {
		c.ToResponse(code.ErrorV3SessionNotFound.WithData(map[string]string{"sessionID": sessionID}))
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.isCompleted {
		// 幂等：迟到分片直接补发完成 ack
		c.ToResponse(code.Success.WithData(dto.V3BlobUploadAckMessage{Vault: session.Vault, Hash: session.Hash, Size: session.Size}), string(V3BlobUploadAck))
		return
	}

	if _, dup := session.uploadedChunks[chunkIndex]; dup {
		return // 重复分片静默忽略
	}

	if session.FileHandle == nil {
		f, err := os.OpenFile(session.SavePath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			h.logError(c, "websocket_router.v3.BlobUploadChunkBinary.OpenFile", err)
			c.ToResponse(code.ErrorV3CommitFailed.WithDetails(err.Error()))
			return
		}
		session.FileHandle = f
	}

	offset := int64(chunkIndex) * session.ChunkSize
	if _, err := session.FileHandle.WriteAt(chunkData, offset); err != nil {
		h.logError(c, "websocket_router.v3.BlobUploadChunkBinary.WriteAt", err)
		c.ToResponse(code.ErrorV3CommitFailed.WithDetails(err.Error()))
		return
	}
	session.uploadedChunks[chunkIndex] = struct{}{}

	if int64(len(session.uploadedChunks)) < session.TotalChunks {
		return
	}

	// 全部到齐：收尾（哈希校验 → 移入 blob store → ack → 清会话）
	session.isCompleted = true
	if session.FileHandle != nil {
		_ = session.FileHandle.Close()
		session.FileHandle = nil
	}
	if err := h.App.SyncV3Service.FinalizeBlobUpload(c.Context(), c.User.UID, session.Hash, session.SavePath); err != nil {
		// 哈希不符即整体作废：清会话与临时文件，客户端下一轮重开新会话全量重传。
		// （保留旧会话会让重传分片全部命中去重标记，永远校验同一份坏文件。
		//   Cleanup 自带 SavePath 删文件，切忌先清 SavePath 再 Cleanup —— 那样临时文件永远漏删）
		session.Cleanup()
		c.Server.RemoveSession(c.User.ID, sessionID)
		h.respondError(c, code.ErrorV3BlobHashInvalid, err, "websocket_router.v3.BlobUploadChunkBinary.Finalize", nil)
		return
	}
	vault := session.Vault
	hash, size := session.Hash, session.Size
	session.SavePath = "" // 文件已被移走，Cleanup 不再删除
	session.Cleanup()
	c.Server.RemoveSession(c.User.ID, sessionID)
	c.ToResponse(code.Success.WithData(dto.V3BlobUploadAckMessage{Vault: vault, Hash: hash, Size: size}).WithVault(vault), string(V3BlobUploadAck))
}

// BlobDownload 处理 V3BlobDownload：按内容哈希读取单个分块（base64）
func (h *SyncV3WSHandler) BlobDownload(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage) {
	params := &dto.V3BlobDownloadRequest{}
	valid, errs := c.BindAndValidWithAction(msg.Type, msg.Data, params)
	if !valid {
		h.respondErrorWithData(c, code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()), errs, errs.MapsToString(), "websocket_router.v3.BlobDownload.BindAndValid", msg)
		return
	}

	rc, size, err := h.App.SyncV3Service.OpenBlob(c.Context(), c.User.UID, params.Hash)
	if err != nil {
		h.respondError(c, code.ErrorV3BlobNotFound, err, "websocket_router.v3.BlobDownload.OpenBlob", msg)
		return
	}
	defer func() { _ = rc.Close() }()

	seeker, ok := rc.(io.Seeker)
	if !ok {
		h.respondError(c, code.ErrorV3BlobNotFound, fmt.Errorf("blob not seekable"), "websocket_router.v3.BlobDownload.Seek", msg)
		return
	}

	chunkSize := getChunkSizeFromConfig(h.App.Config())
	offset := int64(params.ChunkIndex) * chunkSize
	if offset > size {
		h.respondError(c, code.ErrorInvalidParams, fmt.Errorf("chunk index out of range"), "websocket_router.v3.BlobDownload.Range", msg)
		return
	}
	if _, err := seeker.Seek(offset, io.SeekStart); err != nil {
		h.respondError(c, code.ErrorV3BlobNotFound, err, "websocket_router.v3.BlobDownload.Seek", msg)
		return
	}
	readLen := chunkSize
	if size-offset < readLen {
		readLen = size - offset
	}
	buf := make([]byte, readLen)
	if _, err := io.ReadFull(rc, buf); err != nil {
		h.respondError(c, code.ErrorV3BlobNotFound, err, "websocket_router.v3.BlobDownload.Read", msg)
		return
	}

	c.ToResponse(code.Success.WithData(dto.V3BlobChunkMessage{
		Vault:       params.Vault,
		Hash:        params.Hash,
		ChunkIndex:  params.ChunkIndex,
		TotalChunks: util.Ceil(size, chunkSize),
		ChunkSize:   chunkSize,
		Size:        size,
		Data:        base64.StdEncoding.EncodeToString(buf),
	}).WithVault(params.Vault), string(V3BlobChunk))
}

// ==================== blob 分块上传会话 ====================

// BlobUploadChunkSession blob 分块上传会话（内容寻址，PathHash 即内容哈希）
type BlobUploadChunkSession struct {
	ID             string
	Vault          string
	Hash           string // SHA-256，同时充当 PathHash（会话复用键）
	Size           int64
	ChunkSize      int64
	TotalChunks    int64
	SavePath       string
	FileHandle     *os.File
	mu             sync.Mutex
	CreatedAt      time.Time
	CancelFunc     func() // 超时回收取消
	uploadedChunks map[uint32]struct{}
	isCompleted    bool
}

// Cleanup releases session resources
// Cleanup 释放会话资源
func (s *BlobUploadChunkSession) Cleanup() {
	if s.CancelFunc != nil {
		s.CancelFunc()
		s.CancelFunc = nil
	}
	if s.FileHandle != nil {
		_ = s.FileHandle.Close()
		s.FileHandle = nil
	}
	if s.SavePath != "" {
		if _, err := os.Stat(s.SavePath); err == nil {
			_ = os.Remove(s.SavePath)
		}
		s.SavePath = ""
	}
}

// GetPathHash returns the path hash of the session (content hash)
// GetPathHash 返回会话的路径哈希（即内容哈希）
func (s *BlobUploadChunkSession) GetPathHash() string {
	return s.Hash
}

// GetCreatedAt returns the creation time
// GetCreatedAt 返回会话创建时间
func (s *BlobUploadChunkSession) GetCreatedAt() time.Time {
	return s.CreatedAt
}
