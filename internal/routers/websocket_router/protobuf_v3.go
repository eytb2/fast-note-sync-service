// websocket_router: WS v3 协议的 protobuf 编解码钩子（P8）。
// 协商与帧路由都在 pkg/app/websocket.go：连接升级（protocol=protobuf + ClientInfo.protobuf /
// pb=1&pv=2 提前升级）后，客户端二进制帧走 "pb" 前缀 → EnvelopeDecoder 解信封 → 拦截器 → 同一批
// v3 handler；应答经 ProtobufEncoder 编为 "pb" 帧。动作集与 JSON 帧完全一致（git-sync-redesign.md §2）。
//
// 编码策略：
//   - 已知 v3 动作 → 纯 pb 消息体；
//   - 握手应答（Authorization/ClientInfo，data 为任意 map）→ pb 信封 + JSON 载荷（与 JSON 帧逐字相同）；
//   - 未知动作（含错误应答 data=nil）→ 同上兜底，客户端信封层统一解包。
package websocket_router

import (
	"fmt"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/proto/v3"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/json"
	"google.golang.org/protobuf/proto"
)

// RegisterV3ProtobufHooks 注册 v3 动作集的 protobuf 编解码钩子
func RegisterV3ProtobufHooks(wss *pkgapp.WebsocketServer) {
	wss.EnvelopeDecoder = deV3ReceivePacket
	wss.ProtobufDecoder = deV3ReceiveToDTO
	wss.ProtobufEncoder = enV3SendDTO
}

// ==================== 入站：信封 + 动作消息体 ====================

// deV3ReceivePacket 解包 "pb" 帧的 WSMessage 信封（type + data）
func deV3ReceivePacket(data []byte) (WebSocketReceiveAction, []byte, error) {
	var env v3.WSMessage
	if err := proto.Unmarshal(data, &env); err != nil {
		return "", nil, err
	}
	return WebSocketReceiveAction(env.Type), env.Data, nil
}

// deV3ReceiveToDTO 将已知 v3 动作的 pb 消息体映射进目标 DTO；未知动作返回 false 走 JSON 绑定。
func deV3ReceiveToDTO(action WebSocketReceiveAction, data []byte, obj any) (bool, error) {
	switch action {
	case ClientReceiveInfo:
		var pb v3.ClientInfoMessage
		if err := proto.Unmarshal(data, &pb); err != nil {
			return false, err
		}
		dest, ok := obj.(*pkgapp.ClientInfoMessage)
		if !ok {
			return false, nil
		}
		dest.Name = pb.Name
		dest.Version = pb.Version
		dest.Type = pb.Type
		dest.IsDesktop = pb.IsDesktop
		dest.IsMobile = pb.IsMobile
		dest.IsPhone = pb.IsPhone
		dest.IsTablet = pb.IsTablet
		dest.IsMacOS = pb.IsMacOs
		dest.IsWin = pb.IsWin
		dest.IsLinux = pb.IsLinux
		dest.OfflineSyncStrategy = pb.OfflineSyncStrategy
		dest.Protobuf = pb.Protobuf
		return true, nil

	case V3ReceiveSync:
		var pb v3.V3SyncRequest
		if err := proto.Unmarshal(data, &pb); err != nil {
			return false, err
		}
		dest, ok := obj.(*dto.V3SyncRequest)
		if !ok {
			return false, nil
		}
		dest.Vault = pb.Vault
		dest.BaseEpoch = pb.BaseEpoch
		dest.Manifest = pbItemsToDTO(pb.Manifest)
		dest.Tombstones = pbTombsToDTO(pb.Tombstones)
		if pb.Scope != nil {
			dest.Scope = &reconcile.Scope{
				Include: pb.Scope.Include,
				Exclude: pb.Scope.Exclude,
				Types:   pb.Scope.Types,
			}
		}
		return true, nil

	case V3ReceiveCommit:
		var pb v3.V3ManifestCommitRequest
		if err := proto.Unmarshal(data, &pb); err != nil {
			return false, err
		}
		dest, ok := obj.(*dto.V3ManifestCommitRequest)
		if !ok {
			return false, nil
		}
		dest.Vault = pb.Vault
		dest.BaseEpoch = pb.BaseEpoch
		dest.Changes = pbChangesToDTO(pb.Changes)
		return true, nil

	case V3ReceiveBlobUploadOpen:
		var pb v3.V3BlobUploadOpenRequest
		if err := proto.Unmarshal(data, &pb); err != nil {
			return false, err
		}
		dest, ok := obj.(*dto.V3BlobUploadOpenRequest)
		if !ok {
			return false, nil
		}
		dest.Vault = pb.Vault
		dest.Hash = pb.Hash
		dest.Size = pb.Size
		return true, nil

	case V3ReceiveBlobDownload:
		var pb v3.V3BlobDownloadRequest
		if err := proto.Unmarshal(data, &pb); err != nil {
			return false, err
		}
		dest, ok := obj.(*dto.V3BlobDownloadRequest)
		if !ok {
			return false, nil
		}
		dest.Vault = pb.Vault
		dest.Hash = pb.Hash
		dest.ChunkIndex = pb.ChunkIndex
		return true, nil
	}
	return false, nil
}

// ==================== 出站：应答信封 + 动作消息体 ====================

// enV3SendDTO 将应答编为 "pb" 帧（含 2 字节前缀）。载荷：已知 v3 动作纯 pb，其余 JSON 兜底。
func enV3SendDTO(action WebSocketSendAction, res *pkgapp.Res) ([]byte, error) {
	var innerData []byte
	var err error

	if res.Data != nil {
		innerData, err = enV3DataPayload(action, res.Data)
		if err != nil {
			return nil, err
		}
	}

	var pageIndex int32
	if pi, ok := res.PageIndex.(int); ok {
		pageIndex = int32(pi)
	}

	wsResp := &v3.WSResponse{
		Code:      int32(res.Code),
		Status:    res.Status,
		Message:   pbFormatString(res.Message),
		Data:      innerData,
		Details:   pbFormatString(res.Details),
		Vault:     pbFormatString(res.Vault),
		Context:   pbFormatString(res.Context),
		PageIndex: pageIndex,
	}

	wsRespBytes, err := proto.Marshal(wsResp)
	if err != nil {
		return nil, err
	}

	envelopeBytes, err := proto.Marshal(&v3.WSMessage{Type: string(action), Data: wsRespBytes})
	if err != nil {
		return nil, err
	}

	// "pb" 前缀（与 pkg/app OnMessage 的二进制帧路由约定一致）
	result := make([]byte, 2+len(envelopeBytes))
	result[0] = 'p'
	result[1] = 'b'
	copy(result[2:], envelopeBytes)
	return result, nil
}

// enV3DataPayload 按动作序列化载荷；未知动作 JSON 兜底（客户端信封层统一，载荷按 JSON 解）。
func enV3DataPayload(action WebSocketSendAction, data any) ([]byte, error) {
	switch action {
	case V3SyncPlan:
		src, ok := data.(dto.V3SyncPlanMessage)
		if !ok {
			break
		}
		return proto.Marshal(&v3.V3SyncPlanMessage{
			Vault:       src.Vault,
			ServerEpoch: src.ServerEpoch,
			BaseEpoch:   src.BaseEpoch,
			Ops:         dtoOpsToPb(src.Ops),
			Conflicts:   dtoConflictsToPb(src.Conflicts),
			Expected:    dtoChangesToPb(src.Expected),
		})

	case V3BlobNeed:
		if src, ok := data.(dto.V3BlobNeedMessage); ok {
			return proto.Marshal(&v3.V3BlobNeedMessage{
				Vault: src.Vault, Path: src.Path, Hash: src.Hash, Size: src.Size,
			})
		}

	case V3BlobPage:
		if src, ok := data.(dto.V3BlobPageMessage); ok {
			return proto.Marshal(&v3.V3BlobPageMessage{
				Vault: src.Vault, Path: src.Path, Hash: src.Hash,
				Size: src.Size, IsNote: src.IsNote, Content: src.Content,
			})
		}

	case V3CommitAck:
		if src, ok := data.(dto.V3ManifestCommitAckMessage); ok {
			items := make([]*v3.V3CommitAckItem, len(src.Items))
			for i, it := range src.Items {
				items[i] = &v3.V3CommitAckItem{Path: it.Path, Id: it.ID}
			}
			return proto.Marshal(&v3.V3ManifestCommitAckMessage{
				Vault: src.Vault, NewEpoch: src.NewEpoch, Items: items,
			})
		}

	case V3NotifyManifest:
		if src, ok := data.(dto.V3NotifyManifestMessage); ok {
			return proto.Marshal(&v3.V3NotifyManifestMessage{
				Vault: src.Vault, NewEpoch: src.NewEpoch, Ops: dtoOpsToPb(src.Ops),
			})
		}

	case V3BlobUploadOpenAck:
		if src, ok := data.(dto.V3BlobUploadOpenMessage); ok {
			return proto.Marshal(&v3.V3BlobUploadOpenMessage{
				Vault: src.Vault, Hash: src.Hash, SessionId: src.SessionID,
				ChunkSize: src.ChunkSize, TotalChunks: src.TotalChunks, Exists: src.Exists,
			})
		}

	case V3BlobUploadAck:
		if src, ok := data.(dto.V3BlobUploadAckMessage); ok {
			return proto.Marshal(&v3.V3BlobUploadAckMessage{
				Vault: src.Vault, Hash: src.Hash, Size: src.Size,
			})
		}

	case V3BlobChunk:
		if src, ok := data.(dto.V3BlobChunkMessage); ok {
			return proto.Marshal(&v3.V3BlobChunkMessage{
				Vault: src.Vault, Hash: src.Hash, ChunkIndex: src.ChunkIndex,
				TotalChunks: src.TotalChunks, ChunkSize: src.ChunkSize,
				Size: src.Size, Data: src.Data,
			})
		}

	case ShareSyncRefresh:
		// 无消息体：空载荷
		return nil, nil
	}

	// 兜底：JSON 载荷（握手应答等任意 map；与 JSON 帧的载荷逐字相同）
	return json.Marshal(data)
}

// ==================== 映射辅助 ====================

func pbItemsToDTO(items []*v3.ManifestItem) []domain.ManifestItem {
	out := make([]domain.ManifestItem, 0, len(items))
	for _, it := range items {
		if it == nil {
			continue
		}
		out = append(out, domain.ManifestItem{
			ID: it.Id, Path: it.Path, BlobHash: it.Hash, IsNote: it.IsNote,
			Size: it.Size, Mtime: it.Mtime, Ctime: it.Ctime,
		})
	}
	return out
}

func pbTombsToDTO(tombs []*v3.Tombstone) []reconcile.Tombstone {
	out := make([]reconcile.Tombstone, 0, len(tombs))
	for _, t := range tombs {
		if t == nil {
			continue
		}
		out = append(out, reconcile.Tombstone{Path: t.Path, ID: t.Id})
	}
	return out
}

func pbChangesToDTO(changes []*v3.Change) []reconcile.Change {
	out := make([]reconcile.Change, 0, len(changes))
	for _, ch := range changes {
		if ch == nil {
			continue
		}
		c := reconcile.Change{Op: ch.Op, OldPath: ch.OldPath}
		if ch.Item != nil {
			c.Item = domain.ManifestItem{
				ID: ch.Item.Id, Path: ch.Item.Path, BlobHash: ch.Item.Hash, IsNote: ch.Item.IsNote,
				Size: ch.Item.Size, Mtime: ch.Item.Mtime, Ctime: ch.Item.Ctime,
			}
		}
		out = append(out, c)
	}
	return out
}

func dtoItemToPb(it domain.ManifestItem) *v3.ManifestItem {
	return &v3.ManifestItem{
		Id: it.ID, Path: it.Path, Hash: it.BlobHash, IsNote: it.IsNote,
		Size: it.Size, Mtime: it.Mtime, Ctime: it.Ctime,
	}
}

func dtoOpsToPb(ops []reconcile.Op) []*v3.Op {
	out := make([]*v3.Op, 0, len(ops))
	for _, op := range ops {
		out = append(out, &v3.Op{Op: string(op.Kind), Item: dtoItemToPb(op.Item), From: op.From})
	}
	return out
}

func dtoConflictsToPb(cs []reconcile.Conflict) []*v3.Conflict {
	out := make([]*v3.Conflict, 0, len(cs))
	for _, c := range cs {
		out = append(out, &v3.Conflict{
			Path: c.Path, Kind: string(c.Kind), Id: c.ID,
			BaseHash: c.BaseHash, ServerHash: c.ServerHash, ServerMtime: c.ServerMtime,
			LocalHash: c.LocalHash, IsNote: c.IsNote,
		})
	}
	return out
}

func dtoChangesToPb(changes []reconcile.Change) []*v3.Change {
	out := make([]*v3.Change, 0, len(changes))
	for _, ch := range changes {
		out = append(out, &v3.Change{Op: ch.Op, OldPath: ch.OldPath, Item: dtoItemToPb(ch.Item)})
	}
	return out
}

// pbFormatString Res 的 interface{} 字段安全转字符串
func pbFormatString(v any) string {
	if v == nil {
		return ""
	}
	if str, ok := v.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", v)
}
