// websocket_router: P8 protobuf 编解码测试。
// 验收口径（git-sync-redesign.md §5 P8）：同一输入经 JSON 与 PB 两种编码，解进 DTO 后语义等价；
// 出站方向 pb 帧 → 信封 → 消息体 → 与原 DTO 逐字段一致。
package websocket_router

import (
	"slices"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	v3 "github.com/haierkeys/fast-note-sync-service/internal/proto/v3"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/json"
	"google.golang.org/protobuf/proto"
)

// normItems 将 nil 与空 slice 归一（JSON 缺省键 → nil；proto3 空表 → 空 slice，语义等价）
func normItems[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	return s
}

func sampleSyncRequest() *dto.V3SyncRequest {
	return &dto.V3SyncRequest{
		Vault:     "e2e-notes",
		BaseEpoch: 42,
		Manifest: []domain.ManifestItem{
			{ID: "uuid-1", Path: "note/a.md", BlobHash: "hash-a", IsNote: true, Size: 100, Mtime: 1700000000, Ctime: 1699999999},
			{ID: "", Path: "note/新笔记.md", BlobHash: "hash-b", IsNote: true, Size: 200, Mtime: 1700000001, Ctime: 1700000001},
			{ID: "uuid-3", Path: "attach/大 附件.bin", BlobHash: "hash-c", IsNote: false, Size: 104857600, Mtime: 1700000002, Ctime: 1700000002},
		},
		Tombstones: []reconcile.Tombstone{{Path: "note/gone.md", ID: "uuid-gone"}},
		Scope: &reconcile.Scope{
			Include: []string{"note/", "re:attach/.*\\.bin$"},
			Exclude: []string{"note/secret/"},
			Types:   []string{"note", "attachment"},
		},
	}
}

func pbEncodeRequest(t *testing.T, req *dto.V3SyncRequest) []byte {
	t.Helper()
	pbMsg := &v3.V3SyncRequest{
		Vault: req.Vault, BaseEpoch: req.BaseEpoch,
		Manifest:   dtoItemsToPb(req.Manifest),
		Tombstones: dtoTombsToPb(req.Tombstones),
	}
	if req.Scope != nil {
		pbMsg.Scope = &v3.Scope{Include: req.Scope.Include, Exclude: req.Scope.Exclude, Types: req.Scope.Types}
	}
	out, err := proto.Marshal(pbMsg)
	if err != nil {
		t.Fatalf("marshal V3SyncRequest: %v", err)
	}
	return out
}

// dtoItemsToPb / dtoTombsToPb 测试侧构造（生产方向映射在 protobuf_v3.go 的出站辅助之外，
// 入站方向由 pbItemsToPb/pbTombsToPb 完成，这里复用它们做反解验证）
func dtoItemsToPb(items []domain.ManifestItem) []*v3.ManifestItem {
	out := make([]*v3.ManifestItem, 0, len(items))
	for _, it := range items {
		out = append(out, &v3.ManifestItem{
			Id: it.ID, Path: it.Path, Hash: it.BlobHash, IsNote: it.IsNote,
			Size: it.Size, Mtime: it.Mtime, Ctime: it.Ctime,
		})
	}
	return out
}

func dtoTombsToPb(tombs []reconcile.Tombstone) []*v3.Tombstone {
	out := make([]*v3.Tombstone, 0, len(tombs))
	for _, tb := range tombs {
		out = append(out, &v3.Tombstone{Path: tb.Path, Id: tb.ID})
	}
	return out
}

// TestV3Protobuf_JSONAndPBRequestsEquivalent P8 验收主用例：
// 同一请求体分别走 JSON 帧载荷与 PB 帧载荷解码，得到语义等价的 DTO。
func TestV3Protobuf_JSONAndPBRequestsEquivalent(t *testing.T) {
	src := sampleSyncRequest()

	// JSON 路径（生产 = OnMessage 文本帧 "|" 之后的 Data 直入 BindAndValid）
	jsonBytes, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	var viaJSON dto.V3SyncRequest
	if err := json.Unmarshal(jsonBytes, &viaJSON); err != nil {
		t.Fatalf("unmarshal json: %v", err)
	}

	// PB 路径（生产 = "pb" 帧 → EnvelopeDecoder → ProtobufDecoder）
	viaPB := dto.V3SyncRequest{}
	decoded, err := deV3ReceiveToDTO(V3ReceiveSync, pbEncodeRequest(t, src), &viaPB)
	if err != nil || !decoded {
		t.Fatalf("pb decode: decoded=%v err=%v", decoded, err)
	}

	if viaJSON.Vault != viaPB.Vault || viaJSON.BaseEpoch != viaPB.BaseEpoch {
		t.Fatalf("header mismatch: %+v vs %+v", viaJSON, viaPB)
	}
	if len(viaJSON.Manifest) != len(viaPB.Manifest) {
		t.Fatalf("manifest len: %d vs %d", len(viaJSON.Manifest), len(viaPB.Manifest))
	}
	for i := range viaJSON.Manifest {
		if viaJSON.Manifest[i] != viaPB.Manifest[i] {
			t.Fatalf("manifest[%d]: %+v vs %+v", i, viaJSON.Manifest[i], viaPB.Manifest[i])
		}
	}
	if len(normItems(viaJSON.Tombstones)) != len(normItems(viaPB.Tombstones)) ||
		normItems(viaJSON.Tombstones)[0] != normItems(viaPB.Tombstones)[0] {
		t.Fatalf("tombstones: %+v vs %+v", viaJSON.Tombstones, viaPB.Tombstones)
	}
	if viaJSON.Scope == nil || viaPB.Scope == nil {
		t.Fatalf("scope nil: %+v vs %+v", viaJSON.Scope, viaPB.Scope)
	}
	if !slices.Equal(viaJSON.Scope.Include, viaPB.Scope.Include) ||
		!slices.Equal(viaJSON.Scope.Exclude, viaPB.Scope.Exclude) ||
		!slices.Equal(viaJSON.Scope.Types, viaPB.Scope.Types) {
		t.Fatalf("scope: %+v vs %+v", viaJSON.Scope, viaPB.Scope)
	}
}

// TestV3Protobuf_CommitRequestRoundTrip 提交请求 pb 往返（add/modify/delete/move 四种 op 混合）
func TestV3Protobuf_CommitRequestRoundTrip(t *testing.T) {
	pbMsg := &v3.V3ManifestCommitRequest{
		Vault: "e2e-notes", BaseEpoch: 7,
		Changes: []*v3.Change{
			{Op: "add", Item: &v3.ManifestItem{Path: "note/new.md", Hash: "h1", IsNote: true, Size: 10, Mtime: 1, Ctime: 1}},
			{Op: "modify", Item: &v3.ManifestItem{Id: "uuid-1", Path: "note/a.md", Hash: "h2", IsNote: true, Size: 20, Mtime: 2, Ctime: 1}},
			{Op: "delete", Item: &v3.ManifestItem{Id: "uuid-2", Path: "note/b.md"}},
			{Op: "move", OldPath: "note/old.md", Item: &v3.ManifestItem{Id: "uuid-3", Path: "note/new-dir/new.md", Hash: "h3", IsNote: true, Size: 30, Mtime: 3, Ctime: 1}},
		},
	}
	raw, err := proto.Marshal(pbMsg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got dto.V3ManifestCommitRequest
	decoded, err := deV3ReceiveToDTO(V3ReceiveCommit, raw, &got)
	if err != nil || !decoded {
		t.Fatalf("decode: decoded=%v err=%v", decoded, err)
	}

	if got.Vault != "e2e-notes" || got.BaseEpoch != 7 || len(got.Changes) != 4 {
		t.Fatalf("header/len: %+v", got)
	}
	want := []struct {
		op, oldPath, id, path, hash string
	}{
		{"add", "", "", "note/new.md", "h1"},
		{"modify", "", "uuid-1", "note/a.md", "h2"},
		{"delete", "", "uuid-2", "note/b.md", ""},
		{"move", "note/old.md", "uuid-3", "note/new-dir/new.md", "h3"},
	}
	for i, w := range want {
		c := got.Changes[i]
		if c.Op != w.op || c.OldPath != w.oldPath || c.Item.ID != w.id || c.Item.Path != w.path || c.Item.BlobHash != w.hash {
			t.Fatalf("change[%d]: %+v want %v", i, c, w)
		}
	}
}

// TestV3Protobuf_OutgoingPlanEnvelope 出站：SyncPlan DTO → "pb" 帧 → 信封 → 消息体 → 与原 DTO 等价
func TestV3Protobuf_OutgoingPlanEnvelope(t *testing.T) {
	src := dto.V3SyncPlanMessage{
		Vault: "e2e-notes", ServerEpoch: 100, BaseEpoch: 99,
		Ops: []reconcile.Op{
			{Kind: reconcile.OpPull, Item: domain.ManifestItem{ID: "u1", Path: "note/x.md", BlobHash: "hx", IsNote: true, Size: 5, Mtime: 1, Ctime: 1}},
			{Kind: reconcile.OpMove, From: "note/old.md", Item: domain.ManifestItem{ID: "u2", Path: "note/moved.md", BlobHash: "hm", IsNote: true, Size: 6, Mtime: 2, Ctime: 1}},
			{Kind: reconcile.OpDelete, Item: domain.ManifestItem{ID: "u3", Path: "note/dead.md"}},
		},
		Conflicts: []reconcile.Conflict{
			{Path: "note/c.md", Kind: reconcile.ConflictModify, ID: "u4", BaseHash: "hb", ServerHash: "hs", ServerMtime: 9, LocalHash: "hl", IsNote: true},
			{Path: "attach/c.bin", Kind: reconcile.ConflictAdd, ServerHash: "hs2", LocalHash: "hl2", IsNote: false},
		},
		Expected: []reconcile.Change{
			{Op: "add", Item: domain.ManifestItem{Path: "note/n.md", BlobHash: "hn", IsNote: true, Size: 1, Mtime: 1, Ctime: 1}},
			{Op: "move", OldPath: "note/o.md", Item: domain.ManifestItem{ID: "u5", Path: "note/m.md", BlobHash: "hm", IsNote: true, Size: 2, Mtime: 2, Ctime: 1}},
		},
	}

	frame, err := enV3SendDTO(V3SyncPlan, &pkgapp.Res{Code: 1, Status: true, Data: src, Vault: src.Vault})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(frame[:2]) != "pb" {
		t.Fatalf("frame prefix: %q", string(frame[:2]))
	}

	var env v3.WSMessage
	if err := proto.Unmarshal(frame[2:], &env); err != nil {
		t.Fatalf("envelope: %v", err)
	}
	if env.Type != string(V3SyncPlan) {
		t.Fatalf("envelope type: %q", env.Type)
	}
	var resp v3.WSResponse
	if err := proto.Unmarshal(env.Data, &resp); err != nil {
		t.Fatalf("response: %v", err)
	}
	if resp.Code != 1 || !resp.Status || resp.Vault != "e2e-notes" {
		t.Fatalf("response header: %+v", &resp)
	}
	var plan v3.V3SyncPlanMessage
	if err := proto.Unmarshal(resp.Data, &plan); err != nil {
		t.Fatalf("plan: %v", err)
	}

	if plan.Vault != src.Vault || plan.ServerEpoch != src.ServerEpoch || plan.BaseEpoch != src.BaseEpoch {
		t.Fatalf("plan header: %+v", &plan)
	}
	if len(plan.Ops) != 3 || plan.Ops[0].Op != "pull" || plan.Ops[1].Op != "move" || plan.Ops[1].From != "note/old.md" {
		t.Fatalf("plan ops: %+v", plan.Ops)
	}
	if plan.Ops[0].Item.Hash != "hx" || plan.Ops[0].Item.Id != "u1" {
		t.Fatalf("plan op item hash/id: %+v", plan.Ops[0].Item)
	}
	if len(plan.Conflicts) != 2 || plan.Conflicts[0].Kind != "modify" || plan.Conflicts[0].BaseHash != "hb" || !plan.Conflicts[0].IsNote {
		t.Fatalf("plan conflicts: %+v", plan.Conflicts)
	}
	if len(plan.Expected) != 2 || plan.Expected[1].OldPath != "note/o.md" || plan.Expected[1].Item.Id != "u5" {
		t.Fatalf("plan expected: %+v", plan.Expected)
	}
}

// TestV3Protobuf_OutgoingErrorAndFallback 错误应答（data=nil）与未知动作 JSON 兜底
func TestV3Protobuf_OutgoingErrorAndFallback(t *testing.T) {
	// 错误应答：无 data，信封仍可解
	frame, err := enV3SendDTO(V3SyncPlan, &pkgapp.Res{Code: 541, Status: false, Message: "plan failed", Details: "boom"})
	if err != nil {
		t.Fatalf("encode error resp: %v", err)
	}
	var env v3.WSMessage
	var resp v3.WSResponse
	if err := proto.Unmarshal(frame[2:], &env); err != nil {
		t.Fatalf("envelope: %v", err)
	}
	if err := proto.Unmarshal(env.Data, &resp); err != nil {
		t.Fatalf("response: %v", err)
	}
	if resp.Code != 541 || resp.Status || resp.Message != "plan failed" || resp.Details != "boom" || len(resp.Data) != 0 {
		t.Fatalf("error resp: %+v", &resp)
	}

	// 未知动作 + 任意 map 数据 → JSON 载荷兜底
	frame, err = enV3SendDTO(ClientInfo, &pkgapp.Res{Code: 1, Status: true,
		Data: map[string]any{"githubAvailable": false, "versionIsNew": true}})
	if err != nil {
		t.Fatalf("encode fallback: %v", err)
	}
	if err := proto.Unmarshal(frame[2:], &env); err != nil {
		t.Fatalf("envelope: %v", err)
	}
	if err := proto.Unmarshal(env.Data, &resp); err != nil {
		t.Fatalf("response: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(resp.Data, &m); err != nil {
		t.Fatalf("fallback payload not json: %v", err)
	}
	if m["githubAvailable"] != false || m["versionIsNew"] != true {
		t.Fatalf("fallback payload: %+v", m)
	}
}

// TestV3Protobuf_BlobAndAckCovers 上传/下载/ack 各小消息体字段完整性
func TestV3Protobuf_BlobAndAckCovers(t *testing.T) {
	// V3BlobDownload 请求
	raw, _ := proto.Marshal(&v3.V3BlobDownloadRequest{Vault: "v", Hash: "h", ChunkIndex: 3})
	var dl dto.V3BlobDownloadRequest
	ok, err := deV3ReceiveToDTO(V3ReceiveBlobDownload, raw, &dl)
	if err != nil || !ok || dl.Hash != "h" || dl.ChunkIndex != 3 {
		t.Fatalf("blob download: ok=%v err=%v got=%+v", ok, err, dl)
	}

	// V3BlobUpload 请求
	raw, _ = proto.Marshal(&v3.V3BlobUploadOpenRequest{Vault: "v", Hash: "h2", Size: 12345})
	var ul dto.V3BlobUploadOpenRequest
	ok, err = deV3ReceiveToDTO(V3ReceiveBlobUploadOpen, raw, &ul)
	if err != nil || !ok || ul.Size != 12345 {
		t.Fatalf("blob upload: ok=%v err=%v got=%+v", ok, err, ul)
	}

	// V3BlobChunk 应答（base64 data 字段）
	frame, err := enV3SendDTO(V3BlobChunk, &pkgapp.Res{Code: 1, Status: true, Data: dto.V3BlobChunkMessage{
		Vault: "v", Hash: "h", ChunkIndex: 2, TotalChunks: 4, ChunkSize: 512, Size: 2000, Data: "aGVsbG8=",
	}})
	if err != nil {
		t.Fatalf("encode chunk: %v", err)
	}
	var env v3.WSMessage
	var resp v3.WSResponse
	_ = proto.Unmarshal(frame[2:], &env)
	_ = proto.Unmarshal(env.Data, &resp)
	var chunk v3.V3BlobChunkMessage
	if err := proto.Unmarshal(resp.Data, &chunk); err != nil {
		t.Fatalf("chunk: %v", err)
	}
	if chunk.Data != "aGVsbG8=" || chunk.ChunkIndex != 2 || chunk.TotalChunks != 4 || chunk.ChunkSize != 512 || chunk.Size != 2000 {
		t.Fatalf("chunk fields: %+v", &chunk)
	}

	// V3BlobUploadOpenAck（秒传）
	frame, err = enV3SendDTO(V3BlobUploadOpenAck, &pkgapp.Res{Code: 1, Status: true, Data: dto.V3BlobUploadOpenMessage{
		Vault: "v", Hash: "h", Exists: true,
	}})
	if err != nil {
		t.Fatalf("encode open-ack: %v", err)
	}
	_ = proto.Unmarshal(frame[2:], &env)
	_ = proto.Unmarshal(env.Data, &resp)
	var open v3.V3BlobUploadOpenMessage
	if err := proto.Unmarshal(resp.Data, &open); err != nil {
		t.Fatalf("open-ack: %v", err)
	}
	if !open.Exists || open.SessionId != "" {
		t.Fatalf("open-ack fields: %+v", &open)
	}

	// ShareSyncRefresh：无消息体
	frame, err = enV3SendDTO(ShareSyncRefresh, &pkgapp.Res{Code: 1, Status: true, Data: map[string]any{}})
	if err != nil {
		t.Fatalf("encode share refresh: %v", err)
	}
	_ = proto.Unmarshal(frame[2:], &env)
	_ = proto.Unmarshal(env.Data, &resp)
	if len(resp.Data) != 0 {
		t.Fatalf("share refresh should be empty, got %d bytes", len(resp.Data))
	}
}

// TestV3Protobuf_ClientInfoAndUnknownActions ClientInfo 解码；未知动作回 false（JSON 绑定兜底）
func TestV3Protobuf_ClientInfoAndUnknownActions(t *testing.T) {
	raw, _ := proto.Marshal(&v3.ClientInfoMessage{
		Name: "cli", Version: "1.0", Type: "FastNoteCLI",
		IsDesktop: true, IsMacOs: true, OfflineSyncStrategy: "newTimeMerge", Protobuf: true,
	})
	var info pkgapp.ClientInfoMessage
	ok, err := deV3ReceiveToDTO(ClientReceiveInfo, raw, &info)
	if err != nil || !ok {
		t.Fatalf("clientinfo: ok=%v err=%v", ok, err)
	}
	if info.Name != "cli" || !info.IsMacOS || !info.Protobuf || info.OfflineSyncStrategy != "newTimeMerge" {
		t.Fatalf("clientinfo fields: %+v", info)
	}

	// 未知动作：false + 无错 → 框架走 JSON 绑定
	ok, err = deV3ReceiveToDTO("NoteSync", []byte(`{}`), &dto.V3SyncRequest{})
	if ok || err != nil {
		t.Fatalf("unknown action should fall back: ok=%v err=%v", ok, err)
	}
}
