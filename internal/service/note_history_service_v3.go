// Package service: NoteHistoryService 的 v3 实现（P5 功能回接）。
// entry_history 随每次 v3 提交自动落档；这里只做查询与恢复（恢复=对旧 hash 提交 modify，
// 与设计 §2 一致）。diffs 在读取时计算：该版本内容 vs 相邻更新版本内容。
package service

import (
	"context"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"github.com/sergi/go-diff/diffmatchpatch"
	"go.uber.org/zap"
)

type noteHistoryServiceV3 struct {
	content  ContentV3Service
	histRepo domain.EntryHistoryRepository
	fsRepo   domain.FsEntryRepository
	vaultSvc VaultResolver
	client   string
	logger   *zap.Logger
}

// NewNoteHistoryServiceV3 创建 v3 门面版 NoteHistoryService（REST/插件历史查看用）
func NewNoteHistoryServiceV3(
	content ContentV3Service,
	histRepo domain.EntryHistoryRepository,
	fsRepo domain.FsEntryRepository,
	vaultSvc VaultResolver,
	logger *zap.Logger,
) NoteHistoryService {
	return &noteHistoryServiceV3{
		content: content, histRepo: histRepo, fsRepo: fsRepo,
		vaultSvc: vaultSvc, client: "server/history", logger: logger,
	}
}

// entryForPath 活跃或墓碑条目（回收站笔记也有历史可查）
func (s *noteHistoryServiceV3) entryForPath(ctx context.Context, uid int64, vault, path string) (*domain.FsEntry, error) {
	if e, err := s.content.GetEntryByPath(ctx, uid, vault, path); err == nil && e != nil {
		return e, nil
	}
	v, err := s.vaultSvc.GetOrCreate(ctx, uid, vault)
	if err != nil {
		return nil, err
	}
	tombs, err := s.fsRepo.ListDeleted(ctx, v.ID, uid)
	if err != nil {
		return nil, code.ErrorDBQuery.WithDetails(err.Error())
	}
	for _, t := range tombs {
		if t.Path == path {
			return t, nil
		}
	}
	return nil, code.ErrorNoteNotFound
}

// historyDTO 详情：正文 + 与相邻新版本的 diffs（新→旧方向，与旧版呈现一致）
func (s *noteHistoryServiceV3) historyDTO(uid int64, e *domain.FsEntry, items []domain.EntryHistoryItem, idx int) (*dto.NoteHistoryDTO, error) {
	h := &items[idx]
	var contentStr string
	if data, err := s.content.ReadEntryBlob(uid, h.BlobHash); err == nil {
		contentStr = string(data)
	}
	var diffs []diffmatchpatch.Diff
	dmp := diffmatchpatch.New()
	// 对比基准：更近版本（列表倒序，idx-1 更新）；最新版本对比当前条目内容
	var newer string
	if idx > 0 {
		if data, err := s.content.ReadEntryBlob(uid, items[idx-1].BlobHash); err == nil {
			newer = string(data)
		}
	} else {
		if data, err := s.content.ReadEntryBlob(uid, e.BlobHash); err == nil {
			newer = string(data)
		}
	}
	diffs = dmp.DiffMain(newer, contentStr, true)
	diffs = dmp.DiffCleanupSemantic(diffs)
	clientType, clientName, _ := splitClientTag(h.Client)
	return &dto.NoteHistoryDTO{
		ID:          h.ID,
		EntryID:     h.EntryID,
		VaultID:     h.VaultID,
		Path:        e.Path,
		Diffs:       diffs,
		Content:     contentStr,
		ContentHash: h.BlobHash,
		ClientType:  clientType,
		ClientName:  clientName,
		Version:     h.Version,
		CreatedAt:   timex.Time(h.CreatedAt),
	}, nil
}

// Get 按 ID 取历史详情（REST /api/note/history）
func (s *noteHistoryServiceV3) Get(ctx context.Context, uid int64, id int64) (*dto.NoteHistoryDTO, error) {
	h, err := s.histRepo.GetByID(ctx, id, uid)
	if err != nil {
		return nil, code.ErrorHistoryNotFound
	}
	e, err := s.fsRepo.GetByID(ctx, h.EntryID, uid)
	if err != nil {
		return nil, code.ErrorHistoryNotFound.WithDetails("entry gone")
	}
	items, err := s.histRepo.ListByEntry(ctx, h.EntryID, uid)
	if err != nil {
		return nil, code.ErrorDBQuery.WithDetails(err.Error())
	}
	for i := range items {
		if items[i].ID == h.ID {
			return s.historyDTO(uid, e, items, i)
		}
	}
	return nil, code.ErrorHistoryNotFound
}

// List 按路径列历史（REST /api/note/histories）
func (s *noteHistoryServiceV3) List(ctx context.Context, uid int64, params *dto.NoteHistoryListRequest, pager *app.Pager) ([]*dto.NoteHistoryNoContentDTO, int64, error) {
	e, err := s.entryForPath(ctx, uid, params.Vault, params.Path)
	if err != nil {
		return nil, 0, err
	}
	items, err := s.histRepo.ListByEntry(ctx, e.ID, uid)
	if err != nil {
		return nil, 0, code.ErrorDBQuery.WithDetails(err.Error())
	}
	total := int64(len(items))
	page, size := pager.Page, pager.PageSize
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	pager.TotalRows = int(total)
	start := (page - 1) * size
	if start >= len(items) {
		return []*dto.NoteHistoryNoContentDTO{}, total, nil
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	out := make([]*dto.NoteHistoryNoContentDTO, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, entryToHistoryNoContentDTO(e, &items[i]))
	}
	return out, total, nil
}

// RestoreFromHistory 恢复：对历史 hash 提交 modify（身份不变、历史续档）
func (s *noteHistoryServiceV3) RestoreFromHistory(ctx context.Context, uid int64, historyID int64) (*dto.NoteDTO, error) {
	h, err := s.histRepo.GetByID(ctx, historyID, uid)
	if err != nil {
		return nil, code.ErrorHistoryNotFound
	}
	e, err := s.fsRepo.GetByID(ctx, h.EntryID, uid)
	if err != nil {
		return nil, code.ErrorHistoryNotFound.WithDetails("entry gone")
	}
	vaultName, err := s.vaultName(ctx, uid, e.VaultID)
	if err != nil {
		return nil, err
	}
	if e.Deleted {
		// 回收站条目先复活再恢复内容
		if _, err := s.content.RestoreFromTombstone(ctx, uid, vaultName, e.Path, s.client); err != nil {
			return nil, err
		}
	}
	restored, err := s.content.RestoreFromHash(ctx, uid, vaultName, e.Path, h.BlobHash, s.client)
	if err != nil {
		if isCode(err, code.ErrorV3BlobNotFound) {
			return nil, code.ErrorHistoryRestoreFailed.WithDetails("history blob missing")
		}
		return nil, err
	}
	data, err := s.content.ReadEntryBlob(uid, h.BlobHash)
	if err != nil {
		data = nil
	}
	return entryToNoteDTO(restored, string(data)), nil
}

// vaultName 由 vaultID 反查名称（historyID 恢复链路没有 vault 入参）
func (s *noteHistoryServiceV3) vaultName(ctx context.Context, uid, vaultID int64) (string, error) {
	if vs, ok := s.vaultSvc.(VaultService); ok {
		v, err := vs.Get(ctx, uid, vaultID)
		if err != nil {
			return "", err
		}
		return v.Name, nil
	}
	return "", code.ErrorVaultNotFound.WithDetails("vault name unresolvable from id")
}

// ==================== 旧管线专用（v3 实例不可用） ====================

func (s *noteHistoryServiceV3) GetByNoteIDAndHash(ctx context.Context, uid int64, noteID int64, contentHash string) (*dto.NoteHistoryDTO, error) {
	return nil, errV3Unsupported
}
func (s *noteHistoryServiceV3) ProcessDelay(ctx context.Context, noteID int64, uid int64) error {
	return nil
}
func (s *noteHistoryServiceV3) Migrate(ctx context.Context, oldNoteID, newNoteID int64, uid int64) error {
	return errV3Unsupported
}
func (s *noteHistoryServiceV3) CleanupByTime(ctx context.Context, cutoffTime int64, keepVersions int) error {
	return nil // 保留策略由 v3 维护任务执行（旧实例仍服务旧表）
}
