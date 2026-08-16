// Package service: NoteService 的 v3 实现（P5 功能回接）。
// REST / MCP 的笔记读写不再走旧 note 表，而是 fs_entry/manifest + blob；
// 写入经 ContentV3Service → SyncV3Service.Commit 管线（epoch 推进、广播、副作用），
// 因此 AI/面板的修改对 v3 客户端实时可见。
// 旧 v1/v2 WS 管线专用方法（Sync/ListByLastTime/Migrate…）在此实例上不可用——
// 那些调用方仍持有旧实现；这里返回 Unsupported 保底。
package service

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"

	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"go.uber.org/zap"
)

// errV3Unsupported 旧管线专用方法在 v3 实例上的占位错误（正常装配下不会被调用）
var errV3Unsupported = code.ErrorInvalidParams.WithDetails("not available on the v3 service surface")

type noteServiceV3 struct {
	content  ContentV3Service
	fsRepo   domain.FsEntryRepository
	manifest domain.VaultManifestRepository
	vaultSvc VaultResolver
	fts      *dao.BleveManager
	client   string
	logger   *zap.Logger
}

// NewNoteServiceV3 创建 v3 门面版 NoteService（REST/MCP 用）
func NewNoteServiceV3(
	content ContentV3Service,
	fsRepo domain.FsEntryRepository,
	manifest domain.VaultManifestRepository,
	vaultSvc VaultResolver,
	fts *dao.BleveManager,
	logger *zap.Logger,
) NoteService {
	return &noteServiceV3{
		content: content, fsRepo: fsRepo, manifest: manifest,
		vaultSvc: vaultSvc, fts: fts, logger: logger,
	}
}

func (s *noteServiceV3) WithClient(clientType, name, version string) NoteService {
	ns := *s
	ns.client = clientTag(clientType, name, version)
	return &ns
}

// clientTag 三元组拼回 v3 单串标识
func clientTag(clientType, name, version string) string {
	switch {
	case clientType != "" && name != "":
		return clientType + "/" + name
	case clientType != "":
		return clientType
	case name != "":
		return name
	}
	return ""
}

// ==================== 读 ====================

// entryWithContent 取条目并读正文（回收站条目也支持）
func (s *noteServiceV3) entryWithContent(ctx context.Context, uid int64, vault string, path string, isRecycle bool) (*domain.FsEntry, string, error) {
	if !isRecycle {
		e, data, err := s.content.ReadEntry(ctx, uid, vault, path)
		if err != nil {
			if isCode(err, code.ErrorV3EntryNotFound) {
				return nil, "", code.ErrorNoteNotFound
			}
			return nil, "", err
		}
		return e, string(data), nil
	}
	// 回收站视图：同路径已有活跃条目时按不存在处理（防止旧墓碑顶替新笔记，
	// 与 fileServiceV3.entryFor 一致；Restore 预检依赖 Action=delete 仅在真墓碑时出现）
	if e, err := s.content.GetEntryByPath(ctx, uid, vault, path); err == nil && e != nil {
		return nil, "", code.ErrorNoteNotFound
	}
	// 墓碑按路径定位，正文仍从 blob 读（内容寻址，删除不撤 blob）
	e, err := s.deletedByPath(ctx, uid, vault, path)
	if err != nil {
		return nil, "", err
	}
	data, err := s.content.ReadEntryBlob(uid, e.BlobHash)
	if err != nil {
		return e, "", nil // blob 缺失不阻断元数据返回
	}
	return e, string(data), nil
}

// deletedByPath 在墓碑集合中按路径取条目
func (s *noteServiceV3) deletedByPath(ctx context.Context, uid int64, vault, path string) (*domain.FsEntry, error) {
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

func (s *noteServiceV3) Get(ctx context.Context, uid int64, params *dto.NoteGetRequest) (*dto.NoteDTO, error) {
	if params.Path == "" {
		return nil, code.ErrorInvalidParams.WithDetails("path is required on the v3 surface")
	}
	e, content, err := s.entryWithContent(ctx, uid, params.Vault, params.Path, params.IsRecycle)
	if err != nil {
		return nil, err
	}
	return entryToNoteDTO(e, content), nil
}

func (s *noteServiceV3) GetByID(ctx context.Context, uid, id int64) (*dto.NoteDTO, error) {
	// v3 身份是 UUID 字符串，数值 ID 查询不适用；按不存在处理保持接口闭合
	return nil, code.ErrorNoteNotFound
}

// UpdateCheckWithNote 内容一致性与旧版同构：Create / "" / UpdateContent / UpdateMtime。
// 返回的 domain.Note 恒为 nil（v3 无此类型；调用方仅回传给 ModifyOrCreate，可安全忽略）。
func (s *noteServiceV3) UpdateCheckWithNote(ctx context.Context, uid int64, params *dto.NoteUpdateCheckRequest) (string, *domain.Note, *dto.NoteDTO, error) {
	mode, noteDTO, err := s.UpdateCheck(ctx, uid, params)
	return mode, nil, noteDTO, err
}

func (s *noteServiceV3) UpdateCheck(ctx context.Context, uid int64, params *dto.NoteUpdateCheckRequest) (string, *dto.NoteDTO, error) {
	e, err := s.content.GetEntryByPath(ctx, uid, params.Vault, params.Path)
	if err != nil {
		if isCode(err, code.ErrorV3EntryNotFound) {
			return "Create", nil, nil
		}
		return "", nil, err
	}
	noteDTO := entryToNoteDTO(e, "")
	if e.BlobHash == params.ContentHash {
		if params.Mtime < e.Mtime {
			return "UpdateMtime", noteDTO, nil
		}
		return "", noteDTO, nil
	}
	return "UpdateContent", noteDTO, nil
}

// ==================== 写 ====================

func (s *noteServiceV3) ModifyOrCreate(ctx context.Context, uid int64, params *dto.NoteModifyOrCreateRequest, mtimeCheck bool, existingNote ...*domain.Note) (bool, *dto.NoteDTO, error) {
	if params.Path == "" {
		return false, nil, code.ErrorInvalidParams.WithDetails("path is required")
	}
	existing, _ := s.content.GetEntryByPath(ctx, uid, params.Vault, params.Path)
	created := existing == nil
	if !created && mtimeCheck && params.Mtime > 0 && params.Mtime < existing.Mtime {
		return false, nil, code.ErrorNoteConflict.WithDetails("server mtime is newer")
	}
	e, err := s.content.Write(ctx, uid, params.Vault, params.Path, []byte(params.Content), true, s.client)
	if err != nil {
		return false, nil, err
	}
	return created, entryToNoteDTO(e, params.Content), nil
}

func (s *noteServiceV3) Delete(ctx context.Context, uid int64, params *dto.NoteDeleteRequest) (*dto.NoteDTO, error) {
	e, _, err := s.entryWithContent(ctx, uid, params.Vault, params.Path, false)
	if err != nil {
		return nil, err
	}
	if err := s.content.Delete(ctx, uid, params.Vault, params.Path, s.client); err != nil {
		if isCode(err, code.ErrorV3EntryNotFound) {
			return nil, code.ErrorNoteNotFound
		}
		return nil, err
	}
	out := entryToNoteDTO(e, "")
	return out, nil
}

func (s *noteServiceV3) Restore(ctx context.Context, uid int64, params *dto.NoteRestoreRequest) (*dto.NoteDTO, error) {
	e, err := s.content.RestoreFromTombstone(ctx, uid, params.Vault, params.Path, s.client)
	if err != nil {
		if isCode(err, code.ErrorV3EntryNotFound) {
			return nil, code.ErrorNoteNotFound
		}
		return nil, err
	}
	data, err := s.content.ReadEntryBlob(uid, e.BlobHash)
	if err != nil {
		data = nil
	}
	return entryToNoteDTO(e, string(data)), nil
}

func (s *noteServiceV3) Rename(ctx context.Context, uid int64, params *dto.NoteRenameRequest) (*dto.NoteDTO, *dto.NoteDTO, error) {
	old, _, err := s.entryWithContent(ctx, uid, params.Vault, params.OldPath, false)
	if err != nil {
		return nil, nil, err
	}
	// 目标已占用 → 438（与旧版语义一致）
	if _, err := s.content.GetEntryByPath(ctx, uid, params.Vault, params.Path); err == nil {
		return nil, nil, code.ErrorRenameNoteTargetExist
	}
	moved, err := s.content.Move(ctx, uid, params.Vault, params.OldPath, params.Path, s.client)
	if err != nil {
		return nil, nil, err
	}
	return entryToNoteDTO(old, ""), entryToNoteDTO(moved, ""), nil
}

func (s *noteServiceV3) RecycleClear(ctx context.Context, uid int64, params *dto.NoteRecycleClearRequest) error {
	v, err := s.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return err
	}
	tombs, err := s.fsRepo.ListDeleted(ctx, v.ID, uid)
	if err != nil {
		return code.ErrorDBQuery.WithDetails(err.Error())
	}
	for _, t := range tombs {
		if params.Path != "" && t.Path != params.Path {
			continue
		}
		if !t.IsNote {
			continue
		}
		if err := s.fsRepo.Purge(ctx, t.ID, uid); err != nil {
			return err
		}
	}
	return nil
}

// ==================== 内容编辑（读 → 变换 → 提交） ====================

func (s *noteServiceV3) PatchFrontmatter(ctx context.Context, uid int64, params *dto.NotePatchFrontmatterRequest) (*dto.NoteDTO, error) {
	e, content, err := s.entryWithContent(ctx, uid, params.Vault, params.Path, false)
	if err != nil {
		return nil, err
	}
	existingYaml, body, _ := util.ParseFrontmatter(content)
	if existingYaml == nil {
		existingYaml = map[string]interface{}{}
	}
	newYaml := util.MergeFrontmatter(existingYaml, params.Updates, params.Remove)
	newContent := util.ReconstructContent(newYaml, body)
	return s.writeBack(ctx, uid, params.Vault, params.Path, e, newContent)
}

func (s *noteServiceV3) AppendContent(ctx context.Context, uid int64, params *dto.NoteAppendRequest) (*dto.NoteDTO, error) {
	e, content, err := s.entryWithContent(ctx, uid, params.Vault, params.Path, false)
	if err != nil {
		return nil, err
	}
	return s.writeBack(ctx, uid, params.Vault, params.Path, e, content+params.Content)
}

func (s *noteServiceV3) PrependContent(ctx context.Context, uid int64, params *dto.NotePrependRequest) (*dto.NoteDTO, error) {
	e, content, err := s.entryWithContent(ctx, uid, params.Vault, params.Path, false)
	if err != nil {
		return nil, err
	}
	yamlData, body, hasFrontmatter := util.ParseFrontmatter(content)
	newBody := params.Content + body
	newContent := newBody
	if hasFrontmatter {
		newContent = util.ReconstructContent(yamlData, newBody)
	}
	return s.writeBack(ctx, uid, params.Vault, params.Path, e, newContent)
}

func (s *noteServiceV3) ReplaceContent(ctx context.Context, uid int64, params *dto.NoteReplaceRequest) (*dto.NoteReplaceResponse, error) {
	e, content, err := s.entryWithContent(ctx, uid, params.Vault, params.Path, false)
	if err != nil {
		return nil, err
	}

	var matchCount int
	var newContent string
	if params.Regex {
		re, err := regexp.Compile(params.Find)
		if err != nil {
			return nil, code.ErrorInvalidRegex.WithDetails(err.Error())
		}
		matches := re.FindAllStringIndex(content, -1)
		matchCount = len(matches)
		if params.All {
			newContent = re.ReplaceAllString(content, params.Replace)
		} else if matchCount > 0 {
			if loc := re.FindStringIndex(content); loc != nil {
				newContent = content[:loc[0]] + params.Replace + content[loc[1]:]
			}
		} else {
			newContent = content
		}
	} else {
		matchCount = strings.Count(content, params.Find)
		if params.All {
			newContent = strings.ReplaceAll(content, params.Find, params.Replace)
		} else if matchCount > 0 {
			newContent = strings.Replace(content, params.Find, params.Replace, 1)
		} else {
			newContent = content
		}
	}

	if matchCount == 0 && params.FailIfNoMatch {
		return nil, code.ErrorNoMatchFound
	}
	if newContent == content {
		return &dto.NoteReplaceResponse{MatchCount: matchCount, Note: entryToNoteDTO(e, content)}, nil
	}
	out, err := s.writeBack(ctx, uid, params.Vault, params.Path, e, newContent)
	if err != nil {
		return nil, err
	}
	return &dto.NoteReplaceResponse{MatchCount: matchCount, Note: out}, nil
}

// writeBack 编辑后的内容提交（modify；身份延续）
func (s *noteServiceV3) writeBack(ctx context.Context, uid int64, vault, path string, e *domain.FsEntry, newContent string) (*dto.NoteDTO, error) {
	written, err := s.content.Write(ctx, uid, vault, path, []byte(newContent), true, s.client)
	if err != nil {
		return nil, err
	}
	return entryToNoteDTO(written, newContent), nil
}

// ==================== 列举 ====================

func (s *noteServiceV3) List(ctx context.Context, uid int64, params *dto.NoteListRequest, pager *app.Pager) ([]*dto.NoteNoContentDTO, int, error) {
	v, err := s.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return nil, 0, err
	}

	var entries []*domain.FsEntry
	if params.IsRecycle {
		tombs, err := s.fsRepo.ListDeleted(ctx, v.ID, uid)
		if err != nil {
			return nil, 0, code.ErrorDBQuery.WithDetails(err.Error())
		}
		for _, t := range tombs {
			if t.IsNote {
				entries = append(entries, t)
			}
		}
	} else {
		cur, err := s.manifest.Current(ctx, v.ID, uid)
		if err != nil {
			return nil, 0, code.ErrorDBQuery.WithDetails(err.Error())
		}
		if cur != nil {
			for i := range cur.Items {
				if cur.Items[i].IsNote {
					entries = append(entries, itemToEntry(cur.VaultID, &cur.Items[i]))
				}
			}
		}
	}

	// 关键词过滤：content 模式走 Bleve（UUID 文档），path 模式子串匹配
	if params.Keyword != "" {
		contentMode := params.SearchContent || params.SearchMode == "content"
		if contentMode && s.fts != nil && s.fts.IsEnabled() {
			ids, err := s.fts.SearchEntryIDs(uid, v.ID, params.Keyword, params.IsRecycle, params.SortBy, params.SortOrder, 0)
			if err != nil {
				s.logger.Warn("v3 note list: fts search failed, fallback to path mode", zap.Error(err))
			} else if ids != nil {
				idSet := make(map[string]bool, len(ids))
				for _, id := range ids {
					idSet[id] = true
				}
				filtered := entries[:0]
				for _, e := range entries {
					if idSet[e.ID] {
						filtered = append(filtered, e)
					}
				}
				entries = filtered
				// FTS 已按相关性/排序规则有序，不再二次排序
				return s.paginateNotes(entries, params, pager)
			}
		}
		kw := strings.ToLower(params.Keyword)
		filtered := entries[:0]
		for _, e := range entries {
			if strings.Contains(strings.ToLower(e.Path), kw) {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}

	// 路径白名单（分享过滤用）
	if params.Paths != "" {
		set := map[string]bool{}
		for _, p := range strings.Split(params.Paths, ",") {
			if t := strings.TrimSpace(p); t != "" {
				set[t] = true
			}
		}
		filtered := entries[:0]
		for _, e := range entries {
			if set[e.Path] {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}

	sortEntries(entries, params.SortBy, params.SortOrder)
	return s.paginateNotes(entries, params, pager)
}

func (s *noteServiceV3) paginateNotes(entries []*domain.FsEntry, params *dto.NoteListRequest, pager *app.Pager) ([]*dto.NoteNoContentDTO, int, error) {
	total := len(entries)
	page, size := pager.Page, pager.PageSize
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	pager.TotalRows = total
	start := (page - 1) * size
	if start >= total {
		return []*dto.NoteNoContentDTO{}, total, nil
	}
	end := start + size
	if end > total {
		end = total
	}
	out := make([]*dto.NoteNoContentDTO, 0, end-start)
	for _, e := range entries[start:end] {
		out = append(out, entryToNoteNoContentDTO(e))
	}
	return out, total, nil
}

// itemToEntry 清单条目 → 轻量 FsEntry（列表视图：无 DB 时间戳，用 mtime 近似）
func itemToEntry(vaultID int64, it *domain.ManifestItem) *domain.FsEntry {
	return &domain.FsEntry{
		ID: it.ID, VaultID: vaultID, IsNote: it.IsNote, Path: it.Path,
		BlobHash: it.BlobHash, Size: it.Size, Ctime: it.Ctime, Mtime: it.Mtime,
	}
}

// sortEntries 列表排序：mtime(默认)/ctime/path/size × asc/desc
func sortEntries(entries []*domain.FsEntry, sortBy, sortOrder string) {
	if sortOrder == "" {
		sortOrder = "desc"
	}
	less := func(a, b *domain.FsEntry) bool { return a.Mtime < b.Mtime }
	switch sortBy {
	case "ctime":
		less = func(a, b *domain.FsEntry) bool { return a.Ctime < b.Ctime }
	case "path":
		less = func(a, b *domain.FsEntry) bool { return a.Path < b.Path }
	case "size":
		less = func(a, b *domain.FsEntry) bool { return a.Size < b.Size }
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if sortOrder == "asc" {
			return less(entries[i], entries[j])
		}
		return less(entries[j], entries[i])
	})
}

// isCode 判断错误是否为指定 code（content 门面错误均为 *code.Code）
func isCode(err error, target *code.Code) bool {
	if err == nil {
		return false
	}
	var c *code.Code
	if errors.As(err, &c) {
		return c.Code() == target.Code()
	}
	return false
}

// ==================== 旧管线专用（v3 实例不可用） ====================

func (s *noteServiceV3) ListByLastTime(ctx context.Context, uid int64, params *dto.NoteSyncRequest) ([]*dto.NoteDTO, error) {
	return nil, errV3Unsupported
}
func (s *noteServiceV3) Sync(ctx context.Context, uid int64, params *dto.NoteSyncRequest) ([]*dto.NoteDTO, error) {
	return nil, errV3Unsupported
}
func (s *noteServiceV3) ExistsBatch(ctx context.Context, uid int64, vault string, pathHashes []string) (map[string]bool, error) {
	return nil, errV3Unsupported
}
func (s *noteServiceV3) CountSizeSum(ctx context.Context, vaultID int64, uid int64) error { return nil }
func (s *noteServiceV3) Cleanup(ctx context.Context, uid int64) error                     { return nil }
func (s *noteServiceV3) CleanupByTime(ctx context.Context, cutoffTime int64) error        { return nil }
func (s *noteServiceV3) ListNeedSnapshot(ctx context.Context, uid int64) ([]*dto.NoteDTO, error) {
	// v3 历史快照在 ApplyChanges 提交时同步写入条目历史（EntryHistoryItem），
	// 不存在旧架构"ContentLastSnapshot 滞后、需后台补拍"的状态；
	// NoteHistoryTask.resumeTasks 的启动扫描在此返回空即可，不再报 errV3Unsupported。
	// Legacy snapshots lag behind ContentLastSnapshot and need a background catch-up scan;
	// v3 snapshots are written synchronously at commit time (EntryHistoryItem), so the
	// startup resume scan simply finds nothing.
	return nil, nil
}
func (s *noteServiceV3) Migrate(ctx context.Context, oldNoteID, newNoteID int64, uid int64) error {
	return errV3Unsupported
}
func (s *noteServiceV3) MigratePush(oldNoteID, newNoteID int64, uid int64) {}
func (s *noteServiceV3) UpdateNoteLinks(ctx context.Context, noteID int64, content string, vaultID, uid int64) {
}
func (s *noteServiceV3) CleanDuplicateNotes(ctx context.Context, uid int64, vaultID int64) error {
	return nil
}
func (s *noteServiceV3) CleanDuplicateNotesAll(ctx context.Context) error { return nil }
