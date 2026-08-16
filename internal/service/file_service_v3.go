// Package service: FileService 的 v3 实现（P5 功能回接）。
// 附件不再落 file 表 + 专用物理文件，而是内容寻址 blob；上传（REST multipart / MCP file_write）
// 经 ContentV3Service.Write 进入 v3 提交管线。GetContentInfo 返回 blob 物理路径供零拷贝下载。
package service

import (
	"context"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"go.uber.org/zap"
)

type fileServiceV3 struct {
	content  ContentV3Service
	fsRepo   domain.FsEntryRepository
	manifest domain.VaultManifestRepository
	blobs    domain.BlobStore
	vaultSvc VaultResolver
	client   string
	logger   *zap.Logger
}

// NewFileServiceV3 创建 v3 门面版 FileService（REST/MCP/静态下载用）
func NewFileServiceV3(
	content ContentV3Service,
	fsRepo domain.FsEntryRepository,
	manifest domain.VaultManifestRepository,
	blobs domain.BlobStore,
	vaultSvc VaultResolver,
	logger *zap.Logger,
) FileService {
	return &fileServiceV3{
		content: content, fsRepo: fsRepo, manifest: manifest,
		blobs: blobs, vaultSvc: vaultSvc, logger: logger,
	}
}

func (s *fileServiceV3) WithClient(clientType, name, version string) FileService {
	fs := *s
	fs.client = clientTag(clientType, name, version)
	return &fs
}

// ==================== 元数据 ====================

func (s *fileServiceV3) entryFor(ctx context.Context, uid int64, vault, path string, isRecycle bool) (*domain.FsEntry, error) {
	if !isRecycle {
		e, err := s.content.GetEntryByPath(ctx, uid, vault, path)
		if err != nil {
			if isCode(err, code.ErrorV3EntryNotFound) {
				return nil, code.ErrorFileNotFound
			}
			return nil, err
		}
		return e, nil
	}
	if e, err := s.content.GetEntryByPath(ctx, uid, vault, path); err == nil && e != nil {
		return nil, code.ErrorFileNotFound // 非回收态条目在回收视图按不存在处理
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
	return nil, code.ErrorFileNotFound
}

func (s *fileServiceV3) Get(ctx context.Context, uid int64, params *dto.FileGetRequest) (*dto.FileDTO, error) {
	if params.Path == "" {
		return nil, code.ErrorInvalidParams.WithDetails("path is required on the v3 surface")
	}
	e, err := s.entryFor(ctx, uid, params.Vault, params.Path, params.IsRecycle)
	if err != nil {
		return nil, err
	}
	return entryToFileDTO(e), nil
}

func (s *fileServiceV3) UpdateCheck(ctx context.Context, uid int64, params *dto.FileUpdateCheckRequest) (string, *dto.FileDTO, error) {
	e, err := s.content.GetEntryByPath(ctx, uid, params.Vault, params.Path)
	if err != nil {
		if isCode(err, code.ErrorV3EntryNotFound) {
			return "Create", nil, nil
		}
		return "", nil, err
	}
	fileDTO := entryToFileDTO(e)
	if e.BlobHash == params.ContentHash {
		return "", fileDTO, nil
	}
	return "UpdateContent", fileDTO, nil
}

// UploadCheck 与 UpdateCheck 同义（旧接口在上传链路里调用）
func (s *fileServiceV3) UploadCheck(ctx context.Context, uid int64, params *dto.FileUpdateCheckRequest) (string, *dto.FileDTO, error) {
	return s.UpdateCheck(ctx, uid, params)
}

// ==================== 写 ====================

func (s *fileServiceV3) UpdateOrCreate(ctx context.Context, uid int64, params *dto.FileUpdateRequest, mtimeCheck bool) (bool, *dto.FileDTO, error) {
	if params.Path == "" {
		return false, nil, code.ErrorInvalidParams.WithDetails("path is required")
	}
	data, err := s.readUploadPayload(uid, params)
	if err != nil {
		return false, nil, err
	}
	existing, _ := s.content.GetEntryByPath(ctx, uid, params.Vault, params.Path)
	created := existing == nil
	if !created && mtimeCheck && params.Mtime > 0 && params.Mtime < existing.Mtime {
		return false, nil, code.ErrorNoteConflict.WithDetails("server mtime is newer")
	}
	e, err := s.content.Write(ctx, uid, params.Vault, params.Path, data, false, s.client)
	if err != nil {
		return false, nil, err
	}
	return created, entryToFileDTO(e), nil
}

func (s *fileServiceV3) UploadComplete(ctx context.Context, uid int64, params *dto.FileUpdateRequest) (bool, *dto.FileDTO, error) {
	return s.UpdateOrCreate(ctx, uid, params, false)
}

// readUploadPayload 上传内容二选一：SavePath 临时文件（REST multipart / MCP file_write）或
// 校验哈希定位的既有 blob（重传去重场景不产生新内容，直接引用）。
func (s *fileServiceV3) readUploadPayload(uid int64, params *dto.FileUpdateRequest) ([]byte, error) {
	if params.SavePath != "" {
		data, err := os.ReadFile(params.SavePath)
		if err != nil {
			return nil, code.ErrorFileReadFailed.WithDetails(err.Error())
		}
		return data, nil
	}
	if params.ContentHash != "" && s.blobs.BlobExists(uid, params.ContentHash) {
		return s.blobs.BlobReadAll(uid, params.ContentHash)
	}
	return nil, code.ErrorInvalidParams.WithDetails("no upload payload: savePath or known contentHash required")
}

func (s *fileServiceV3) Delete(ctx context.Context, uid int64, params *dto.FileDeleteRequest) (*dto.FileDTO, error) {
	e, err := s.entryFor(ctx, uid, params.Vault, params.Path, false)
	if err != nil {
		return nil, err
	}
	if err := s.content.Delete(ctx, uid, params.Vault, params.Path, s.client); err != nil {
		if isCode(err, code.ErrorV3EntryNotFound) {
			return nil, code.ErrorFileNotFound
		}
		return nil, err
	}
	return entryToFileDTO(e), nil
}

func (s *fileServiceV3) Restore(ctx context.Context, uid int64, params *dto.FileRestoreRequest) (*dto.FileDTO, error) {
	e, err := s.content.RestoreFromTombstone(ctx, uid, params.Vault, params.Path, s.client)
	if err != nil {
		if isCode(err, code.ErrorV3EntryNotFound) {
			return nil, code.ErrorFileNotFound
		}
		return nil, err
	}
	return entryToFileDTO(e), nil
}

func (s *fileServiceV3) Rename(ctx context.Context, uid int64, params *dto.FileRenameRequest) (*dto.FileDTO, *dto.FileDTO, error) {
	old, err := s.entryFor(ctx, uid, params.Vault, params.OldPath, false)
	if err != nil {
		return nil, nil, err
	}
	if _, err := s.content.GetEntryByPath(ctx, uid, params.Vault, params.Path); err == nil {
		return nil, nil, code.ErrorFileExist.WithDetails("target path exists")
	}
	moved, err := s.content.Move(ctx, uid, params.Vault, params.OldPath, params.Path, s.client)
	if err != nil {
		return nil, nil, err
	}
	return entryToFileDTO(old), entryToFileDTO(moved), nil
}

func (s *fileServiceV3) RecycleClear(ctx context.Context, uid int64, params *dto.FileRecycleClearRequest) error {
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
		if t.IsNote {
			continue
		}
		if err := s.fsRepo.Purge(ctx, t.ID, uid); err != nil {
			return err
		}
	}
	return nil
}

// ==================== 下载 ====================

func (s *fileServiceV3) GetContent(ctx context.Context, uid int64, params *dto.FileGetRequest) (io.ReadCloser, string, int64, string, error) {
	e, err := s.entryFor(ctx, uid, params.Vault, params.Path, params.IsRecycle)
	if err != nil {
		return nil, "", 0, "", err
	}
	rc, err := s.blobs.BlobOpen(uid, e.BlobHash)
	if err != nil {
		return nil, "", 0, "", code.ErrorFileReadFailed.WithDetails(err.Error())
	}
	return rc, contentTypeOf(params.Path), e.Mtime, e.BlobHash, nil
}

// GetContentInfo 零拷贝下载元数据：blob 物理路径 + 展示名
func (s *fileServiceV3) GetContentInfo(ctx context.Context, uid int64, params *dto.FileGetRequest) (string, string, int64, string, string, error) {
	e, err := s.entryFor(ctx, uid, params.Vault, params.Path, params.IsRecycle)
	if err != nil {
		return "", "", 0, "", "", err
	}
	if !s.blobs.BlobExists(uid, e.BlobHash) {
		return "", "", 0, "", "", code.ErrorFileNotFound.WithDetails("blob missing")
	}
	pathLocator, ok := s.blobs.(interface{ BlobPath(int64, string) string })
	if !ok {
		return "", "", 0, "", "", code.ErrorFileReadFailed.WithDetails("blob store has no local path")
	}
	savePath := pathLocator.BlobPath(uid, e.BlobHash)
	return savePath, contentTypeOf(params.Path), e.Mtime, e.BlobHash, filepath.Base(params.Path), nil
}

// contentTypeOf 按扩展名推断 MIME（未知回退二进制流）
func contentTypeOf(path string) string {
	ct := mime.TypeByExtension(filepath.Ext(path))
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}

// ==================== 嵌入链接解析（![[...]] → 实际附件路径） ====================

func (s *fileServiceV3) ResolveEmbedLinks(ctx context.Context, uid int64, vaultName string, notePath string, content string) (map[string]string, error) {
	rawRefs := extractSharedNoteFileRefs(content)
	resultMap := make(map[string]string, len(rawRefs))
	if len(rawRefs) == 0 {
		return resultMap, nil
	}
	v, err := s.vaultSvc.GetOrCreate(ctx, uid, vaultName)
	if err != nil {
		return nil, err
	}
	cur, err := s.manifest.Current(ctx, v.ID, uid)
	if err != nil {
		return nil, code.ErrorDBQuery.WithDetails(err.Error())
	}
	liveFiles := map[string]bool{}
	if cur != nil {
		for i := range cur.Items {
			if !cur.Items[i].IsNote {
				liveFiles[cur.Items[i].Path] = true
			}
		}
	}
	for _, rawRef := range rawRefs {
		ref := strings.TrimSpace(rawRef)
		if !isLocalSharePath(ref) {
			continue
		}
		for _, candidate := range buildSharePathCandidates(notePath, ref) {
			if liveFiles[candidate] {
				resultMap[rawRef] = candidate
				break
			}
		}
		// 兜底：裸文件名在 vault 内唯一命中
		if _, ok := resultMap[rawRef]; !ok {
			normalizedRef := normalizeShareVaultPath(ref)
			if normalizedRef != "" && !strings.Contains(normalizedRef, "/") {
				var hit string
				for p := range liveFiles {
					if filepath.Base(p) == normalizedRef {
						if hit != "" {
							hit = "" // 多命中歧义：不解析
							break
						}
						hit = p
					}
				}
				if hit != "" {
					resultMap[rawRef] = hit
				}
			}
		}
	}
	return resultMap, nil
}

// ==================== 列举 ====================

func (s *fileServiceV3) List(ctx context.Context, uid int64, params *dto.FileListRequest, pager *app.Pager) ([]*dto.FileDTO, int, error) {
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
			if !t.IsNote {
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
				if !cur.Items[i].IsNote {
					entries = append(entries, itemToEntry(cur.VaultID, &cur.Items[i]))
				}
			}
		}
	}
	if params.Keyword != "" {
		kw := strings.ToLower(params.Keyword)
		filtered := entries[:0]
		for _, e := range entries {
			if strings.Contains(strings.ToLower(e.Path), kw) {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}
	sortEntries(entries, params.SortBy, params.SortOrder)

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
		return []*dto.FileDTO{}, total, nil
	}
	end := start + size
	if end > total {
		end = total
	}
	out := make([]*dto.FileDTO, 0, end-start)
	for _, e := range entries[start:end] {
		out = append(out, entryToFileDTO(e))
	}
	return out, total, nil
}

// ==================== 旧管线专用（v3 实例不可用） ====================

func (s *fileServiceV3) ListByLastTime(ctx context.Context, uid int64, params *dto.FileSyncRequest) ([]*dto.FileDTO, error) {
	return nil, errV3Unsupported
}
func (s *fileServiceV3) CountSizeSum(ctx context.Context, vaultID int64, uid int64) error { return nil }
func (s *fileServiceV3) Cleanup(ctx context.Context, uid int64) error                     { return nil }
func (s *fileServiceV3) CleanupByTime(ctx context.Context, cutoffTime int64) error        { return nil }
func (s *fileServiceV3) CleanDuplicateFiles(ctx context.Context, uid int64, vaultID int64) error {
	return nil
}
func (s *fileServiceV3) CleanDuplicateFilesAll(ctx context.Context) error { return nil }
