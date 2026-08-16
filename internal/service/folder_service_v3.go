// Package service: FolderService 的 v3 实现（P5 功能回接）。
// v3 里目录不是实体——是清单中的路径前缀。这里的目录操作全部编译为条目级变更：
// 建目录=无操作（文件落位即成）；目录删除=子树墓碑；目录重命名=子树 move（身份全部保留）。
package service

import (
	"context"
	"path"
	"sort"
	"strings"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	"github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"go.uber.org/zap"
)

type folderServiceV3 struct {
	content  ContentV3Service
	manifest domain.VaultManifestRepository
	vaultSvc VaultResolver
	client   string
	logger   *zap.Logger
}

// NewFolderServiceV3 创建 v3 门面版 FolderService（REST/MCP 用）
func NewFolderServiceV3(
	content ContentV3Service,
	manifest domain.VaultManifestRepository,
	vaultSvc VaultResolver,
	logger *zap.Logger,
) FolderService {
	return &folderServiceV3{content: content, manifest: manifest, vaultSvc: vaultSvc, logger: logger}
}

func (s *folderServiceV3) WithClient(clientType, clientName, clientVersion string) FolderService {
	fs := *s
	fs.client = clientTag(clientType, clientName, clientVersion)
	return &fs
}

// ==================== 视图 ====================

// dirEntries 取清单（或回收站）全部条目
func (s *folderServiceV3) dirEntries(ctx context.Context, uid int64, vault string) ([]*domain.FsEntry, int64, error) {
	v, err := s.vaultSvc.GetOrCreate(ctx, uid, vault)
	if err != nil {
		return nil, 0, err
	}
	cur, err := s.manifest.Current(ctx, v.ID, uid)
	if err != nil {
		return nil, 0, code.ErrorDBQuery.WithDetails(err.Error())
	}
	if cur == nil {
		return nil, v.ID, nil
	}
	out := make([]*domain.FsEntry, 0, len(cur.Items))
	for i := range cur.Items {
		out = append(out, itemToEntry(cur.VaultID, &cur.Items[i]))
	}
	return out, v.ID, nil
}

// folderAggregate 聚合目录元数据：ctime=子项最早，mtime=子项最新
type folderAggregate struct {
	exists bool
	ctime  int64
	mtime  int64
}

func (s *folderServiceV3) aggregateUnder(entries []*domain.FsEntry, folder string) folderAggregate {
	agg := folderAggregate{}
	prefix := ""
	if folder != "" {
		prefix = folder + "/"
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Path, prefix) {
			continue
		}
		agg.exists = true
		if agg.ctime == 0 || e.Ctime < agg.ctime {
			agg.ctime = e.Ctime
		}
		if e.Mtime > agg.mtime {
			agg.mtime = e.Mtime
		}
	}
	return agg
}

func folderToDTO(folder string, agg folderAggregate) *dto.FolderDTO {
	return &dto.FolderDTO{
		Path:             folder,
		PathHash:         util.EncodeHash32(folder),
		Ctime:            agg.ctime,
		Mtime:            agg.mtime,
		UpdatedTimestamp: agg.mtime,
		UpdatedAt:        timex.Time(timex.Now()),
		CreatedAt:        timex.Time(timex.Now()),
	}
}

func (s *folderServiceV3) Get(ctx context.Context, uid int64, params *dto.FolderGetRequest) (*dto.FolderDTO, error) {
	entries, _, err := s.dirEntries(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}
	agg := s.aggregateUnder(entries, params.Path)
	if !agg.exists {
		return nil, code.ErrorFolderNotFound
	}
	return folderToDTO(params.Path, agg), nil
}

func (s *folderServiceV3) List(ctx context.Context, uid int64, params *dto.FolderListRequest) ([]*dto.FolderDTO, error) {
	entries, _, err := s.dirEntries(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}
	dirs := collectDirs(entries, params.Path)
	out := make([]*dto.FolderDTO, 0, len(dirs))
	for _, d := range dirs {
		out = append(out, folderToDTO(d, s.aggregateUnder(entries, d)))
	}
	return out, nil
}

// collectDirs 汇总出现的目录路径（可选限定 base 之下），字典序
func collectDirs(entries []*domain.FsEntry, base string) []string {
	prefix := ""
	if base != "" {
		prefix = base + "/"
	}
	set := map[string]bool{}
	for _, e := range entries {
		if !strings.HasPrefix(e.Path, prefix) {
			continue
		}
		dir := path.Dir(e.Path)
		for dir != "." && dir != "/" {
			if !strings.HasPrefix(dir, prefix) && prefix != "" {
				break
			}
			set[dir] = true
			dir = path.Dir(dir)
		}
	}
	out := make([]string, 0, len(set))
	for d := range set {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// directChildren 直属于该目录的条目（不再含更深层级）
func directChildren(entries []*domain.FsEntry, folder string) []*domain.FsEntry {
	prefix := ""
	if folder != "" {
		prefix = folder + "/"
	}
	out := make([]*domain.FsEntry, 0)
	for _, e := range entries {
		if !strings.HasPrefix(e.Path, prefix) {
			continue
		}
		rest := e.Path[len(prefix):]
		if rest == "" || strings.Contains(rest, "/") {
			continue
		}
		out = append(out, e)
	}
	return out
}

func (s *folderServiceV3) ListNotes(ctx context.Context, uid int64, params *dto.FolderContentRequest, pager *app.Pager) ([]*dto.NoteNoContentDTO, int, error) {
	entries, _, err := s.dirEntries(ctx, uid, params.Vault)
	if err != nil {
		return nil, 0, err
	}
	children := directChildren(entries, params.Path)
	notes := make([]*domain.FsEntry, 0, len(children))
	for _, e := range children {
		if e.IsNote {
			notes = append(notes, e)
		}
	}
	sortEntries(notes, params.SortBy, params.SortOrder)

	total := len(notes)
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
	for _, e := range notes[start:end] {
		out = append(out, entryToNoteNoContentDTO(e))
	}
	return out, total, nil
}

func (s *folderServiceV3) ListFiles(ctx context.Context, uid int64, params *dto.FolderContentRequest, pager *app.Pager) ([]*dto.FileDTO, int, error) {
	entries, _, err := s.dirEntries(ctx, uid, params.Vault)
	if err != nil {
		return nil, 0, err
	}
	children := directChildren(entries, params.Path)
	files := make([]*domain.FsEntry, 0, len(children))
	for _, e := range children {
		if !e.IsNote {
			files = append(files, e)
		}
	}
	sortEntries(files, params.SortBy, params.SortOrder)

	total := len(files)
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
	for _, e := range files[start:end] {
		out = append(out, entryToFileDTO(e))
	}
	return out, total, nil
}

// GetTree 从清单推导目录树；NoteCount/FileCount 为直属子项数
func (s *folderServiceV3) GetTree(ctx context.Context, uid int64, params *dto.FolderTreeRequest) (*dto.FolderTreeResponse, error) {
	entries, _, err := s.dirEntries(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}
	byPath := map[string]*dto.FolderTreeNode{}
	var ensure func(dir string) *dto.FolderTreeNode
	ensure = func(dir string) *dto.FolderTreeNode {
		if dir == "" {
			return nil // 根不占节点
		}
		if n, ok := byPath[dir]; ok {
			return n
		}
		n := &dto.FolderTreeNode{Path: dir, Name: path.Base(dir)}
		byPath[dir] = n
		parent := path.Dir(dir)
		if parent != "." && parent != "/" {
			if p := ensure(parent); p != nil {
				p.Children = append(p.Children, n)
			}
		}
		return n
	}

	resp := &dto.FolderTreeResponse{Folders: []*dto.FolderTreeNode{}}
	for _, e := range entries {
		dir := path.Dir(e.Path)
		if dir == "." || dir == "/" {
			if e.IsNote {
				resp.RootNoteCount++
			} else {
				resp.RootFileCount++
			}
			continue
		}
		node := ensure(dir)
		if node == nil {
			continue
		}
		if e.IsNote {
			node.NoteCount++
		} else {
			node.FileCount++
		}
	}
	// 收集顶层节点（无父的）并排序
	for _, n := range byPath {
		parent := path.Dir(n.Path)
		if parent == "." || parent == "/" {
			resp.Folders = append(resp.Folders, n)
		}
	}
	sort.Slice(resp.Folders, func(i, j int) bool { return resp.Folders[i].Path < resp.Folders[j].Path })
	for _, n := range byPath {
		sort.Slice(n.Children, func(i, j int) bool { return n.Children[i].Path < n.Children[j].Path })
	}
	// depth>0 时剪枝
	if params.Depth > 0 {
		var prune func(nodes []*dto.FolderTreeNode, depth int) []*dto.FolderTreeNode
		prune = func(nodes []*dto.FolderTreeNode, depth int) []*dto.FolderTreeNode {
			if depth <= 0 {
				return nil
			}
			out := make([]*dto.FolderTreeNode, 0, len(nodes))
			for _, n := range nodes {
				n.Children = prune(n.Children, depth-1)
				out = append(out, n)
			}
			return out
		}
		resp.Folders = prune(resp.Folders, params.Depth)
	}
	return resp, nil
}

// ==================== 写（目录操作编译为条目变更） ====================

// UpdateOrCreate 建目录：v3 无目录实体，文件落位即成；幂等成功并返回聚合视图
func (s *folderServiceV3) UpdateOrCreate(ctx context.Context, uid int64, params *dto.FolderCreateRequest) (*dto.FolderDTO, error) {
	entries, _, err := s.dirEntries(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}
	agg := s.aggregateUnder(entries, params.Path)
	return folderToDTO(params.Path, agg), nil
}

// subtreeChanges 组装子树变更：move（前缀替换）或 delete（墓碑）
func subtreeChanges(entries []*domain.FsEntry, folder string, moveTarget string) []reconcile.Change {
	prefix := ""
	if folder != "" {
		prefix = folder + "/"
	}
	var changes []reconcile.Change
	for _, e := range entries {
		if !strings.HasPrefix(e.Path, prefix) {
			continue
		}
		if moveTarget != "" {
			changes = append(changes, reconcile.Change{
				Op:      "move",
				OldPath: e.Path,
				Item: domain.ManifestItem{
					ID: e.ID, Path: moveTarget + "/" + e.Path[len(prefix):],
					BlobHash: e.BlobHash, IsNote: e.IsNote, Size: e.Size,
					Mtime: e.Mtime, Ctime: e.Ctime,
				},
			})
		} else {
			changes = append(changes, reconcile.Change{
				Op: "delete",
				Item: domain.ManifestItem{
					ID: e.ID, Path: e.Path, BlobHash: e.BlobHash, IsNote: e.IsNote,
					Size: e.Size, Mtime: e.Mtime, Ctime: e.Ctime,
				},
			})
		}
	}
	return changes
}

func (s *folderServiceV3) deleteTree(ctx context.Context, uid int64, params *dto.FolderDeleteRequest) (*dto.FolderDTO, error) {
	entries, _, err := s.dirEntries(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}
	agg := s.aggregateUnder(entries, params.Path)
	if !agg.exists {
		return nil, code.ErrorFolderNotFound
	}
	changes := subtreeChanges(entries, params.Path, "")
	if err := s.content.ApplyChanges(ctx, uid, params.Vault, changes, s.client); err != nil {
		return nil, err
	}
	return folderToDTO(params.Path, agg), nil
}

func (s *folderServiceV3) Delete(ctx context.Context, uid int64, params *dto.FolderDeleteRequest) (*dto.FolderDTO, error) {
	return s.deleteTree(ctx, uid, params)
}

func (s *folderServiceV3) DeleteTree(ctx context.Context, uid int64, params *dto.FolderDeleteRequest) (*dto.FolderDTO, error) {
	return s.deleteTree(ctx, uid, params)
}

func (s *folderServiceV3) Rename(ctx context.Context, uid int64, params *dto.FolderRenameRequest) (*dto.FolderDTO, *dto.FolderDTO, error) {
	entries, _, err := s.dirEntries(ctx, uid, params.Vault)
	if err != nil {
		return nil, nil, err
	}
	oldAgg := s.aggregateUnder(entries, params.OldPath)
	if !oldAgg.exists {
		return nil, nil, code.ErrorFolderNotFound
	}
	changes := subtreeChanges(entries, params.OldPath, params.Path)
	if err := s.content.ApplyChanges(ctx, uid, params.Vault, changes, s.client); err != nil {
		return nil, nil, err
	}
	return folderToDTO(params.OldPath, oldAgg), folderToDTO(params.Path, oldAgg), nil
}

// ==================== 旧管线专用（v3 实例不可用） ====================

func (s *folderServiceV3) ListByUpdatedTimestamp(ctx context.Context, uid int64, vault string, lastTime int64) ([]*dto.FolderDTO, error) {
	return nil, errV3Unsupported
}
func (s *folderServiceV3) EnsurePathFID(ctx context.Context, uid int64, vaultID int64, path string) (int64, error) {
	return 0, nil // v3 无 FID 概念
}
func (s *folderServiceV3) CleanupEmptyAncestors(ctx context.Context, uid int64, vaultID int64, resourcePath string) error {
	return nil
}
func (s *folderServiceV3) SyncResourceFID(ctx context.Context, uid int64, vaultID int64, noteIDs []int64, fileIDs []int64) error {
	return nil
}
func (s *folderServiceV3) CleanDuplicateFolders(ctx context.Context, uid int64, vaultID int64) error {
	return nil
}
