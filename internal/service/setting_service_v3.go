// Package service: SettingService 的 v3 实现（REST 配置端点用）。
// setting 通道在 v3 数据层上 = 「.obsidian/ 与 _localStorage/ 前缀」的条目集合
// （P7R 起旧协议兼容层已删除，legacy_setting_paths 在册表随之退役：
//
//	旧协议 SettingModify 写过的非默认前缀路径不再属于 setting 通道，按普通文件同步）。
//	- ContentHash 按旧算法现算（_localStorage/ 文本、.obsidian/ 字节），保持 REST DTO 形状不变
//	- ModifyOrCreate 哈希相等不重写；仅 mtime 变更走 modify 保身份；全量写保留客户端 mtime/ctime
//	- Get 支持 PathHash 单参反查
package service

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"go.uber.org/zap"
)

// settingChannelPrefixes setting 通道默认前缀（v3 客户端按同口径划分）
var settingChannelPrefixes = []string{".obsidian/", "_localStorage/"}

// isSettingPath 判断路径是否属于 setting 通道
func isSettingPath(path string) bool {
	for _, p := range settingChannelPrefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

type settingServiceV3 struct {
	content  ContentV3Service
	fsRepo   domain.FsEntryRepository
	vaultSvc VaultResolver
	blobs    domain.BlobStore
	syncLog  SyncLogService

	clientType string
	clientName string
	clientVer  string
	logger     *zap.Logger
}

// NewSettingServiceV3 创建 REST 配置端点用的 SettingService v3 实例
func NewSettingServiceV3(
	content ContentV3Service,
	fsRepo domain.FsEntryRepository,
	vaultSvc VaultResolver,
	blobs domain.BlobStore,
	syncLog SyncLogService,
	logger *zap.Logger,
) SettingService {
	return &settingServiceV3{
		content:  content,
		fsRepo:   fsRepo,
		vaultSvc: vaultSvc,
		blobs:    blobs,
		syncLog:  syncLog,
		logger:   logger,
	}
}

func (c *settingServiceV3) WithClient(clientType, name, version string) SettingService {
	sc := *c
	sc.clientType = clientType
	sc.clientName = name
	sc.clientVer = version
	return &sc
}

// ==================== DTO 构造与条目定位 ====================

// toSettingDTO v3 条目 → SettingDTO。活跃行内联正文与 ContentHash（旧算法现算）；墓碑行 Action=delete。
func (c *settingServiceV3) toSettingDTO(ctx context.Context, uid int64, e *domain.FsEntry, withContent bool) *dto.SettingDTO {
	d := &dto.SettingDTO{
		ID:               0,
		Path:             e.Path,
		PathHash:         util.EncodeHash32(e.Path),
		Ctime:            e.Ctime,
		Mtime:            e.Mtime,
		UpdatedTimestamp: e.UpdatedAt.UnixMilli(),
		UpdatedAt:        timex.Time(e.UpdatedAt),
		CreatedAt:        timex.Time(e.CreatedAt),
	}
	if e.Deleted {
		d.Action = "delete"
		return d
	}
	d.Action = "modify"
	if !withContent {
		return d
	}
	if data, err := c.content.ReadEntryBlob(uid, e.BlobHash); err == nil {
		d.Content = string(data)
		d.ContentHash = settingLegacyHash(e.Path, data)
	}
	return d
}

// settingLegacyHash 通道内条目的旧算法哈希（_localStorage 文本、其余字节）
func settingLegacyHash(path string, data []byte) string {
	if strings.HasPrefix(path, "_localStorage/") {
		return util.EncodeHash32(string(data))
	}
	return util.EncodeHash32FileJS(data)
}

// findByPath 优先按路径取活跃行，缺失再看墓碑（Get 语义：软删行也返回）
func (c *settingServiceV3) findByPath(ctx context.Context, uid int64, vaultID int64, vaultName, path string) (*domain.FsEntry, error) {
	if e, err := c.fsRepo.GetLiveByPath(ctx, path, vaultID, uid); err == nil && e != nil {
		return e, nil
	} else if err != nil && !errors.Is(err, domain.ErrEntryNotFound) {
		return nil, err
	}
	tombs, err := c.fsRepo.ListDeleted(ctx, vaultID, uid)
	if err != nil {
		return nil, err
	}
	for _, t := range tombs {
		if t.Path == path {
			return t, nil
		}
	}
	return nil, code.ErrorSettingNotFound
}

// findByPathHash 仅凭 pathHash 反查。pathHash = EncodeHash32(path)；活/死同路径取 UpdatedAt 新者。
func (c *settingServiceV3) findByPathHash(ctx context.Context, uid int64, vaultID int64, pathHash string) (*domain.FsEntry, error) {
	if pathHash == "" {
		return nil, code.ErrorSettingNotFound
	}
	live, err := c.fsRepo.ListLive(ctx, vaultID, uid)
	if err != nil {
		return nil, err
	}
	var found *domain.FsEntry
	for _, e := range live {
		if util.EncodeHash32(e.Path) == pathHash {
			if found == nil || e.UpdatedAt.After(found.UpdatedAt) {
				found = e
			}
		}
	}
	if found != nil {
		return found, nil
	}
	tombs, err := c.fsRepo.ListDeleted(ctx, vaultID, uid)
	if err != nil {
		return nil, err
	}
	for _, t := range tombs {
		if util.EncodeHash32(t.Path) == pathHash {
			if found == nil || t.UpdatedAt.After(found.UpdatedAt) {
				found = t
			}
		}
	}
	if found != nil {
		return found, nil
	}
	return nil, code.ErrorSettingNotFound
}

// ==================== 检查 ====================

// UpdateCheck 旧 evalUpdateCheck 语义复刻。Create 模式也返回带 Path 的 DTO。
func (c *settingServiceV3) UpdateCheck(ctx context.Context, uid int64, params *dto.SettingUpdateCheckRequest) (string, *dto.SettingDTO, error) {
	v, err := c.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return "", nil, err
	}
	e, gerr := c.fsRepo.GetLiveByPath(ctx, params.Path, v.ID, uid)
	if gerr != nil && !errors.Is(gerr, domain.ErrEntryNotFound) {
		return "", nil, gerr
	}
	if e == nil {
		return "Create", &dto.SettingDTO{Path: params.Path, PathHash: params.PathHash}, nil
	}
	d := c.toSettingDTO(ctx, uid, e, true)
	if d.ContentHash == params.ContentHash {
		if params.Mtime < e.Mtime {
			return "UpdateMtime", d, nil
		}
		return "", d, nil
	}
	return "UpdateContent", d, nil
}

func (c *settingServiceV3) ModifyCheck(ctx context.Context, uid int64, params *dto.SettingUpdateCheckRequest) (string, *dto.SettingDTO, error) {
	return c.UpdateCheck(ctx, uid, params)
}

// ==================== 写 ====================

// ModifyOrCreate 哈希+mtime 全等不重写；哈希等仅 mtime 新 → modify 保身份；全量写保留客户端 mtime/ctime
func (c *settingServiceV3) ModifyOrCreate(ctx context.Context, uid int64, params *dto.SettingModifyOrCreateRequest, mtimeCheck bool) (bool, *dto.SettingDTO, error) {
	v, err := c.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return false, nil, err
	}
	existing, gerr := c.fsRepo.GetLiveByPath(ctx, params.Path, v.ID, uid)
	if gerr != nil && !errors.Is(gerr, domain.ErrEntryNotFound) {
		return false, nil, gerr
	}
	isNew := existing == nil

	if !isNew && mtimeCheck {
		d := c.toSettingDTO(ctx, uid, existing, true)
		if d.ContentHash == params.ContentHash {
			if existing.Mtime == params.Mtime {
				return isNew, d, nil
			}
			// 内容一致、客户端 mtime 较新：只改 mtime（blob 不动，身份不变）
			if existing.Mtime < params.Mtime {
				item := domain.ManifestItem{
					ID: existing.ID, Path: existing.Path, BlobHash: existing.BlobHash,
					IsNote: existing.IsNote, Size: existing.Size,
					Mtime: params.Mtime, Ctime: existing.Ctime,
				}
				if aerr := c.content.ApplyChanges(ctx, uid, params.Vault, []reconcile.Change{{Op: "modify", Item: item}}, c.clientTag()); aerr != nil {
					return false, nil, aerr
				}
				if c.syncLog != nil {
					c.syncLog.Log(uid, v.ID, domain.SyncLogTypeSetting, domain.SyncLogActionModify, "mtime", existing.Path, util.EncodeHash32(existing.Path), c.clientType, c.clientName, c.clientVer, existing.Size)
				}
				after, aerr := c.fsRepo.GetLiveByPath(ctx, params.Path, v.ID, uid)
				if aerr != nil {
					return false, nil, aerr
				}
				return isNew, c.toSettingDTO(ctx, uid, after, true), nil
			}
			// 客户端 mtime 较旧：不回写
			return isNew, d, nil
		}
	}

	data := []byte(params.Content)
	blobHash, serr := c.blobs.BlobStoreFromBytes(uid, data)
	if serr != nil {
		return false, nil, code.ErrorSettingModifyOrCreateFailed.WithDetails(serr.Error())
	}

	item := domain.ManifestItem{
		Path:     params.Path,
		BlobHash: blobHash,
		Size:     int64(len(data)),
		Mtime:    params.Mtime,
		Ctime:    params.Ctime,
	}
	op := "add"
	if !isNew {
		op = "modify"
		item.ID = existing.ID
		item.Ctime = existing.Ctime // 身份延续
		item.IsNote = existing.IsNote
	} else {
		item.IsNote = false // setting 通道新条目按文件落位
	}
	if aerr := c.content.ApplyChanges(ctx, uid, params.Vault, []reconcile.Change{{Op: op, Item: item}}, c.clientTag()); aerr != nil {
		return false, nil, aerr
	}
	if c.syncLog != nil {
		action := domain.SyncLogActionModify
		changed := "content,mtime"
		if isNew {
			action = domain.SyncLogActionCreate
			changed = ""
		}
		c.syncLog.Log(uid, v.ID, domain.SyncLogTypeSetting, action, changed, params.Path, util.EncodeHash32(params.Path), c.clientType, c.clientName, c.clientVer, int64(len(data)))
	}

	after, aerr := c.fsRepo.GetLiveByPath(ctx, params.Path, v.ID, uid)
	if aerr != nil {
		return false, nil, aerr
	}
	return isNew, c.toSettingDTO(ctx, uid, after, true), nil
}

func (c *settingServiceV3) Modify(ctx context.Context, uid int64, params *dto.SettingModifyOrCreateRequest) (bool, *dto.SettingDTO, error) {
	return c.ModifyOrCreate(ctx, uid, params, true)
}

// Delete 软删除（置 Action=delete、清内容）
func (c *settingServiceV3) Delete(ctx context.Context, uid int64, params *dto.SettingDeleteRequest) (*dto.SettingDTO, error) {
	v, err := c.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}
	e, ferr := c.findByPath(ctx, uid, v.ID, params.Vault, params.Path)
	if ferr != nil {
		// 仅有 pathHash 时反查
		if params.PathHash != "" {
			e, ferr = c.findByPathHash(ctx, uid, v.ID, params.PathHash)
		}
		if ferr != nil {
			return nil, code.ErrorSettingNotFound
		}
	}
	if e.Deleted {
		return c.toSettingDTO(ctx, uid, e, false), nil // 已删幂等
	}
	before := c.toSettingDTO(ctx, uid, e, false)
	if derr := c.content.Delete(ctx, uid, params.Vault, e.Path, c.clientTag()); derr != nil {
		return nil, derr
	}
	if c.syncLog != nil {
		c.syncLog.Log(uid, v.ID, domain.SyncLogTypeSetting, domain.SyncLogActionSoftDelete, "", e.Path, util.EncodeHash32(e.Path), c.clientType, c.clientName, c.clientVer, 0)
	}
	// 墓碑行的 UpdatedAt 即 ack 的 LastTime 基准
	if t, terr := c.findByPathHash(ctx, uid, v.ID, before.PathHash); terr == nil && t.Deleted {
		return c.toSettingDTO(ctx, uid, t, false), nil
	}
	before.Action = "delete"
	before.UpdatedTimestamp = timex.Now().UnixMilli()
	return before, nil
}

// Rename 路径迁移：v3 同行 move（身份与历史不断链）
func (c *settingServiceV3) Rename(ctx context.Context, uid int64, params *dto.SettingRenameRequest) (*dto.SettingDTO, error) {
	v, err := c.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}
	old, ferr := c.findByPath(ctx, uid, v.ID, params.Vault, params.OldPath)
	if ferr != nil {
		return nil, code.ErrorSettingNotFound
	}
	if target, terr := c.fsRepo.GetLiveByPath(ctx, params.NewPath, v.ID, uid); terr == nil && target != nil {
		return nil, code.ErrorSettingExist
	} else if terr != nil && !errors.Is(terr, domain.ErrEntryNotFound) {
		return nil, terr
	}
	if _, merr := c.content.Move(ctx, uid, params.Vault, params.OldPath, params.NewPath, c.clientTag()); merr != nil {
		return nil, merr
	}
	if c.syncLog != nil {
		c.syncLog.Log(uid, v.ID, domain.SyncLogTypeSetting, domain.SyncLogActionRename, "path", params.NewPath, util.EncodeHash32(params.NewPath), c.clientType, c.clientName, c.clientVer, old.Size)
	}
	moved, gerr := c.fsRepo.GetLiveByPath(ctx, params.NewPath, v.ID, uid)
	if gerr != nil {
		return nil, gerr
	}
	return c.toSettingDTO(ctx, uid, moved, true), nil
}

// ==================== 读 / 同步列表 ====================

// Get 支持 Path 优先、PathHash 兜底
func (c *settingServiceV3) Get(ctx context.Context, uid int64, params *dto.SettingGetRequest) (*dto.SettingDTO, error) {
	v, err := c.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}
	var e *domain.FsEntry
	if params.Path != "" {
		e, err = c.findByPath(ctx, uid, v.ID, params.Vault, params.Path)
	} else {
		e, err = c.findByPathHash(ctx, uid, v.ID, params.PathHash)
	}
	if err != nil {
		return nil, code.ErrorSettingNotFound
	}
	return c.toSettingDTO(ctx, uid, e, true), nil
}

// ListByLastTime setting 通道（前缀）条目，活跃+墓碑，UpdatedTimestamp 倒序按 pathHash 去重；活跃行内联正文与哈希
func (c *settingServiceV3) ListByLastTime(ctx context.Context, uid int64, params *dto.SettingSyncRequest) ([]*dto.SettingDTO, error) {
	v, err := c.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}
	live, err := c.fsRepo.ListLive(ctx, v.ID, uid)
	if err != nil {
		return nil, err
	}
	tombs, err := c.fsRepo.ListDeleted(ctx, v.ID, uid)
	if err != nil {
		return nil, err
	}

	rows := make([]*domain.FsEntry, 0, len(live)+len(tombs))
	for _, e := range live {
		if isSettingPath(e.Path) && (params.LastTime == 0 || e.UpdatedAt.UnixMilli() > params.LastTime) {
			rows = append(rows, e)
		}
	}
	for _, e := range tombs {
		if isSettingPath(e.Path) && (params.LastTime == 0 || e.UpdatedAt.UnixMilli() > params.LastTime) {
			rows = append(rows, e)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].UpdatedAt.After(rows[j].UpdatedAt) })

	results := make([]*dto.SettingDTO, 0, len(rows))
	seen := make(map[string]bool, len(rows))
	for _, e := range rows {
		ph := util.EncodeHash32(e.Path)
		if seen[ph] {
			continue
		}
		seen[ph] = true
		results = append(results, c.toSettingDTO(ctx, uid, e, true))
	}
	return results, nil
}

func (c *settingServiceV3) Sync(ctx context.Context, uid int64, params *dto.SettingSyncRequest) ([]*dto.SettingDTO, error) {
	return c.ListByLastTime(ctx, uid, params)
}

// List REST 分页：setting 通道活跃条目，路径关键字过滤，字典序 + offset 页
func (c *settingServiceV3) List(ctx context.Context, uid int64, params *dto.SettingListRequest, pager *pkgapp.Pager) ([]*dto.SettingDTO, int64, error) {
	v, err := c.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return nil, 0, err
	}
	live, err := c.fsRepo.ListLive(ctx, v.ID, uid)
	if err != nil {
		return nil, 0, err
	}
	filtered := make([]*domain.FsEntry, 0, len(live))
	for _, e := range live {
		if !isSettingPath(e.Path) {
			continue
		}
		if params.Keyword != "" && !strings.Contains(e.Path, params.Keyword) {
			continue
		}
		filtered = append(filtered, e)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Path < filtered[j].Path })

	total := int64(len(filtered))
	pager.TotalRows = int(total)
	page, size := pager.Page, pager.PageSize
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	start := (page - 1) * size
	if start >= len(filtered) {
		return []*dto.SettingDTO{}, total, nil
	}
	end := start + size
	if end > len(filtered) {
		end = len(filtered)
	}
	out := make([]*dto.SettingDTO, 0, end-start)
	for _, e := range filtered[start:end] {
		out = append(out, c.toSettingDTO(ctx, uid, e, true))
	}
	return out, total, nil
}

// ==================== 清理（v3 由 GC 负责，这里全部无操作） ====================

func (c *settingServiceV3) Cleanup(ctx context.Context, uid int64) error { return nil }

func (c *settingServiceV3) CleanupByTime(ctx context.Context, cutoffTime int64) error { return nil }

// CleanDuplicateSettings v3 路径唯一约束天然去重
func (c *settingServiceV3) CleanDuplicateSettings(ctx context.Context, uid int64, vaultID int64) error {
	return nil
}

// ClearByVault 清空 setting 通道条目（软删）
func (c *settingServiceV3) ClearByVault(ctx context.Context, uid int64, vaultName string) error {
	v, err := c.vaultSvc.GetOrCreate(ctx, uid, vaultName)
	if err != nil {
		return err
	}
	live, err := c.fsRepo.ListLive(ctx, v.ID, uid)
	if err != nil {
		return err
	}
	changes := make([]reconcile.Change, 0)
	for _, e := range live {
		if !isSettingPath(e.Path) {
			continue
		}
		changes = append(changes, reconcile.Change{Op: "delete", Item: e.ToManifestItem()})
	}
	if len(changes) > 0 {
		if aerr := c.content.ApplyChanges(ctx, uid, vaultName, changes, c.clientTag()); aerr != nil {
			return aerr
		}
	}
	return nil
}

// clientTag 日志用客户端标签
func (c *settingServiceV3) clientTag() string {
	return clientTag(c.clientType, c.clientName, c.clientVer)
}

var _ SettingService = (*settingServiceV3)(nil)
