// Package service: 各业务服务接口定义。
// P7R 起旧协议实现（含兼容层）已删除，仅保留接口与 v3 门面实现
// （note/file/folder/note_history/note_link 的 *_v3.go + setting_service_v3.go）。
// REST/MCP/任务调度通过这些接口访问服务。
package service

import (
	"context"
	"io"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	app "github.com/haierkeys/fast-note-sync-service/pkg/app"
)

type NoteService interface {
	// Get retrieves a single note
	// Get 获取单条笔记
	Get(ctx context.Context, uid int64, params *dto.NoteGetRequest) (*dto.NoteDTO, error)

	// UpdateCheck checks if note needs updating
	// UpdateCheck 检查笔记是否需要更新
	UpdateCheck(ctx context.Context, uid int64, params *dto.NoteUpdateCheckRequest) (string, *dto.NoteDTO, error)

	// UpdateCheckWithNote is like UpdateCheck but also returns the raw domain.Note fetched
	// during the check, letting callers that immediately call ModifyOrCreate reuse it and
	// avoid a duplicate lookup.
	// UpdateCheckWithNote 与 UpdateCheck 相同，但额外返回检查过程中查到的 domain.Note，
	// 便于紧接着调用 ModifyOrCreate 的调用方复用，避免重复查询。
	UpdateCheckWithNote(ctx context.Context, uid int64, params *dto.NoteUpdateCheckRequest) (string, *domain.Note, *dto.NoteDTO, error)

	// ModifyOrCreate creates or modifies a note. existingNote is optional: pass the note
	// already fetched via UpdateCheckWithNote (for the same pathHash) to skip the internal
	// lookup; omit it (or pass nil) to look it up as before.
	// ModifyOrCreate 创建或修改笔记。existingNote 为可选参数：传入已通过 UpdateCheckWithNote
	// 查到的 note（针对同一 pathHash）可跳过内部查询；不传（或传 nil）则按原逻辑查询。
	ModifyOrCreate(ctx context.Context, uid int64, params *dto.NoteModifyOrCreateRequest, mtimeCheck bool, existingNote ...*domain.Note) (bool, *dto.NoteDTO, error)

	// Delete deletes a note
	// Delete 删除笔记
	Delete(ctx context.Context, uid int64, params *dto.NoteDeleteRequest) (*dto.NoteDTO, error)

	// Restore restores a note (from recycle bin)
	// Restore 恢复笔记（从回收站恢复）
	Restore(ctx context.Context, uid int64, params *dto.NoteRestoreRequest) (*dto.NoteDTO, error)

	// Rename renames a note
	// Rename 重命名笔记
	Rename(ctx context.Context, uid int64, params *dto.NoteRenameRequest) (*dto.NoteDTO, *dto.NoteDTO, error)

	// List retrieves note list
	// List 获取笔记列表
	List(ctx context.Context, uid int64, params *dto.NoteListRequest, pager *app.Pager) ([]*dto.NoteNoContentDTO, int, error)

	// ListByLastTime retrieves notes updated after lastTime
	// ListByLastTime 获取在 lastTime 之后更新的笔记
	ListByLastTime(ctx context.Context, uid int64, params *dto.NoteSyncRequest) ([]*dto.NoteDTO, error)

	// GetByID retrieves a single note by ID, including full content
	// GetByID 根据 ID 获取单条笔记（含正文）
	GetByID(ctx context.Context, uid, id int64) (*dto.NoteDTO, error)

	// ExistsBatch checks, in a single batch query, whether each of the given pathHashes
	// currently exists and is not soft-deleted. Used to avoid per-item existence checks
	// (N+1) before batch-processing client-reported deletions.
	// ExistsBatch 单次批量查询一组 pathHash 是否存在且未被软删除。
	// 用于批量处理客户端上报删除前，避免逐条存在性检查造成的 N+1。
	ExistsBatch(ctx context.Context, uid int64, vault string, pathHashes []string) (map[string]bool, error)

	// Sync syncs notes (alias for ListByLastTime, used for WebSocket sync)
	// Sync 同步笔记（ListByLastTime 的别名，用于 WebSocket 同步）
	Sync(ctx context.Context, uid int64, params *dto.NoteSyncRequest) ([]*dto.NoteDTO, error)

	// CountSizeSum counts total number and size of notes in a vault
	// CountSizeSum 统计 vault 中笔记总数与总大小
	CountSizeSum(ctx context.Context, vaultID int64, uid int64) error

	// Cleanup cleans up expired soft-deleted notes
	// Cleanup 清理过期的软删除笔记
	Cleanup(ctx context.Context, uid int64) error

	// CleanupByTime cleans up expired soft-deleted notes for all users by cutoff time
	// CleanupByTime 按截止时间清理所有用户的过期软删除笔记
	CleanupByTime(ctx context.Context, cutoffTime int64) error

	// ListNeedSnapshot retrieves notes that need snapshot
	// ListNeedSnapshot 获取需要快照的笔记
	ListNeedSnapshot(ctx context.Context, uid int64) ([]*dto.NoteDTO, error)

	// Migrate migrates note history records
	// Migrate 迁移笔记历史记录
	Migrate(ctx context.Context, oldNoteID, newNoteID int64, uid int64) error

	// MigratePush submits note migration task
	// MigratePush 提交笔记迁移任务
	MigratePush(oldNoteID, newNoteID int64, uid int64)

	// WithClient sets client info
	// WithClient 设置客户端信息
	WithClient(clientType, name, version string) NoteService

	// PatchFrontmatter patches note frontmatter
	// PatchFrontmatter 修改笔记 Frontmatter
	PatchFrontmatter(ctx context.Context, uid int64, params *dto.NotePatchFrontmatterRequest) (*dto.NoteDTO, error)

	// AppendContent appends content to a note
	// AppendContent 在笔记末尾追加内容
	AppendContent(ctx context.Context, uid int64, params *dto.NoteAppendRequest) (*dto.NoteDTO, error)

	// PrependContent prepends content to a note
	// PrependContent 在笔记开头插入内容
	PrependContent(ctx context.Context, uid int64, params *dto.NotePrependRequest) (*dto.NoteDTO, error)

	// ReplaceContent performs find/replace in a note
	// ReplaceContent 在笔记中执行替换
	ReplaceContent(ctx context.Context, uid int64, params *dto.NoteReplaceRequest) (*dto.NoteReplaceResponse, error)

	// UpdateNoteLinks extracts wiki links from content and updates the link index
	// UpdateNoteLinks 从内容中提取 Wiki 链接并更新链接索引
	UpdateNoteLinks(ctx context.Context, noteID int64, content string, vaultID, uid int64)

	// RecycleClear cleans up the recycle bin
	// RecycleClear 清理回收站
	RecycleClear(ctx context.Context, uid int64, params *dto.NoteRecycleClearRequest) error

	// CleanDuplicateNotes cleans up duplicate note records
	// CleanDuplicateNotes 清理重复的笔记记录
	CleanDuplicateNotes(ctx context.Context, uid int64, vaultID int64) error

	// CleanDuplicateNotesAll cleans up duplicate note records for all users
	// CleanDuplicateNotesAll 清理所有用户的重复笔记记录
	CleanDuplicateNotesAll(ctx context.Context) error
}

type FileService interface {
	// Get retrieves a single file
	// Get 获取单条文件
	Get(ctx context.Context, uid int64, params *dto.FileGetRequest) (*dto.FileDTO, error)

	// UpdateCheck checks if file needs updating
	// UpdateCheck 检查文件是否需要更新
	UpdateCheck(ctx context.Context, uid int64, params *dto.FileUpdateCheckRequest) (string, *dto.FileDTO, error)

	// UploadCheck checks file upload (alias for UpdateCheck, used for WebSocket upload check)
	// UploadCheck 检查文件上传（UpdateCheck 的别名，用于 WebSocket 上传检查）
	UploadCheck(ctx context.Context, uid int64, params *dto.FileUpdateCheckRequest) (string, *dto.FileDTO, error)

	// UpdateOrCreate creates or modifies a file
	// UpdateOrCreate 创建或修改文件
	UpdateOrCreate(ctx context.Context, uid int64, params *dto.FileUpdateRequest, mtimeCheck bool) (bool, *dto.FileDTO, error)

	// UploadComplete completes file upload (alias for UpdateOrCreate, used for WebSocket upload completion)
	// UploadComplete 完成文件上传（UpdateOrCreate 的别名，用于 WebSocket 上传完成）
	UploadComplete(ctx context.Context, uid int64, params *dto.FileUpdateRequest) (bool, *dto.FileDTO, error)

	// Delete deletes a file
	// Delete 删除文件
	Delete(ctx context.Context, uid int64, params *dto.FileDeleteRequest) (*dto.FileDTO, error)

	// List retrieves file list
	// List 获取文件列表
	List(ctx context.Context, uid int64, params *dto.FileListRequest, pager *app.Pager) ([]*dto.FileDTO, int, error)

	// ListByLastTime retrieves files updated after lastTime
	// ListByLastTime 获取在 lastTime 之后更新的文件
	ListByLastTime(ctx context.Context, uid int64, params *dto.FileSyncRequest) ([]*dto.FileDTO, error)

	// CountSizeSum counts total number and total size of files in a vault
	// CountSizeSum 统计 vault 中文件总数与总大小
	CountSizeSum(ctx context.Context, vaultID int64, uid int64) error

	// Cleanup cleans up expired soft-deleted files
	// Cleanup 清理过期的软删除文件
	Cleanup(ctx context.Context, uid int64) error

	// CleanupByTime cleans up expired soft-deleted files for all users by cutoff time
	// CleanupByTime 按截止时间清理所有用户的过期软删除文件
	CleanupByTime(ctx context.Context, cutoffTime int64) error

	// ResolveEmbedLinks resolves local file links in note content
	// ResolveEmbedLinks 解析笔记内容中的本地文件链接
	ResolveEmbedLinks(ctx context.Context, uid int64, vaultName string, notePath string, content string) (map[string]string, error)

	// GetContent retrieves raw content of note or attachment file
	// GetContent 获取笔记或附件文件的原始内容
	GetContent(ctx context.Context, uid int64, params *dto.FileGetRequest) (io.ReadCloser, string, int64, string, error)

	// GetContentInfo retrieves file metadata and path for zero-copy download
	// GetContentInfo 获取文件的元数据和路径，用于零拷贝下载
	GetContentInfo(ctx context.Context, uid int64, params *dto.FileGetRequest) (savePath string, contentType string, mtime int64, etag string, fileName string, err error)

	// Restore restores a file (from recycle bin)
	// Restore 恢复文件（从回收站恢复）
	Restore(ctx context.Context, uid int64, params *dto.FileRestoreRequest) (*dto.FileDTO, error)
	// Rename renames a file
	// Rename 重命名文件
	Rename(ctx context.Context, uid int64, params *dto.FileRenameRequest) (*dto.FileDTO, *dto.FileDTO, error)
	// WithClient sets client info
	// WithClient 设置客户端信息
	WithClient(clientType, name, version string) FileService

	// RecycleClear cleans up the recycle bin
	// RecycleClear 清理回收站
	RecycleClear(ctx context.Context, uid int64, params *dto.FileRecycleClearRequest) error

	// CleanDuplicateFiles cleans up duplicate file records
	// CleanDuplicateFiles 清理重复的文件记录
	CleanDuplicateFiles(ctx context.Context, uid int64, vaultID int64) error

	// CleanDuplicateFilesAll cleans up duplicate file records for all users
	// CleanDuplicateFilesAll 清理所有用户的重复文件记录
	CleanDuplicateFilesAll(ctx context.Context) error
}

type FolderService interface {
	Get(ctx context.Context, uid int64, params *dto.FolderGetRequest) (*dto.FolderDTO, error)
	List(ctx context.Context, uid int64, params *dto.FolderListRequest) ([]*dto.FolderDTO, error)
	ListByUpdatedTimestamp(ctx context.Context, uid int64, vault string, lastTime int64) ([]*dto.FolderDTO, error)
	UpdateOrCreate(ctx context.Context, uid int64, params *dto.FolderCreateRequest) (*dto.FolderDTO, error)
	Delete(ctx context.Context, uid int64, params *dto.FolderDeleteRequest) (*dto.FolderDTO, error)
	DeleteTree(ctx context.Context, uid int64, params *dto.FolderDeleteRequest) (*dto.FolderDTO, error)
	Rename(ctx context.Context, uid int64, params *dto.FolderRenameRequest) (*dto.FolderDTO, *dto.FolderDTO, error)
	ListNotes(ctx context.Context, uid int64, params *dto.FolderContentRequest, pager *app.Pager) ([]*dto.NoteNoContentDTO, int, error)
	ListFiles(ctx context.Context, uid int64, params *dto.FolderContentRequest, pager *app.Pager) ([]*dto.FileDTO, int, error)
	EnsurePathFID(ctx context.Context, uid int64, vaultID int64, path string) (int64, error)
	CleanupEmptyAncestors(ctx context.Context, uid int64, vaultID int64, resourcePath string) error
	SyncResourceFID(ctx context.Context, uid int64, vaultID int64, noteIDs []int64, fileIDs []int64) error
	GetTree(ctx context.Context, uid int64, params *dto.FolderTreeRequest) (*dto.FolderTreeResponse, error)
	CleanDuplicateFolders(ctx context.Context, uid int64, vaultID int64) error
	WithClient(clientType, clientName, clientVersion string) FolderService
}

type SettingService interface {
	// UpdateCheck checks if configuration needs updating
	// UpdateCheck 检查配置是否需要更新
	UpdateCheck(ctx context.Context, uid int64, params *dto.SettingUpdateCheckRequest) (string, *dto.SettingDTO, error)

	// ModifyCheck checks configuration modification (alias for UpdateCheck)
	// ModifyCheck 检查配置修改（UpdateCheck 的别名）
	ModifyCheck(ctx context.Context, uid int64, params *dto.SettingUpdateCheckRequest) (string, *dto.SettingDTO, error)

	// ModifyOrCreate creates or modifies configuration
	// ModifyOrCreate 创建或修改配置
	ModifyOrCreate(ctx context.Context, uid int64, params *dto.SettingModifyOrCreateRequest, mtimeCheck bool) (bool, *dto.SettingDTO, error)

	// Modify modifies configuration (alias for ModifyOrCreate)
	// Modify 修改配置（ModifyOrCreate 的别名）
	Modify(ctx context.Context, uid int64, params *dto.SettingModifyOrCreateRequest) (bool, *dto.SettingDTO, error)

	// Delete deletes configuration
	// Delete 删除配置
	Delete(ctx context.Context, uid int64, params *dto.SettingDeleteRequest) (*dto.SettingDTO, error)

	// Get retrieves a single configuration
	// Get 获取单条配置
	Get(ctx context.Context, uid int64, params *dto.SettingGetRequest) (*dto.SettingDTO, error)

	// ListByLastTime retrieves configurations updated after lastTime
	// ListByLastTime 获取在 lastTime 之后更新的配置
	ListByLastTime(ctx context.Context, uid int64, params *dto.SettingSyncRequest) ([]*dto.SettingDTO, error)

	// CleanDuplicateSettings cleans up duplicate configuration records
	// CleanDuplicateSettings 清理重复的配置记录
	CleanDuplicateSettings(ctx context.Context, uid int64, vaultID int64) error

	// Sync synchronizes configuration (alias for ListByLastTime)
	// Sync 同步配置（ListByLastTime 的别名）
	Sync(ctx context.Context, uid int64, params *dto.SettingSyncRequest) ([]*dto.SettingDTO, error)

	// List retrieves configurations with pagination
	// List 分页获取配置列表
	List(ctx context.Context, uid int64, params *dto.SettingListRequest, pager *app.Pager) ([]*dto.SettingDTO, int64, error)

	// Rename renames a configuration
	// Rename 重命名配置
	Rename(ctx context.Context, uid int64, params *dto.SettingRenameRequest) (*dto.SettingDTO, error)

	// Cleanup cleans up expired soft-deleted configurations
	// Cleanup 清理过期的软删除配置
	Cleanup(ctx context.Context, uid int64) error

	// CleanupByTime cleans up expired soft-deleted configurations for all users by cutoff time
	// CleanupByTime 按截止时间清理所有用户的过期软删除配置
	CleanupByTime(ctx context.Context, cutoffTime int64) error

	// ClearByVault clears all settings for a specific vault of a user
	// ClearByVault 清除用户指定笔记本的所有配置
	ClearByVault(ctx context.Context, uid int64, vaultName string) error

	// WithClient sets client info
	// WithClient 设置客户端信息
	WithClient(clientType, name, version string) SettingService
}

type NoteHistoryService interface {
	// Get retrieves note history details for a specified ID
	// Get 获取指定 ID 的笔记历史详情
	Get(ctx context.Context, uid int64, id int64) (*dto.NoteHistoryDTO, error)

	// GetByNoteIDAndHash retrieves history record by note ID and content hash
	// GetByNoteIDAndHash 根据笔记 ID 和内容哈希获取历史记录
	GetByNoteIDAndHash(ctx context.Context, uid int64, noteID int64, contentHash string) (*dto.NoteHistoryDTO, error)

	// List retrieves history version list for a specified note
	// List 获取指定笔记的历史版本列表
	List(ctx context.Context, uid int64, params *dto.NoteHistoryListRequest, pager *app.Pager) ([]*dto.NoteHistoryNoContentDTO, int64, error)

	// RestoreFromHistory restores note content from a history version
	// RestoreFromHistory 从历史版本恢复笔记内容
	RestoreFromHistory(ctx context.Context, uid int64, historyID int64) (*dto.NoteDTO, error)

	// ProcessDelay processes note history with delay (calculates diff and saves patch version)
	// ProcessDelay 延时处理笔记历史（计算 diff 并保存补丁版本）
	ProcessDelay(ctx context.Context, noteID int64, uid int64) error

	// Migrate handles note history migration
	// Migrate 处理笔记历史迁移
	Migrate(ctx context.Context, oldNoteID, newNoteID int64, uid int64) error

	// CleanupByTime cleans up history records by cutoff time, keeping recent N versions per note
	// CleanupByTime 按截止时间清理历史记录，保留每个笔记最近 N 个版本
	CleanupByTime(ctx context.Context, cutoffTime int64, keepVersions int) error
}

type NoteLinkService interface {
	// GetBacklinks gets all notes that link to a target note
	// GetBacklinks 获取链接到目标笔记的所有笔记
	GetBacklinks(ctx context.Context, uid int64, params *dto.NoteLinkQueryRequest) ([]*dto.NoteLinkItem, error)

	// GetOutlinks gets all links from a source note
	// GetOutlinks 获取源笔记中的所有链接
	GetOutlinks(ctx context.Context, uid int64, params *dto.NoteLinkQueryRequest) ([]*dto.NoteLinkItem, error)
}
