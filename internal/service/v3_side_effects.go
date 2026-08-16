// Package service: v3 提交副作用监听 —— 把旧层写路径的副作用回接到 v3 提交管线。
// 旧层里 NoteService/FileService 每次写都会：记同步日志、刷新 FTS 索引、通知备份/Git
// 触发、撤销已删资源的分享。v3 时代所有写入（客户端提交与服务器侧门面写入）都汇入
// SyncV3Service.Commit，副作用在此统一挂载。规则见 git-sync-redesign.md §8。
package service

import (
	"strings"

	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"go.uber.org/zap"
)

// V3ShareRevoker v3 条目的分享撤销出口（分享服务回接后由 ShareService 实现）
type V3ShareRevoker interface {
	// RevokeV3Entries 撤销这些 entry（fs_entry.id）上的活跃分享（条目被删除时）
	RevokeV3Entries(ev *CommitEvent, deletedIDs []string)
}

// v3SideEffects 旧层写副作用的 v3 等价物：sync_log + Bleve FTS + 备份/Git 通知 + 分享撤销
type v3SideEffects struct {
	syncLogs SyncLogService
	backup   BackupService
	git      GitSyncService
	fts      *dao.BleveManager
	blobs    domain.BlobStore
	share    V3ShareRevoker // 可为 nil（分享服务未注入时跳过）
	logger   *zap.Logger
}

// NewV3SideEffects 创建监听实例；backup/git/share/fts 均可为 nil（对应副作用跳过）
func NewV3SideEffects(
	syncLogs SyncLogService,
	backup BackupService,
	git GitSyncService,
	fts *dao.BleveManager,
	blobs domain.BlobStore,
	share V3ShareRevoker,
	logger *zap.Logger,
) CommitListener {
	return &v3SideEffects{
		syncLogs: syncLogs,
		backup:   backup,
		git:      git,
		fts:      fts,
		blobs:    blobs,
		share:    share,
		logger:   logger,
	}
}

// OnCommit 单次提交的全部副作用。均为尽力而为：失败只记日志，绝不影响已落盘的提交。
func (l *v3SideEffects) OnCommit(ev *CommitEvent) {
	if l == nil {
		return
	}
	clientType, clientName, clientVersion := splitClientTag(ev.Client)

	var deletedIDs []string
	for _, ch := range ev.Changes {
		it := ch.Item
		logType := domain.SyncLogTypeFile
		if it.IsNote {
			logType = domain.SyncLogTypeNote
		}

		var action domain.SyncLogAction
		switch ch.Op {
		case "add":
			action = domain.SyncLogActionCreate
		case "modify":
			action = domain.SyncLogActionModify
		case "move":
			action = domain.SyncLogActionRename
		case "delete":
			action = domain.SyncLogActionSoftDelete
			if it.ID != "" {
				deletedIDs = append(deletedIDs, it.ID)
			}
		default:
			continue
		}

		// 同步日志（异步批量落库，channel 满时降级丢弃——审计日志允许）
		if l.syncLogs != nil {
			l.syncLogs.Log(ev.UID, ev.VaultID, logType, action, "", it.Path, "",
				clientType, clientName, clientVersion, it.Size)
		}

		// FTS：仅笔记；move 需以新路径重建文档（内容未变仍需重读，doc 里带 Path）
		if l.fts != nil && l.fts.IsEnabled() && it.IsNote && it.ID != "" {
			switch ch.Op {
			case "add", "modify", "move":
				if content, err := l.blobs.BlobReadAll(ev.UID, it.BlobHash); err != nil {
					l.logger.Warn("v3 fts upsert: read blob failed",
						zap.String("path", it.Path), zap.Error(err))
				} else {
					l.fts.EnqueueUpsert(ev.UID, ev.VaultID, dao.BleveNoteDoc{
						ID:      it.ID,
						Path:    it.Path,
						PathRaw: it.Path,
						Content: string(content),
						Action:  "",
						Rename:  float64(it.Mtime),
						Ctime:   float64(it.Ctime),
						Mtime:   float64(it.Mtime),
					})
				}
			case "delete":
				l.fts.EnqueueDelete(ev.UID, ev.VaultID, it.ID)
			}
		}
	}

	// 分享撤销：被删条目上的活跃分享整体作废（旧层语义：删除即停分享）
	if l.share != nil && len(deletedIDs) > 0 {
		func() {
			defer func() {
				if r := recover(); r != nil {
					l.logger.Error("v3 share revoke panic", zap.Any("recover", r))
				}
			}()
			l.share.RevokeV3Entries(ev, deletedIDs)
		}()
	}

	// 备份 / Git 去抖触发（两个服务内部自带合并与节流）
	if l.backup != nil {
		l.backup.NotifyUpdated(ev.UID)
	}
	if l.git != nil {
		l.git.NotifyUpdated(ev.UID, ev.VaultID)
	}
}

// splitClientTag 客户端标识 "type/name"（v3ClientTag）拆回日志三元组
func splitClientTag(tag string) (clientType, clientName, clientVersion string) {
	if tag == "" {
		return "", "", ""
	}
	parts := strings.SplitN(tag, "/", 2)
	clientType = parts[0]
	if len(parts) > 1 {
		clientName = parts[1]
	}
	return clientType, clientName, ""
}
