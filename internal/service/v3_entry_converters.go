// Package service: v3 条目 → 旧 REST DTO 的字段映射（P5 功能回接）。
// fs_entry 的身份是 UUID；旧 DTO 的数值 ID 字段保留占位（0），v3 身份放 EntryID。
package service

import (
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
)

func entryToNoteDTO(e *domain.FsEntry, content string) *dto.NoteDTO {
	if e == nil {
		return nil
	}
	clientType, clientName, _ := splitClientTag(e.ClientName)
	d := &dto.NoteDTO{
		EntryID:          e.ID,
		Path:             e.Path,
		PathHash:         util.EncodeHash32(e.Path),
		Content:          content,
		ContentHash:      e.BlobHash,
		Version:          e.Mtime,
		Ctime:            e.Ctime,
		Mtime:            e.Mtime,
		Size:             e.Size,
		ClientType:       clientType,
		ClientName:       clientName,
		UpdatedTimestamp: e.UpdatedAt.UnixMilli(),
		UpdatedAt:        timex.Time(e.UpdatedAt),
		CreatedAt:        timex.Time(e.CreatedAt),
	}
	if e.Deleted {
		d.Action = string(domain.NoteActionDelete) // 回收站条目标记（Restore 预检依赖）
	}
	return d
}

func entryToNoteNoContentDTO(e *domain.FsEntry) *dto.NoteNoContentDTO {
	if e == nil {
		return nil
	}
	clientType, clientName, _ := splitClientTag(e.ClientName)
	return &dto.NoteNoContentDTO{
		EntryID:          e.ID,
		Path:             e.Path,
		PathHash:         util.EncodeHash32(e.Path),
		Version:          e.Mtime,
		Ctime:            e.Ctime,
		Mtime:            e.Mtime,
		Size:             e.Size,
		ClientType:       clientType,
		ClientName:       clientName,
		UpdatedTimestamp: e.UpdatedAt.UnixMilli(),
		UpdatedAt:        timex.Time(e.UpdatedAt),
		CreatedAt:        timex.Time(e.CreatedAt),
	}
}

func entryToFileDTO(e *domain.FsEntry) *dto.FileDTO {
	if e == nil {
		return nil
	}
	d := &dto.FileDTO{
		EntryID:          e.ID,
		Path:             e.Path,
		PathHash:         util.EncodeHash32(e.Path),
		ContentHash:      e.BlobHash,
		Size:             e.Size,
		Ctime:            e.Ctime,
		Mtime:            e.Mtime,
		UpdatedTimestamp: e.UpdatedAt.UnixMilli(),
		UpdatedAt:        timex.Time(e.UpdatedAt),
		CreatedAt:        timex.Time(e.CreatedAt),
	}
	if e.Deleted {
		d.Action = string(domain.FileActionDelete) // 回收站条目标记（Restore 预检依赖）
	}
	return d
}

func entryToHistoryNoContentDTO(e *domain.FsEntry, h *domain.EntryHistoryItem) *dto.NoteHistoryNoContentDTO {
	if h == nil {
		return nil
	}
	clientType, clientName, _ := splitClientTag(h.Client)
	return &dto.NoteHistoryNoContentDTO{
		ID:         h.ID,
		EntryID:    h.EntryID,
		VaultID:    h.VaultID,
		Path:       pathOf(e, h),
		ClientType: clientType,
		ClientName: clientName,
		Version:    h.Version,
		CreatedAt:  timex.Time(h.CreatedAt),
	}
}

func pathOf(e *domain.FsEntry, h *domain.EntryHistoryItem) string {
	if e != nil {
		return e.Path
	}
	return ""
}
