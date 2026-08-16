// Package service: NoteLinkService 的 v3 实现（P5 功能回接）。
// v3 提交管线不维护 note_links 索引表——反向/出链在读取时基于清单计算：
// 扫描全部活笔记解析 wiki 链接建内存索引，按 manifest epoch 缓存，提交后自动失效。
// 链接目标匹配沿用旧版后缀变体语义（[[note]] 可命中 projects/folder/note.md），
// 并对 [[target#heading]]、[[note.md]]、%20 编码做了归一化（旧哈希索引命不中的形态）。
package service

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"go.uber.org/zap"
)

// indexedNote 一篇含链接的活笔记（源条目 + 解析出的链接）
type indexedNote struct {
	entry *domain.FsEntry
	links []util.WikiLink
}

// noteLinkIndex 一次全量扫描的解析结果；epoch 变化即整体重建
type noteLinkIndex struct {
	epoch   int64
	sources []*indexedNote
}

type noteLinkServiceV3 struct {
	fsRepo       domain.FsEntryRepository
	manifestRepo domain.VaultManifestRepository
	content      ContentV3Service
	vaultSvc     VaultResolver
	logger       *zap.Logger

	mu    sync.Mutex
	cache map[string]*noteLinkIndex // key: uid:vaultID
}

// NewNoteLinkServiceV3 创建 v3 版 NoteLinkService（REST/MCP 链接查询用）
func NewNoteLinkServiceV3(
	fsRepo domain.FsEntryRepository,
	manifestRepo domain.VaultManifestRepository,
	content ContentV3Service,
	vaultSvc VaultResolver,
	logger *zap.Logger,
) NoteLinkService {
	return &noteLinkServiceV3{
		fsRepo: fsRepo, manifestRepo: manifestRepo, content: content,
		vaultSvc: vaultSvc, logger: logger,
		cache: make(map[string]*noteLinkIndex),
	}
}

// normalizeLinkTarget 归一化链接目标用于变体匹配：去 "#heading"、去 "./" 前缀、
// 去 ".md" 后缀、解码 %20（变体本身不含扩展名，见 util.GeneratePathVariations）
func normalizeLinkTarget(target string) string {
	if i := strings.Index(target, "#"); i >= 0 {
		target = target[:i]
	}
	target = strings.TrimPrefix(target, "./")
	target = strings.TrimSuffix(target, ".md")
	target = strings.ReplaceAll(target, "%20", " ")
	return target
}

// GetBacklinks 获取链接到目标笔记的所有笔记（REST /api/note/backlinks；MCP 同名工具）
func (s *noteLinkServiceV3) GetBacklinks(ctx context.Context, uid int64, params *dto.NoteLinkQueryRequest) ([]*dto.NoteLinkItem, error) {
	v, err := s.vaultSvc.GetOrCreate(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}

	// 目标路径的后缀变体集合：projects/folder/note.md → {note, folder/note, projects/folder/note}
	variations := util.GeneratePathVariations(params.Path)
	if len(variations) == 0 {
		return nil, nil
	}
	matchSet := make(map[string]bool, len(variations))
	for _, variation := range variations {
		matchSet[variation] = true
	}

	idx, err := s.findIndex(ctx, uid, v.ID)
	if err != nil {
		return nil, err
	}

	var results []*dto.NoteLinkItem
	for _, src := range idx.sources {
		var matched *util.WikiLink
		for i := range src.links {
			if matchSet[normalizeLinkTarget(src.links[i].Path)] {
				matched = &src.links[i]
				break
			}
		}
		if matched == nil {
			continue
		}
		item := &dto.NoteLinkItem{
			Path:     src.entry.Path,
			LinkText: matched.Alias,
			IsEmbed:  matched.IsEmbed,
		}
		// 上下文：优先按链接原文定位，退回各变体（与旧版提取行为一致）
		if data, err := s.content.ReadEntryBlob(uid, src.entry.BlobHash); err == nil {
			content := string(data)
			item.Context = s.extractLinkContext(content, matched.Path)
			if item.Context == "" {
				for _, variation := range variations {
					if item.Context = s.extractLinkContext(content, variation); item.Context != "" {
						break
					}
				}
			}
		}
		results = append(results, item)
	}
	return results, nil
}

// GetOutlinks 获取源笔记中的所有链接（REST /api/note/outlinks；MCP 同名工具）
func (s *noteLinkServiceV3) GetOutlinks(ctx context.Context, uid int64, params *dto.NoteLinkQueryRequest) ([]*dto.NoteLinkItem, error) {
	e, err := s.content.GetEntryByPath(ctx, uid, params.Vault, params.Path)
	if err != nil {
		if isCode(err, code.ErrorV3EntryNotFound) {
			return nil, code.ErrorNoteNotFound.WithPath(params.Path)
		}
		return nil, err
	}
	if !e.IsNote {
		return nil, code.ErrorNoteNotFound.WithPath(params.Path)
	}
	data, err := s.content.ReadEntryBlob(uid, e.BlobHash)
	if err != nil {
		return nil, err
	}
	content := string(data)

	links := util.ParseWikiLinks(content)
	results := make([]*dto.NoteLinkItem, 0, len(links))
	for _, link := range links {
		item := &dto.NoteLinkItem{
			Path:     link.Path,
			LinkText: link.Alias,
			IsEmbed:  link.IsEmbed,
		}
		item.Context = s.extractLinkContext(content, link.Path)
		results = append(results, item)
	}
	return results, nil
}

// findIndex 取该 vault 的链接索引；epoch 不匹配则全量重建。
// 索引只保留含链接的笔记——无链接的笔记不可能成为反链来源，无需占用内存。
func (s *noteLinkServiceV3) findIndex(ctx context.Context, uid, vaultID int64) (*noteLinkIndex, error) {
	var epoch int64
	if cur, err := s.manifestRepo.Current(ctx, vaultID, uid); err != nil {
		return nil, code.ErrorDBQuery.WithDetails(err.Error())
	} else if cur != nil {
		epoch = cur.Epoch
	}

	key := fmt.Sprintf("%d:%d", uid, vaultID)
	s.mu.Lock()
	if idx, ok := s.cache[key]; ok && idx.epoch == epoch {
		s.mu.Unlock()
		return idx, nil
	}
	s.mu.Unlock()

	entries, err := s.fsRepo.ListLive(ctx, vaultID, uid)
	if err != nil {
		return nil, code.ErrorDBQuery.WithDetails(err.Error())
	}
	idx := &noteLinkIndex{epoch: epoch}
	for _, e := range entries {
		if !e.IsNote {
			continue
		}
		data, err := s.content.ReadEntryBlob(uid, e.BlobHash)
		if err != nil {
			// 单个 blob 损坏不拖垮整次查询，跳过并记录
			s.logger.Warn("note-link index: skip unreadable blob",
				zap.String("path", e.Path), zap.String("blobHash", e.BlobHash), zap.Error(err))
			continue
		}
		links := util.ParseWikiLinks(string(data))
		if len(links) == 0 {
			continue
		}
		idx.sources = append(idx.sources, &indexedNote{entry: e, links: links})
	}

	s.mu.Lock()
	s.cache[key] = idx
	s.mu.Unlock()
	return idx, nil
}

// extractLinkContext 提取链接周围约 50 个字符的上下文（与旧版一致）
func (s *noteLinkServiceV3) extractLinkContext(content, targetPath string) string {
	// Look for [[targetPath]] or [[targetPath|alias]]
	// 查找 [[targetPath]] 或 [[targetPath|alias]]
	searchPatterns := []string{
		"[[" + targetPath + "]]",
		"[[" + targetPath + "|",
	}

	var pos int = -1
	var matchLen int

	for _, pattern := range searchPatterns {
		idx := strings.Index(content, pattern)
		if idx >= 0 && (pos < 0 || idx < pos) {
			pos = idx
			matchLen = len(pattern)
		}
	}

	if pos < 0 {
		return ""
	}

	// Extract context: 25 chars before and after the link
	// 提取上下文：链接前后各 25 个字符
	contextRadius := 25
	start := pos - contextRadius
	if start < 0 {
		start = 0
	}

	// Find the end of the link (closing ]])
	// 查找链接的结尾（闭合的 ]]）
	linkEnd := strings.Index(content[pos:], "]]")
	if linkEnd < 0 {
		linkEnd = matchLen
	} else {
		linkEnd += 2 // Include ]] / 包含 ]]
	}

	end := pos + linkEnd + contextRadius
	if end > len(content) {
		end = len(content)
	}

	context := content[start:end]

	// Clean up: replace newlines with spaces and trim
	// 清理：将换行符替换为空格并修剪
	context = strings.ReplaceAll(context, "\n", " ")
	context = strings.TrimSpace(context)

	// Add ellipsis if truncated
	// 如果被截断则添加省略号
	if start > 0 {
		context = "..." + context
	}
	if end < len(content) {
		context = context + "..."
	}

	return context
}

var _ NoteLinkService = (*noteLinkServiceV3)(nil)
