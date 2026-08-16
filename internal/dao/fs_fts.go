// Package dao: v3 条目的 Bleve 全文检索。
// 与旧 note 表的 searchFTS 同构，差异仅在文档 ID：v3 用 fs_entry 的 UUID（BleveNoteDoc.ID）。
// 索引由 v3 提交副作用监听（service.v3SideEffects）随提交增量维护，这里只读。
package dao

import (
	"github.com/blevesearch/bleve/v2"
	bleveQuery "github.com/blevesearch/bleve/v2/search/query"
	"go.uber.org/zap"
)

// SearchEntryIDs 按 keyword 检索 vault 的 v3 笔记条目，返回命中的 entry UUID（按排序规则有序）。
// deleted=true 检索墓碑（回收站搜索），false 检索活跃条目。limit<=0 时取全部命中。
// 索引为空时返回空结果（不触发重建——索引进度随提交增长，冷启动首灌后即完整）。
func (m *BleveManager) SearchEntryIDs(uid, vaultID int64, keyword string, deleted bool, sortBy, sortOrder string, limit int) ([]string, error) {
	index, err := m.GetIndex(uid, vaultID)
	if err != nil {
		return nil, err
	}
	if docCount, err := index.DocCount(); err == nil && docCount == 0 {
		return nil, nil
	}

	var actionQuery bleveQuery.Query
	deleteTerm := bleve.NewTermQuery("delete")
	deleteTerm.SetField("action")
	if deleted {
		actionQuery = deleteTerm
	} else {
		boolQuery := bleve.NewBooleanQuery()
		boolQuery.AddMustNot(deleteTerm)
		actionQuery = boolQuery
	}

	pathQuery := bleve.NewMatchQuery(keyword)
	pathQuery.SetField("path")
	pathQuery.Operator = bleveQuery.MatchQueryOperatorAnd
	contentQuery := bleve.NewMatchQuery(keyword)
	contentQuery.SetField("content")
	contentQuery.Operator = bleveQuery.MatchQueryOperatorAnd

	query := bleve.NewConjunctionQuery(
		bleve.NewDisjunctionQuery(pathQuery, contentQuery),
		actionQuery,
	)

	req := bleve.NewSearchRequest(query)
	if limit > 0 {
		req.Size = limit
	}
	if sortOrder == "" {
		sortOrder = "desc"
	}
	sortField := getSortField(sortBy)
	if sortField == "path" {
		sortField = "path_raw"
	}
	if sortOrder == "desc" {
		sortField = "-" + sortField
	}
	req.SortBy([]string{sortField})

	res, err := index.Search(req)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(res.Hits))
	for _, hit := range res.Hits {
		ids = append(ids, hit.ID)
	}
	m.logger.Debug("v3 FTS search",
		zap.String("keyword", keyword),
		zap.Int64("uid", uid), zap.Int64("vaultID", vaultID),
		zap.Int("hits", len(ids)))
	return ids, nil
}
