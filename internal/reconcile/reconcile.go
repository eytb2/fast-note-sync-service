// Package reconcile 实现 git 式快照同步的三方对账引擎（git-sync-redesign.md §2.1/§2.3）。
//
// 输入三方：L（客户端声明树）、B（客户端基线，baseEpoch 时的服务器清单）、S（服务器当前清单）。
// 输出 Plan：
//   - Ops       客户端需本地应用的操作（拉取/移动/删除）
//   - Uploads   客户端可能需要上传的 blob（调用方按 blob store 存在性过滤后发 BlobNeed）
//   - Conflicts 冲突项（内容级三路合并由调用方执行，引擎只标记）
//   - Expected  服务器认可、期望客户端放入 ManifestCommit 的变更集
//
// 引擎是纯函数：不碰存储、不读时钟、无随机；同名输入必得同名输出（测试与多端一致性都依赖这一点）。
package reconcile

import (
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
)

// Tombstone 客户端本地墓碑：「曾同步过、现已不存在」（§2.2）。
type Tombstone struct {
	Path string `json:"path"`
	ID   string `json:"id"`
}

// Scope 客户端声明范围（§2.3 sparse 对账）。nil = 全量客户端。
//
// 模式串语法（include/exclude 共用，客户端按同一语义实现）：
//   - "re:<正则>"：前缀 re: 标记为正则，锚定开头（服务端自动加 ^），忽略大小写；
//   - 其余：路径段前缀（"a/b" 匹配 "a/b" 与 "a/b/..."，不匹配 "a/bx"）。
type Scope struct {
	Include []string `json:"include"`           // 声明前缀/正则；空 = 不限
	Exclude []string `json:"exclude,omitempty"` // 排除前缀/正则（插件"排除规则"），优先于 include
	Types   []string `json:"types"`             // note | attachment | config；空 = 全部
}

// scopeRegexCache 已编译的 re: 模式缓存（模式串数量有限，避免每次对账重复编译）
var scopeRegexCache sync.Map

// scopeMatch 单个模式匹配：re: 前缀走正则（锚定 ^、忽略大小写），否则路径段前缀。
// 非法正则按字面前缀处理（与旧插件 isPathMatch 的兜底一致，避免整条规则失效）。
func scopeMatch(pattern, path string) bool {
	if rest, ok := strings.CutPrefix(pattern, "re:"); ok {
		var re *regexp.Regexp
		if cached, ok := scopeRegexCache.Load(rest); ok {
			re = cached.(*regexp.Regexp)
		} else {
			compiled, err := regexp.Compile("(?i)^" + rest)
			if err != nil {
				return prefixMatch(rest, path)
			}
			re = compiled
			scopeRegexCache.Store(rest, compiled)
		}
		return re.MatchString(path)
	}
	return prefixMatch(pattern, path)
}

// scopeMatchAny 任一模式命中即真（空列表 = 不命中任何 ⇒ 调用方据此表达"不限"）。
func scopeMatchAny(patterns []string, path string) bool {
	for _, p := range patterns {
		if scopeMatch(p, path) {
			return true
		}
	}
	return false
}

// Match 判断条目是否落在声明范围内。
func (s *Scope) Match(path string, isNote bool) bool {
	if s == nil {
		return true
	}
	// exclude 优先：命中排除 ⇒ 不参与对账（即使同时在 include 内）。
	// 曾同步过、后被排除的文件经 restrict() 从 B'/S' 中移出 → 不拉取、不删除、在服务器原地保留。
	if len(s.Exclude) > 0 && scopeMatchAny(s.Exclude, path) {
		return false
	}
	if len(s.Include) > 0 && !scopeMatchAny(s.Include, path) {
		return false
	}
	if len(s.Types) > 0 && !typeMatchAny(s.Types, path, isNote) {
		return false
	}
	return true
}

// prefixMatch 单个路径段前缀匹配："a/b" 匹配 "a/b" 与 "a/b/..."，不匹配 "a/bx"。
func prefixMatch(prefix, path string) bool {
	p := strings.TrimSuffix(prefix, "/")
	return path == p || strings.HasPrefix(path, p+"/")
}

func prefixMatchAny(prefixes []string, path string) bool {
	for _, p := range prefixes {
		if prefixMatch(p, path) {
			return true
		}
	}
	return false
}

// scopeType 条目类别：配置（.obsidian/ 前缀）/ 笔记 / 附件。
func scopeType(path string, isNote bool) string {
	switch {
	case strings.HasPrefix(path, ".obsidian/"):
		return "config"
	case isNote:
		return "note"
	default:
		return "attachment"
	}
}

func typeMatchAny(types []string, path string, isNote bool) bool {
	t := scopeType(path, isNote)
	for _, want := range types {
		if want == t {
			return true
		}
	}
	return false
}

// OpKind 客户端需应用的操作。
type OpKind string

const (
	OpPull   OpKind = "pull"   // 服务器内容较新或服务器新增 → 客户端拉取（笔记内联，附件分块）
	OpMove   OpKind = "move"   // 服务器已移动 → 客户端本地重命名（必须走 fileManager.renameFile 以更新双链）
	OpDelete OpKind = "delete" // 服务器已删除 → 客户端本地删除并记墓碑
)

// Op 下发给客户端的操作。
type Op struct {
	Kind OpKind              `json:"op"`
	Item domain.ManifestItem `json:"item"`           // 目标条目（move 时 Path=新路径）
	From string              `json:"from,omitempty"` // move 的旧路径
}

// Upload 客户端待上传 blob 候选（服务端再按 blob 存在性过滤）。
type Upload struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

// ConflictKind 冲突类别。
type ConflictKind string

const (
	ConflictModify ConflictKind = "modify" // 双方相对基线都修改了同一路径
	ConflictAdd    ConflictKind = "add"    // 双方独立新增了同一路径（无基线）
)

// Conflict 冲突项：笔记走三路合并（base=B 内容），附件/合并失败走 .conflict. 副本（§2.1）。
type Conflict struct {
	Path        string       `json:"path"`
	Kind        ConflictKind `json:"kind"`
	ID          string       `json:"id"`
	BaseHash    string       `json:"baseHash,omitempty"`
	ServerHash  string       `json:"serverHash"`
	ServerMtime int64        `json:"serverMtime,omitempty"` // newest-wins 策略的比较依据（客户端本地 mtime 自持）
	LocalHash   string       `json:"localHash"`
	IsNote      bool         `json:"isNote"`
}

// Change ManifestCommit 的一条变更（Expected 的元素）。
type Change struct {
	Op      string              `json:"op"`                // add | modify | delete | move
	OldPath string              `json:"oldPath,omitempty"` // move 专用
	Item    domain.ManifestItem `json:"item"`
}

// Plan 对账计划（SyncPlan 的内核）。
type Plan struct {
	Ops       []Op       `json:"ops"`
	Uploads   []Upload   `json:"uploads"`
	Conflicts []Conflict `json:"conflicts"`
	Expected  []Change   `json:"expected"`
}

// Input 引擎输入。Base 为空即无基线（首次同步，§2.1 末条：以 S 为准全量拉取，本地多出的按新增上传）。
type Input struct {
	Local      []domain.ManifestItem
	Tombstones []Tombstone
	Base       []domain.ManifestItem
	Server     []domain.ManifestItem
	Scope      *Scope
}

// ==================== 索引 ====================

type itemIndex struct {
	byPath map[string]domain.ManifestItem
	byID   map[string]domain.ManifestItem
}

func indexItems(items []domain.ManifestItem) *itemIndex {
	ix := &itemIndex{
		byPath: make(map[string]domain.ManifestItem, len(items)),
		byID:   make(map[string]domain.ManifestItem, len(items)),
	}
	for _, it := range items {
		if it.Path == "" {
			continue
		}
		if _, dup := ix.byPath[it.Path]; !dup {
			ix.byPath[it.Path] = it
		}
		if it.ID != "" {
			if _, dup := ix.byID[it.ID]; !dup {
				ix.byID[it.ID] = it
			}
		}
	}
	return ix
}

// restrict 把 B/S 收敛到「本次声明的路径 ∪ L 中出现的路径 ∪ 客户端墓碑路径」（§2.3）。
// 这样 scope 之外的条目既不产生 pull，也不会被解释为删除。
func restrict(ix *itemIndex, sc *Scope, local []domain.ManifestItem, tombs []Tombstone) *itemIndex {
	if sc == nil {
		return ix
	}
	keep := func(path string, isNote bool) bool {
		if sc.Match(path, isNote) {
			return true
		}
		for _, l := range local {
			if l.Path == path {
				return true
			}
		}
		for _, t := range tombs {
			if t.Path == path {
				return true
			}
		}
		return false
	}
	out := &itemIndex{byPath: map[string]domain.ManifestItem{}, byID: map[string]domain.ManifestItem{}}
	for p, it := range ix.byPath {
		if keep(p, it.IsNote) {
			out.byPath[p] = it
			if it.ID != "" {
				out.byID[it.ID] = it
			}
		}
	}
	return out
}

func tombstoneHit(tombs []Tombstone, id, path string) bool {
	for _, t := range tombs {
		if (t.ID != "" && t.ID == id) || (t.Path != "" && t.Path == path) {
			return true
		}
	}
	return false
}

// ==================== 主流程 ====================

// Reconcile 执行三方对账。规则见设计文档 §2.1；move 检测两轮：先按 id（§2.1），后按同哈希消失+出现。
func Reconcile(in Input) *Plan {
	// 空集合必须编出 []（nil slice 会变 null，客户端按数组迭代会炸）
	p := &Plan{Ops: []Op{}, Conflicts: []Conflict{}, Expected: []Change{}}

	lx := indexItems(in.Local)
	bx := indexItems(in.Base)
	sx := indexItems(in.Server)

	// Phase 0：scope 收敛
	bx = restrict(bx, in.Scope, in.Local, in.Tombstones)
	sx = restrict(sx, in.Scope, in.Local, in.Tombstones)

	exempt := map[string]bool{} // 已由 move 判定处理（或虚拟化基线后不再参与）的路径

	// Phase 1a：服务器侧 move（id 匹配；客户端落后于服务器）
	for _, s := range sortedByID(sx.byID) {
		b, ok := bx.byID[s.ID]
		if !ok || b.Path == s.Path {
			continue
		}
		l, hasL := lx.byID[s.ID]
		switch {
		case !hasL:
			// 客户端连 id 都没有（从未同步过该文件）或已删除：
			// 墓碑命中 ⇒ 客户端删了它，期望删除；否则交给 per-path（旧路径 FTF 无操作、新路径 FFT 拉取，收敛为按新路径恢复）。
			if tombstoneHit(in.Tombstones, s.ID, b.Path) {
				p.Expected = append(p.Expected, Change{Op: "delete", Item: s})
				exempt[b.Path] = true
				exempt[s.Path] = true
			}
		case l.Path == s.Path:
			// 客户端已应用该 move：把基线条目虚拟挪到新路径，让 per-path 按普通内容 diff 判定
			nb := b
			nb.Path = s.Path
			bx.byPath[s.Path] = nb
			delete(bx.byPath, b.Path)
		case l.Path == b.Path:
			// 客户端还在旧路径：应用 move
			p.Ops = append(p.Ops, Op{Kind: OpMove, Item: s, From: b.Path})
			exempt[b.Path] = true
			exempt[s.Path] = true
			if l.BlobHash != s.BlobHash {
				if l.BlobHash == b.BlobHash {
					p.Ops = append(p.Ops, Op{Kind: OpPull, Item: s}) // 移动 + 服务器内容更新
				} else {
					p.Conflicts = append(p.Conflicts, conflict(ConflictModify, s, l, &b)) // 移动 + 双方都改
				}
			}
		default:
			// 双方把同一文件移到了不同路径：内容冲突挂在服务器新路径（本端 l.Path 由 per-path 作为客户端 move 处理）
			p.Conflicts = append(p.Conflicts, conflict(ConflictModify, s, l, &b))
			exempt[b.Path] = true
			exempt[s.Path] = true
		}
	}

	// Phase 1b：客户端侧 move（id 匹配；服务器落后于客户端）
	for _, l := range sortedByID(lx.byID) {
		if l.ID == "" {
			continue
		}
		s, ok := sx.byID[l.ID]
		if !ok || s.Path == l.Path {
			continue // 服务器无此条目（被清除）→ per-path 按新增处理；同路径 → per-path 处理内容
		}
		b, hasB := bx.byID[l.ID]
		if !hasB || b.Path != s.Path {
			continue // 服务器侧也动过这个 id（Phase 1a 已判）→ 不再反判为客户端 move
		}
		// 服务器还在 s.Path，客户端已挪到 l.Path。
		// 注意目标路径被服务器上另一条目占用时不在此处判 move，由 per-path 在 l.Path 产生冲突（提交侧做冲突副本）。
		p.Expected = append(p.Expected, Change{Op: "move", OldPath: s.Path, Item: l})
		if l.BlobHash != s.BlobHash {
			p.Uploads = append(p.Uploads, Upload{Path: l.Path, Hash: l.BlobHash, Size: l.Size})
		}
		exempt[s.Path] = true
		exempt[l.Path] = true
	}

	// Phase 2：逐路径三方 diff（universe = L ∪ B' ∪ S'，扣除已判 move 的端点）
	for _, path := range sortedPaths(lx.byPath, bx.byPath, sx.byPath) {
		if exempt[path] {
			continue
		}
		l, hasL := lx.byPath[path]
		b, hasB := bx.byPath[path]
		s, hasS := sx.byPath[path]

		switch {
		case hasL && hasS:
			if l.BlobHash == s.BlobHash {
				continue // 已一致（id 采纳靠提交与基线刷新）
			}
			switch {
			case hasB && l.BlobHash == b.BlobHash:
				p.Ops = append(p.Ops, Op{Kind: OpPull, Item: s}) // 服务器改了，fast-forward
			case hasB && s.BlobHash == b.BlobHash:
				p.expect(l, "modify") // 客户端改了，服务器认可
			case !hasB:
				p.Conflicts = append(p.Conflicts, conflict(ConflictAdd, s, l, nil)) // 双方独立新增
			default:
				p.Conflicts = append(p.Conflicts, conflict(ConflictModify, s, l, &b)) // 双方都改
			}

		case hasL && !hasS:
			if hasB {
				// 服务器删了它 → 本地删除并记墓碑（本地有未提交修改也一样，删除胜出，§2.1）
				p.Ops = append(p.Ops, Op{Kind: OpDelete, Item: l})
			} else {
				p.expect(l, "add") // 客户端新文件（或 move 目标，Phase 3 识别）
			}

		case !hasL && hasS:
			if hasB {
				if tombstoneHit(in.Tombstones, s.ID, s.Path) {
					p.expect(s, "delete") // 墓碑明确：客户端删了它 → 上报删除
				} else if in.Scope == nil {
					p.expect(s, "delete") // 全量客户端：缺失即删除（git 工作树语义）
				} else {
					p.Ops = append(p.Ops, Op{Kind: OpPull, Item: s}) // 稀疏客户端墓碑丢失/scope 扩张 → 拉回（宁重复勿丢数据）
				}
			} else {
				p.Ops = append(p.Ops, Op{Kind: OpPull, Item: s}) // 服务器新增 → 拉取
			}

		default:
			// !hasL && !hasS && hasB：双方都删 → 无操作（客户端可在 ack 后清掉对应墓碑）
		}
	}

	// Phase 3：离线移动推断（move 检测第二轮，§2.1）：
	// 同哈希的「删除 + 新增」⇒ move（客户端离线期间移动、且不知 id）。
	inferHashMoves(p)

	sortPlan(p)
	return p
}

// expect 记录一条期望提交（add/modify/delete 的公共入口，Phase 3 需要按 Op 归位）。
func (p *Plan) expect(it domain.ManifestItem, op string) {
	p.Expected = append(p.Expected, Change{Op: op, Item: it})
	if op == "add" || op == "modify" {
		p.Uploads = append(p.Uploads, Upload{Path: it.Path, Hash: it.BlobHash, Size: it.Size})
	}
}

func conflict(kind ConflictKind, s, l domain.ManifestItem, b *domain.ManifestItem) Conflict {
	c := Conflict{
		Kind:        kind,
		Path:        s.Path,
		ID:          s.ID,
		ServerHash:  s.BlobHash,
		ServerMtime: s.Mtime,
		LocalHash:   l.BlobHash,
		IsNote:      s.IsNote,
	}
	if b != nil {
		c.BaseHash = b.BlobHash
	}
	return c
}

// inferHashMoves 把 Expected 中同哈希的 (delete, add) 对折成 move；继承被删条目的服务器身份。
// 多候选时按路径序取第一个（确定性）；不配对时保持 delete+add（内容仍收敛，仅丢失身份链）。
func inferHashMoves(p *Plan) {
	dels := map[string][]int{} // hash → Expected 下标
	for i, c := range p.Expected {
		if c.Op == "delete" {
			h := c.Item.BlobHash
			dels[h] = append(dels[h], i)
		}
	}
	if len(dels) == 0 {
		return
	}
	// 同哈希候选按路径排序，保证配对确定性
	for h := range dels {
		idxs := dels[h]
		sort.Slice(idxs, func(a, b int) bool {
			return p.Expected[idxs[a]].Item.Path < p.Expected[idxs[b]].Item.Path
		})
	}

	remove := map[int]bool{}
	for i, c := range p.Expected {
		if c.Op != "add" {
			continue
		}
		var del Change
		delIdx, found := -1, false
		for len(dels[c.Item.BlobHash]) > 0 {
			cand := dels[c.Item.BlobHash][0]
			d := p.Expected[cand]
			dels[c.Item.BlobHash] = dels[c.Item.BlobHash][1:]
			if c.Item.ID != "" && c.Item.ID != d.Item.ID {
				continue // id 已知且不兼容：不是同一个文件
			}
			del, delIdx, found = d, cand, true
			break
		}
		if !found {
			continue
		}
		remove[i] = true
		remove[delIdx] = true
		it := c.Item
		if it.ID == "" {
			it.ID = del.Item.ID // 继承服务器身份，历史/分享不断链
		}
		p.Expected = append(p.Expected, Change{Op: "move", OldPath: del.Item.Path, Item: it})
	}
	if len(remove) == 0 {
		return
	}
	kept := p.Expected[:0]
	for i, c := range p.Expected {
		if !remove[i] {
			kept = append(kept, c)
		}
	}
	p.Expected = kept
}

// ==================== 排序（确定性输出） ====================

func sortedByID(m map[string]domain.ManifestItem) []domain.ManifestItem {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]domain.ManifestItem, 0, len(ids))
	for _, id := range ids {
		out = append(out, m[id])
	}
	return out
}

func sortedPaths(maps ...map[string]domain.ManifestItem) []string {
	set := map[string]bool{}
	for _, m := range maps {
		for p := range m {
			set[p] = true
		}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func sortPlan(p *Plan) {
	sort.Slice(p.Ops, func(a, b int) bool {
		if p.Ops[a].Kind != p.Ops[b].Kind {
			return p.Ops[a].Kind < p.Ops[b].Kind
		}
		return p.Ops[a].Item.Path < p.Ops[b].Item.Path
	})
	sort.Slice(p.Uploads, func(a, b int) bool { return p.Uploads[a].Path < p.Uploads[b].Path })
	sort.Slice(p.Conflicts, func(a, b int) bool { return p.Conflicts[a].Path < p.Conflicts[b].Path })
	sort.Slice(p.Expected, func(a, b int) bool {
		if p.Expected[a].Op != p.Expected[b].Op {
			return p.Expected[a].Op < p.Expected[b].Op
		}
		return p.Expected[a].Item.Path < p.Expected[b].Item.Path
	})
}
