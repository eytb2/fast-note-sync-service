package reconcile

import (
	"reflect"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
)

func it(id, path, hash string) domain.ManifestItem {
	return domain.ManifestItem{ID: id, Path: path, BlobHash: hash, IsNote: true, Size: int64(len(hash))}
}

func att(id, path, hash string) domain.ManifestItem {
	return domain.ManifestItem{ID: id, Path: path, BlobHash: hash, IsNote: false, Size: int64(len(hash))}
}

func findOp(p *Plan, kind OpKind, path string) *Op {
	for i := range p.Ops {
		if p.Ops[i].Kind == kind && p.Ops[i].Item.Path == path {
			return &p.Ops[i]
		}
	}
	return nil
}

func findChange(p *Plan, op, path string) *Change {
	for i := range p.Expected {
		if p.Expected[i].Op == op && p.Expected[i].Item.Path == path {
			return &p.Expected[i]
		}
	}
	return nil
}

func findConflict(p *Plan, path string) *Conflict {
	for i := range p.Conflicts {
		if p.Conflicts[i].Path == path {
			return &p.Conflicts[i]
		}
	}
	return nil
}

func findUpload(p *Plan, path string) *Upload {
	for i := range p.Uploads {
		if p.Uploads[i].Path == path {
			return &p.Uploads[i]
		}
	}
	return nil
}

func TestReconcile(t *testing.T) {
	t.Run("三方一致 → 空计划", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "a.md", "h1")},
			Base:   []domain.ManifestItem{it("1", "a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "a.md", "h1")},
		})
		if len(p.Ops)+len(p.Uploads)+len(p.Conflicts)+len(p.Expected) != 0 {
			t.Fatalf("期望空计划， got %+v", p)
		}
	})

	t.Run("服务器新增 → pull", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  nil,
			Server: []domain.ManifestItem{it("1", "a.md", "h1")},
		})
		if op := findOp(p, OpPull, "a.md"); op == nil || op.Item.ID != "1" {
			t.Fatalf("期望 pull a.md， got %+v", p.Ops)
		}
		if len(p.Expected) != 0 {
			t.Fatalf("不应有期望提交: %+v", p.Expected)
		}
	})

	t.Run("客户端新增 → expected add + upload", func(t *testing.T) {
		p := Reconcile(Input{
			Local: []domain.ManifestItem{it("", "new.md", "h1")},
		})
		c := findChange(p, "add", "new.md")
		if c == nil {
			t.Fatalf("期望 add new.md: %+v", p.Expected)
		}
		if u := findUpload(p, "new.md"); u == nil || u.Hash != "h1" {
			t.Fatalf("期望 upload new.md h1: %+v", p.Uploads)
		}
	})

	t.Run("服务器修改（客户端==基线）→ pull", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "a.md", "h1")},
			Base:   []domain.ManifestItem{it("1", "a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "a.md", "h2")},
		})
		if op := findOp(p, OpPull, "a.md"); op == nil || op.Item.BlobHash != "h2" {
			t.Fatalf("期望 pull a.md h2: %+v", p.Ops)
		}
	})

	t.Run("客户端修改（服务器==基线）→ expected modify + upload", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "a.md", "h2")},
			Base:   []domain.ManifestItem{it("1", "a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "a.md", "h1")},
		})
		if c := findChange(p, "modify", "a.md"); c == nil || c.Item.ID != "1" {
			t.Fatalf("期望 modify a.md: %+v", p.Expected)
		}
		if u := findUpload(p, "a.md"); u == nil || u.Hash != "h2" {
			t.Fatalf("期望 upload a.md h2: %+v", p.Uploads)
		}
	})

	t.Run("双方都改笔记 → modify 冲突", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "a.md", "h2")},
			Base:   []domain.ManifestItem{it("1", "a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "a.md", "h3")},
		})
		c := findConflict(p, "a.md")
		if c == nil || c.Kind != ConflictModify || c.BaseHash != "h1" || c.ServerHash != "h3" || c.LocalHash != "h2" || !c.IsNote {
			t.Fatalf("期望 modify 冲突: %+v", p.Conflicts)
		}
	})

	t.Run("双方独立新增同路径同内容 → 收敛无操作", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("", "a.md", "h1")},
			Server: []domain.ManifestItem{it("9", "a.md", "h1")},
		})
		if len(p.Ops)+len(p.Conflicts)+len(p.Expected) != 0 {
			t.Fatalf("同内容应无操作: %+v", p)
		}
	})

	t.Run("双方独立新增同路径不同内容 → add 冲突", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("", "a.md", "h1")},
			Server: []domain.ManifestItem{it("9", "a.md", "h2")},
		})
		c := findConflict(p, "a.md")
		if c == nil || c.Kind != ConflictAdd || c.BaseHash != "" {
			t.Fatalf("期望 add 冲突: %+v", p.Conflicts)
		}
	})

	t.Run("服务器删除（本地还在）→ op delete", func(t *testing.T) {
		p := Reconcile(Input{
			Local: []domain.ManifestItem{it("1", "a.md", "h1")},
			Base:  []domain.ManifestItem{it("1", "a.md", "h1")},
		})
		if op := findOp(p, OpDelete, "a.md"); op == nil {
			t.Fatalf("期望 delete a.md: %+v", p.Ops)
		}
	})

	t.Run("客户端删除（全量、无墓碑）→ expected delete", func(t *testing.T) {
		p := Reconcile(Input{
			Base:   []domain.ManifestItem{it("1", "a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "a.md", "h1")},
		})
		if c := findChange(p, "delete", "a.md"); c == nil {
			t.Fatalf("期望 delete a.md: %+v", p.Expected)
		}
	})

	t.Run("客户端删除（稀疏 + 墓碑）→ expected delete", func(t *testing.T) {
		p := Reconcile(Input{
			Tombstones: []Tombstone{{Path: "a.md", ID: "1"}},
			Base:       []domain.ManifestItem{it("1", "a.md", "h1")},
			Server:     []domain.ManifestItem{it("1", "a.md", "h1")},
			Scope:      &Scope{Include: []string{"a"}},
		})
		if c := findChange(p, "delete", "a.md"); c == nil {
			t.Fatalf("期望 delete a.md: %+v", p.Expected)
		}
		if len(p.Ops) != 0 {
			t.Fatalf("墓碑明确时不应 pull: %+v", p.Ops)
		}
	})

	t.Run("稀疏客户端无墓碑缺失 → 拉回（宁重复勿丢）", func(t *testing.T) {
		p := Reconcile(Input{
			Base:   []domain.ManifestItem{it("1", "a/a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "a/a.md", "h1")},
			Scope:  &Scope{Include: []string{"a"}},
		})
		if op := findOp(p, OpPull, "a/a.md"); op == nil {
			t.Fatalf("期望 pull a/a.md: %+v", p.Ops)
		}
		if len(p.Expected) != 0 {
			t.Fatalf("不应推断删除: %+v", p.Expected)
		}
	})

	t.Run("双方都删 → 无操作", func(t *testing.T) {
		p := Reconcile(Input{
			Base: []domain.ManifestItem{it("1", "a.md", "h1")},
		})
		if len(p.Ops)+len(p.Expected)+len(p.Conflicts) != 0 {
			t.Fatalf("双方都删应无操作: %+v", p)
		}
	})

	t.Run("服务器移动（客户端在旧路径）→ op move，无传输", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "old/a.md", "h1")},
			Base:   []domain.ManifestItem{it("1", "old/a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "new/a.md", "h1")},
		})
		op := findOp(p, OpMove, "new/a.md")
		if op == nil || op.From != "old/a.md" {
			t.Fatalf("期望 move old→new: %+v", p.Ops)
		}
		if len(p.Uploads)+len(p.Expected)+len(p.Conflicts) != 0 {
			t.Fatalf("纯 move 不应有其他输出: %+v", p)
		}
	})

	t.Run("服务器移动+改内容（客户端已应用 move）→ pull", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "new/a.md", "h1")},
			Base:   []domain.ManifestItem{it("1", "old/a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "new/a.md", "h2")},
		})
		if op := findOp(p, OpPull, "new/a.md"); op == nil || op.Item.BlobHash != "h2" {
			t.Fatalf("期望 pull new/a.md h2: %+v", p.Ops)
		}
		if len(p.Conflicts) != 0 {
			t.Fatalf("客户端内容==基线不应冲突: %+v", p.Conflicts)
		}
	})

	t.Run("服务器移动+双方改 → move + modify 冲突", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "old/a.md", "h3")},
			Base:   []domain.ManifestItem{it("1", "old/a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "new/a.md", "h2")},
		})
		if op := findOp(p, OpMove, "new/a.md"); op == nil {
			t.Fatalf("期望 move: %+v", p.Ops)
		}
		c := findConflict(p, "new/a.md")
		if c == nil || c.Kind != ConflictModify || c.BaseHash != "h1" {
			t.Fatalf("期望冲突挂新路径: %+v", p.Conflicts)
		}
	})

	t.Run("客户端移动（id 已知）→ expected move，无传输", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "new/a.md", "h1")},
			Base:   []domain.ManifestItem{it("1", "old/a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "old/a.md", "h1")},
		})
		c := findChange(p, "move", "new/a.md")
		if c == nil || c.OldPath != "old/a.md" || c.Item.ID != "1" {
			t.Fatalf("期望 move old→new: %+v", p.Expected)
		}
		if len(p.Uploads) != 0 {
			t.Fatalf("内容未变不应 upload: %+v", p.Uploads)
		}
		if len(p.Expected) != 1 {
			t.Fatalf("不应推断 delete: %+v", p.Expected)
		}
	})

	t.Run("客户端移动+修改 → expected move + upload", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "new/a.md", "h2")},
			Base:   []domain.ManifestItem{it("1", "old/a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "old/a.md", "h1")},
		})
		if findChange(p, "move", "new/a.md") == nil {
			t.Fatalf("期望 move: %+v", p.Expected)
		}
		if u := findUpload(p, "new/a.md"); u == nil || u.Hash != "h2" {
			t.Fatalf("期望 upload h2: %+v", p.Uploads)
		}
	})

	t.Run("离线移动（id 未知，同哈希消失+出现）→ 推断 move 并继承身份", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("", "new/a.md", "h1")},
			Base:   []domain.ManifestItem{it("1", "old/a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "old/a.md", "h1")},
		})
		c := findChange(p, "move", "new/a.md")
		if c == nil || c.OldPath != "old/a.md" || c.Item.ID != "1" {
			t.Fatalf("期望推断 move 且继承 id: %+v", p.Expected)
		}
		if len(p.Expected) != 1 {
			t.Fatalf("delete+add 应折叠为单条 move: %+v", p.Expected)
		}
	})

	t.Run("真删除+同内容新文件（id 已知且不同）→ 不配对 move", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("2", "b.md", "h1")},
			Base:   []domain.ManifestItem{it("1", "a.md", "h1")},
			Server: []domain.ManifestItem{it("1", "a.md", "h1")},
		})
		// 本地 b.md 是全新文件（新 id 2），a.md 是删除；id 不兼容 ⇒ 不折叠
		if findChange(p, "move", "b.md") != nil {
			t.Fatalf("id 不兼容不应配对: %+v", p.Expected)
		}
		if findChange(p, "delete", "a.md") == nil || findChange(p, "add", "b.md") == nil {
			t.Fatalf("应保持 delete+add: %+v", p.Expected)
		}
	})

	t.Run("无基线首同步 → 全量拉取 + 本地全量上报", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("", "mine.md", "h1")},
			Server: []domain.ManifestItem{it("9", "srv.md", "h2")},
		})
		if findOp(p, OpPull, "srv.md") == nil {
			t.Fatalf("期望 pull srv.md: %+v", p.Ops)
		}
		if findChange(p, "add", "mine.md") == nil {
			t.Fatalf("期望 add mine.md: %+v", p.Expected)
		}
	})

	t.Run("scope 外的服务器条目 → 不拉不删", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "a/keep.md", "h1")},
			Base:   []domain.ManifestItem{it("1", "a/keep.md", "h1"), att("2", "b/pic.png", "h2")},
			Server: []domain.ManifestItem{it("1", "a/keep.md", "h1"), att("2", "b/pic.png", "h2"), it("3", "c/x.md", "h3")},
			Scope:  &Scope{Include: []string{"a"}, Types: []string{"note"}},
		})
		if findOp(p, OpPull, "b/pic.png") != nil || findOp(p, OpPull, "c/x.md") != nil {
			t.Fatalf("scope 外不应 pull: %+v", p.Ops)
		}
		if findChange(p, "delete", "b/pic.png") != nil {
			t.Fatalf("scope 外不应推断删除: %+v", p.Expected)
		}
		if len(p.Ops)+len(p.Expected)+len(p.Conflicts) != 0 {
			t.Fatalf("scope 内一致的条目应无操作: %+v", p)
		}
	})

	t.Run("附件排除（types=note）→ 附件不产生 pull", func(t *testing.T) {
		p := Reconcile(Input{
			Server: []domain.ManifestItem{it("1", "a/x.md", "h1"), att("2", "a/p.png", "h2")},
			Scope:  &Scope{Include: []string{"a"}, Types: []string{"note"}},
		})
		if findOp(p, OpPull, "a/p.png") != nil {
			t.Fatalf("附件在 types 外不应 pull: %+v", p.Ops)
		}
		if findOp(p, OpPull, "a/x.md") == nil {
			t.Fatalf("笔记应 pull: %+v", p.Ops)
		}
	})

	t.Run("确定性：同输入两次结果完全一致", func(t *testing.T) {
		in := Input{
			Local:  []domain.ManifestItem{it("1", "a.md", "h2"), it("", "n.md", "h9"), it("3", "d/c.md", "h4")},
			Base:   []domain.ManifestItem{it("1", "a.md", "h1"), it("2", "b.md", "h5"), it("3", "c.md", "h4")},
			Server: []domain.ManifestItem{it("1", "a.md", "h3"), it("2", "b.md", "h5"), it("3", "c.md", "h4"), it("8", "z.md", "h7")},
		}
		a, b := Reconcile(in), Reconcile(in)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("引擎必须确定性输出")
		}
	})
}

func TestScopeMatch(t *testing.T) {
	s := &Scope{Include: []string{"notes", "docs/"}}
	cases := []struct {
		path string
		want bool
	}{
		{"notes/a.md", true},
		{"notes/sub/b.md", true},
		{"notes", true},
		{"notesx/a.md", false}, // 段边界，不是字符串前缀
		{"docs/a.md", true},
		{"other/a.md", false},
	}
	for _, c := range cases {
		if got := prefixMatchAny(s.Include, c.path); got != c.want {
			t.Errorf("prefixMatchAny(%q) = %v, want %v", c.path, got, c.want)
		}
	}
	if !(typeMatchAny([]string{"config"}, ".obsidian/app.json", false) &&
		!typeMatchAny([]string{"note"}, ".obsidian/app.json", true) &&
		typeMatchAny([]string{"note"}, "a/b.md", true) &&
		typeMatchAny([]string{"attachment"}, "a/b.png", false) &&
		!typeMatchAny([]string{"note"}, "a/b.png", false)) {
		t.Fatalf("typeMatchAny 分类错误")
	}

	// exclude 优先于 include：命中排除段即出局
	sx := &Scope{Include: []string{"notes"}, Exclude: []string{"notes/private"}}
	for _, c := range []struct {
		path string
		want bool
	}{
		{"notes/a.md", true},
		{"notes/private/x.md", false},
		{"notes/private/deep/y.md", false},
		{"other/a.md", false}, // 未命中 include
	} {
		if got := sx.Match(c.path, true); got != c.want {
			t.Errorf("Match(%q) = %v, want %v", c.path, got, c.want)
		}
	}
	// 仅排除、无 include：排除段外全量生效
	so := &Scope{Exclude: []string{"media"}}
	for _, c := range []struct {
		path string
		want bool
	}{
		{"a.md", true},
		{"assets/pic.png", true},
		{"media/x.mp4", false},
		{"media/sub/y.mp4", false},
	} {
		if got := so.Match(c.path, true); got != c.want {
			t.Errorf("Match(%q) = %v, want %v", c.path, got, c.want)
		}
	}

	// re: 正则模式（扩展名排除等场景）；锚定开头 + 忽略大小写
	sr := &Scope{Exclude: []string{`re:.*\.(mp4|mov)$`, `re:bigfiles/`}}
	for _, c := range []struct {
		path string
		want bool
	}{
		{"a.md", true},
		{"assets/video.MP4", false},
		{"assets/clip.mov", false},
		{"bigfiles/x.bin", false},
		{"sub/bigfiles/x.bin", true}, // 正则 ^bigfiles/ 不匹配子路径下的 bigfiles（与纯前缀段语义一致）
		{"assets/video.mp4x", true},  // $ 锚定
	} {
		if got := sr.Match(c.path, true); got != c.want {
			t.Errorf("Match(%q) = %v, want %v", c.path, got, c.want)
		}
	}
	// 非法正则：按字面前缀处理（不命中任何正常路径）
	sbad := &Scope{Include: []string{"re:["}, Exclude: nil}
	if sbad.Match("anything.md", true) {
		t.Fatalf("非法正则应按字面前缀处理 → 不命中")
	}
}

// 排除规则对账语义：排除段内的服务器条目不拉取；曾同步后被排除的条目原地保留（不判删除）
func TestReconcileExcludeScope(t *testing.T) {
	t.Run("排除段不拉取", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{},
			Server: []domain.ManifestItem{it("1", "a.md", "h1"), it("2", "media/x.mp4", "h2"), it("3", "media/sub/y.mp4", "h3")},
			Scope:  &Scope{Exclude: []string{"media"}},
		})
		if len(p.Ops) != 1 || p.Ops[0].Item.Path != "a.md" {
			t.Fatalf("排除段外应只拉取 a.md，got %+v", p.Ops)
		}
	})
	t.Run("曾同步后被排除 → 原地保留不删除", func(t *testing.T) {
		p := Reconcile(Input{
			Local:  []domain.ManifestItem{it("1", "a.md", "h1")}, // 客户端清单已不含 media/x.mp4
			Base:   []domain.ManifestItem{it("1", "a.md", "h1"), it("2", "media/x.mp4", "h2")},
			Server: []domain.ManifestItem{it("1", "a.md", "h1"), it("2", "media/x.mp4", "h3")},
			Scope:  &Scope{Exclude: []string{"media"}},
		})
		if len(p.Ops) != 0 || len(p.Expected) != 0 {
			t.Fatalf("被排除条目应完全不参与对账（不拉取、不上报删除），got ops=%+v expected=%+v", p.Ops, p.Expected)
		}
	})
}
