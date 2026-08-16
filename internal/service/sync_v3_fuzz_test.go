// Package service: P6 收敛性 fuzz——种子化随机操作矩阵（多客户端、随机掉线、
// CLI 式无名事件改名、大小写重命名、内容复用触发的同哈希推断）之后，
// 三端必须收敛到唯一一致状态；被跟踪的 id 谱系必须经哈希推断保住。
package service

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/reconcile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fuzzWorld 一次 fuzz 场景的世界状态
type fuzzWorld struct {
	clients []*simClient
	paths   []string // 活跃路径池（可能已不存在，操作时再校验）
	rng     *rand.Rand
	// lineage: 路径 → 初始提交后记录的条目 UUID（跟踪哈希推断是否保谱系）
	lineage map[string]string
	// lineageContent 记录谱系时的内容：终态内容一致才算“可断言谱系”（内容变了换 id 属正常）
	lineageContent map[string]string
	// lineageFrom 谱系目标 → 改名前路径（from/to 任一被后续操作触碰即污染）
	lineageFrom map[string]string
	// lineageDirty 被污染的谱系：改名后有他端写入/删除/挪动 from 或 to，
	// 哈希推断面对的是分叉历史，换 id 属合法协议行为，不可断言
	lineageDirty map[string]bool
	// srvLive 记录谱系时校验服务器现状（from 路径仍持有该 id 才无歧义）
	srvLive func(path, id string) bool
}

var fuzzContents = []string{
	"# a\nhello world", "binary-ish \x00\x01\x02 payload", "中文内容 加油",
	"line1\nline2\nline3", "same-content-token-A", "same-content-token-A",
	"same-content-token-A", "", "x",
}

func (w *fuzzWorld) randPath() string {
	if len(w.paths) == 0 || w.rng.Intn(4) == 0 {
		// 新路径：嵌套目录 + 随机大小写 + 随机扩展名
		ext := ".md"
		if w.rng.Intn(3) == 0 {
			ext = ".png"
		}
		p := fmt.Sprintf("dir%d/sub%d/file%d%s", w.rng.Intn(3), w.rng.Intn(3), w.rng.Intn(1000), ext)
		if w.rng.Intn(8) == 0 {
			p = upperFirst(p) // 大小写敏感路径空间的一部分
		}
		w.paths = append(w.paths, p)
		return p
	}
	return w.paths[w.rng.Intn(len(w.paths))]
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	c := s[0]
	switch {
	case c >= 'a' && c <= 'z':
		return string(c-32) + s[1:]
	case c >= 'A' && c <= 'Z':
		return string(c+32) + s[1:]
	}
	return s // 非字母不翻转：避免链式 ±32 产生非法 UTF-8 路径字节
}

// mutate 对单个客户端施加一次随机操作
func (w *fuzzWorld) mutate(t *testing.T, c *simClient) {
	switch n := w.rng.Intn(12); {
	case n <= 4: // 写（新文件或覆盖）
		p := w.randPath()
		w.touch(p)
		c.write(p, fuzzContents[w.rng.Intn(len(fuzzContents))])
	case n == 5: // 删除
		if len(c.files) > 0 {
			p := w.pickExisting(c)
			w.touch(p)
			c.remove(p)
		}
	case n == 6: // GUI 式改名（id 随行）
		if len(c.files) > 0 {
			from := w.pickExisting(c)
			to := w.randPath()
			if from != to {
				w.touch(from, to)
				c.rename(from, to)
			}
		}
	case n == 7: // CLI 式改名：无名事件，退化为 删+增（服务器需靠同哈希推断 move）
		if len(c.files) > 0 {
			from := w.pickExisting(c)
			to := w.randPath()
			if from != to {
				content := c.files[from]
				id := c.ids[from]
				w.touch(from, to)
				c.write(to, content)
				c.remove(from)
				// 记录谱系：收敛后新路径应保住同一 UUID（内容未分叉时才可断言）
				w.recordLineage(c, to, from, id, content)
			}
		}
	case n == 8: // 大小写翻转重命名（note.md → Note.md）
		if len(c.files) > 0 {
			from := w.pickExisting(c)
			flipped := upperFirst(from)
			if from != flipped && c.files[flipped] == "" {
				content := c.files[from]
				id := c.ids[from]
				w.touch(from, flipped)
				c.write(flipped, content)
				c.remove(from)
				w.recordLineage(c, flipped, from, id, content)
			}
		}
	case n == 9, n == 10: // 跨客户端内容复制（制造同哈希多路径，考验 move 唯一性）
		src := w.clients[w.rng.Intn(len(w.clients))]
		if len(src.files) > 0 {
			from := w.pickExisting(src)
			to := w.randPath()
			w.touch(to)
			c.write(to, src.files[from])
		}
	default: // no-op
	}
}

func (w *fuzzWorld) pickExisting(c *simClient) string {
	keys := make([]string, 0, len(c.files))
	for k := range c.files {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 确定性：map 迭代序随机会破坏种子可复现性
	return keys[w.rng.Intn(len(keys))]
}

// recordLineage 记录一条可断言谱系：仅在“改名发起时本地内容与上次同步一致”时记录
// （客户端持有的是服务器最新内容，推断才有无歧义的哈希可配对）
func (w *fuzzWorld) recordLineage(c *simClient, to, from, id, content string) {
	if id == "" || c.syncedContent[from] != content {
		return // 内容已本地分叉/从未确认：推断本就无配对义务
	}
	if w.srvLive != nil && !w.srvLive(from, id) {
		return // 服务器侧 from 已不持有该 id（他端已删/已挪）：推断无配对候选
	}
	w.lineage[to] = id
	w.lineageContent[to] = content
	w.lineageFrom[to] = from
	delete(w.lineageDirty, to)
}

// touch 任一客户端对路径的写入/删除/挪动都会污染涉及该路径的谱系
func (w *fuzzWorld) touch(paths ...string) {
	for _, p := range paths {
		for to := range w.lineage {
			if p == to || p == w.lineageFrom[to] {
				w.lineageDirty[to] = true
			}
		}
	}
}

// observeCommit 提交级污染追踪：他端在记录之前积累的删除/移动可能在记录之后才落库，
// mutation 级 touch 覆盖不到这个窗口，须在每次提交回看变更集。
// 谱系自身的改名落地（id+from+to 全匹配）不污染自己。
func (w *fuzzWorld) observeCommit(_ string, changes []reconcile.Change) {
	for _, ch := range changes {
		switch ch.Op {
		case "delete":
			w.touch(ch.Item.Path)
		case "move":
			for _, p := range []string{ch.OldPath, ch.Item.Path} {
				for to := range w.lineage {
					if to == ch.Item.Path && w.lineageFrom[to] == ch.OldPath && w.lineage[to] == ch.Item.ID {
						continue // 本谱系的改名本身
					}
					if p == to || p == w.lineageFrom[to] {
						w.lineageDirty[to] = true
					}
				}
			}
		}
	}
}

// TestSyncV3_FuzzConvergence 种子化随机矩阵 → 三端收敛 + 谱系保持
func TestSyncV3_FuzzConvergence(t *testing.T) {
	seeds := []int64{11, 42, 2026, 777, 90210, 314159}
	if v := os.Getenv("FNS_FUZZ_SEEDS"); v != "" { // 种子扫描：FNS_FUZZ_SEEDS="1-300" 或 "7,13"
		seeds = parseSeedRange(v)
	}
	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			svc, d, fsRepo, manifestRepo, cleanup := setupV3TestEnv(t)
			defer cleanup()
			ctx := context.Background()

			w := &fuzzWorld{
				clients: []*simClient{newSimClient("A", svc, d), newSimClient("B", svc, d), newSimClient("C", svc, d)},
				rng:     rand.New(rand.NewSource(seed)),
				lineage: map[string]string{}, lineageContent: map[string]string{},
				lineageFrom: map[string]string{}, lineageDirty: map[string]bool{},
			}
			w.srvLive = func(path, id string) bool {
				cur, err := manifestRepo.Current(ctx, 1, simUID)
				if err != nil || cur == nil {
					return false
				}
				for _, it := range cur.Items {
					if it.Path == path {
						return it.ID == id
					}
				}
				return false
			}
			commitObserver = w.observeCommit
			defer func() { commitObserver = nil }()

			// 行级转储：收敛失败时打印争议路径的原始 fs_entry 行（含墓碑），
			// 用于定位“ack 成功但服务器状态不动”这类清单视图无法解释的活锁
			convergeDebugRows = func(t *testing.T, ctx context.Context, hot map[string]bool) {
				for p := range hot {
					live, err := fsRepo.ListLive(ctx, 1, simUID)
					if err != nil {
						t.Logf("[行级] %s ListLive 失败: %v", p, err)
						continue
					}
					for _, e := range live {
						if e.Path == p {
							t.Logf("[行级] path=%s LIVE id=%s hash=%s size=%d mtime=%d",
								p, e.ID, shortHash(e.BlobHash), e.Size, e.Mtime)
						}
					}
					del, _ := fsRepo.ListDeleted(ctx, 1, simUID)
					for _, e := range del {
						if e.Path == p {
							t.Logf("[行级] path=%s TOMB id=%s hash=%s deleted_at=%d",
								p, e.ID, shortHash(e.BlobHash), e.DeletedAt)
						}
					}
					// 各端申报 id 对应的行（无论在哪个路径）：揭示 id 被挪去/落在哪
					for _, c := range w.clients {
						id := c.ids[p]
						if id == "" {
							continue
						}
						if e, err := fsRepo.GetByID(ctx, id, 1); err == nil && e != nil {
							t.Logf("[行级] %s@%s 申报 id=%s → 实际行 path=%s deleted=%t hash=%s",
								c.name, p, id, e.Path, e.Deleted, shortHash(e.BlobHash))
						} else {
							t.Logf("[行级] %s@%s 申报 id=%s → 无对应行", c.name, p, id)
						}
					}
				}
			}
			defer func() { convergeDebugRows = nil }()

			// 种子文件：A 建初始集并首同步（确立 id 谱系基线），B/C 全量拉平
			for i := 0; i < 6; i++ {
				w.clients[0].write(fmt.Sprintf("seed/dir%d/note%d.md", i%2, i), fmt.Sprintf("seed content %d", i))
			}
			w.clients[0].syncRound(t, ctx)
			converge(t, ctx, svc, manifestRepo, w.clients...)
			for p, id := range w.clients[0].ids {
				// 初始谱系：内容已与服务器一致（刚 converge），from 即自身路径
				w.recordLineage(w.clients[0], p, p, id, w.clients[0].files[p])
			}

			// 随机轮次：每轮 1-3 个客户端各施 1-2 次操作，然后随机子集“在线”同步（其余掉线）
			rounds := 15 + w.rng.Intn(15)
			for r := 0; r < rounds; r++ {
				for _, c := range w.clients {
					for k := 0; k < 1+w.rng.Intn(2); k++ {
						w.mutate(t, c)
					}
				}
				for _, c := range w.clients {
					if w.rng.Intn(4) > 0 { // 75% 在线
						c.syncRound(t, ctx)
					}
				}
			}

			// 全员上线 → 必须收敛（诊断上限放宽：观察真实收敛深度分布）
			final := convergeN(t, ctx, svc, manifestRepo, 60, w.clients...)

			// 谱系断言：跟踪路径若仍存活，其 UUID 必须等于初始谱系（CLI 改名靠哈希推断保 id）。
			// 注：另一端先提交了同路径同哈希内容时，本端无需提交也拿不到 ack —— id 只需
			// 由任一客户端持有（提交方必有；其余端经 OpPull 获得），本地缺失属协议正常降级
			// （真客户端退化为哈希推断，见 TestSyncV3_CLIRenameHashInference）。
			// 内容哈希在终态多路径重复时跳过：同哈希多候选下推断的 move 唯一性
			// 选择可能合法地换 id（离线并发改名 + 重复内容是协议层歧义，非缺陷）。
			hashCount := map[string]int{}
			for _, content := range final {
				hashCount[content]++
			}
			survived, eligible, moved, excluded := 0, 0, 0, 0
			for p, id := range w.lineage {
				if id == "" {
					continue
				}
				if _, alive := final[p]; !alive {
					continue // 路径已消失（后续操作又删/改名）：无法断言
				}
				survived++
				var gotID string
				for _, c := range w.clients {
					if v := c.ids[p]; v != "" {
						gotID = v
						break
					}
				}
				require.NotEmpty(t, gotID, "收敛后 %s 在所有客户端都缺少服务器 id", p)
				if final[p] != w.lineageContent[p] {
					continue // 内容已被他端改写：换新 id 属正常（条目身份确实变了）
				}
				if w.lineageDirty[p] || hashCount[final[p]] > 1 {
					excluded++ // 改名后 from/to 被触碰或同内容多路径：推断歧义，仅观察
					continue
				}
				eligible++
				if gotID == id {
					moved++
				} else {
					t.Logf("[谱系断裂] seed=%d path=%s from=%s content=%q 期望 id=%s 实际=%s",
						seed, p, w.lineageFrom[p], final[p], id, gotID)
				}
			}
			t.Logf("seed=%d: rounds=%d files=%d lineage-survived=%d eligible=%d excluded=%d id-preserved=%d",
				seed, rounds, len(final), survived, eligible, excluded, moved)
			// 无歧义谱系（内容未变、from/to 未被触碰、内容全局唯一）：
			// id 必须逐一保持——哈希推断在这些前提下没有换 id 的正当理由
			assert.Equal(t, eligible, moved, "存在无歧义的存活谱系但 id 未被保住")
		})
	}
}

// parseSeedRange 解析 "1-300" 或 "7,13,42" 形式的种子列表
func parseSeedRange(v string) []int64 {
	var out []int64
	for _, part := range strings.Split(v, ",") {
		if i := strings.IndexByte(part, '-'); i > 0 {
			lo, err1 := strconv.ParseInt(part[:i], 10, 64)
			hi, err2 := strconv.ParseInt(part[i+1:], 10, 64)
			if err1 == nil && err2 == nil && hi >= lo && hi-lo < 100000 {
				for s := lo; s <= hi; s++ {
					out = append(out, s)
				}
			}
			continue
		}
		if n, err := strconv.ParseInt(part, 10, 64); err == nil {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return []int64{11}
	}
	return out
}
