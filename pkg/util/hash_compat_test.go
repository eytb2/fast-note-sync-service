// Package util: P7 旧协议哈希算法与插件 master（src/lib/utils/helpers.ts）的一致性测试。
// 参照值由 node 脚本逐行复刻插件算法计算得出（2026-08）：
//   - 文本：hashContentAsync —— UTF-16 code unit 滚动（charCodeAt）
//   - 二进制：hashArrayBuffer —— 字节滚动；>10MB 头 5MB + 中 5MB + 尾 5MB 拼接
//
// 字节用例的确定性填充 b[i] = (i*31+7) % 251：周期 251 与 5MB 互质，保证不同
// 采样窗口/不同尺寸产生不同字节序列（周期 256 填充下所有 5MB 窗口字节相同，区分度不足）。
package util

import (
	"bytes"
	"testing"
)

func TestEncodeHash32UTF16MatchesJS(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"empty", "", "0"},
		{"ascii", "hello", "99162322"},
		{"cjk", "你好，世界！", "-723777316"},
		{"astral-surrogate-pair", "a\U0001F600b", "57849694"},
		{"mixed-controls", "line1\nline2\ttab中文", "1477509655"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := EncodeHash32(c.in); got != c.want {
				t.Errorf("EncodeHash32(%q) = %s, want %s (JS hashContentAsync)", c.in, got, c.want)
			}
			// 流式变体（UTF-8 → UTF-16 解码）必须与整串结果一致
			if got, err := EncodeHash32UTF16Stream(bytes.NewReader([]byte(c.in))); err != nil || got != c.want {
				t.Errorf("EncodeHash32UTF16Stream(%q) = %s (err=%v), want %s", c.in, got, err, c.want)
			}
		})
	}
}

func TestEncodeHash32FileJSMatchesJS(t *testing.T) {
	if got := EncodeHash32FileJS([]byte("hello world")); got != "1794106052" {
		t.Errorf("small = %s, want 1794106052", got)
	}
	small := detBytesCompat(1000)
	if got := EncodeHash32FileJS(small); got != "1236401538" {
		t.Errorf("1000-pattern = %s, want 1236401538", got)
	}
	exact := detBytesCompat(FileHashThreshold) // 恰好 10MB（阈值含）
	if got := EncodeHash32FileJS(exact); got != "807627400" {
		t.Errorf("10MB-exact = %s, want 807627400", got)
	}
}

func TestEncodeHash32FileJSStreamMatchesJS(t *testing.T) {
	// >10MB 采样路径：流式（seek）与整块（拼 15MB view）必须一致，且都等于 JS 参照值
	big1 := detBytesCompat(FileHashThreshold + 12345)
	if got := EncodeHash32FileJS(big1); got != "1575681767" {
		t.Errorf("big-sampled = %s, want 1575681767", got)
	}
	if got, err := EncodeHash32FileJSStream(bytes.NewReader(big1), int64(len(big1))); err != nil || got != "1575681767" {
		t.Errorf("big-sampled stream = %s (err=%v), want 1575681767", got, err)
	}

	big2 := detBytesCompat(FileHashThreshold + 12345 + FileHashSliceSize)
	if got := EncodeHash32FileJS(big2); got != "-1077647961" {
		t.Errorf("big2-sampled = %s, want -1077647961", got)
	}
	if got, err := EncodeHash32FileJSStream(bytes.NewReader(big2), int64(len(big2))); err != nil || got != "-1077647961" {
		t.Errorf("big2-sampled stream = %s (err=%v), want -1077647961", got, err)
	}

	// 阈值以下：流式与整块同样一致
	small := detBytesCompat(1000)
	if got, err := EncodeHash32FileJSStream(bytes.NewReader(small), int64(len(small))); err != nil || got != "1236401538" {
		t.Errorf("small stream = %s (err=%v), want 1236401538", got, err)
	}
}

// detBytesCompat b[i] = (i*31+7) % 251（与 JS 参照脚本一致）
func detBytesCompat(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*31 + 7) % 251)
	}
	return b
}
