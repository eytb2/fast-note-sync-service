package util

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strconv"
	"unicode/utf16"
)

// EncodeMD5 performs MD5 encoding on a string
// EncodeMD5 对字符串进行MD5编码
// str: string to be encoded
// str: 待编码的字符串
// return: MD5 encoded 32-bit hexadecimal string
// 返回值: MD5编码后的32位十六进制字符串
func EncodeMD5(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}

// EncodeHash32 performs 32-bit hash encoding on a string
// EncodeHash32 对字符串进行 32 位哈希编码
func EncodeHash32(content string) string {
	// Convert string to rune slice, then to UTF-16 code units (consistent with JS internal representation)
	// 首先将字符串转为 rune 切片，再转为 UTF-16 code units（与 JS 的内部表示一致）
	runes := []rune(content)
	utf16Units := utf16.Encode(runes) // []uint16
	var hash int32 = 0
	for _, u := range utf16Units {
		char := int32(u) // Consistent with 16-bit value returned by JS charCodeAt // 与 JS charCodeAt 返回的 16-bit 值一致
		hash = (hash << 5) - hash + char
		// int32 will automatically overflow, equivalent to JS 32-bit bitwise operation result
		// int32 会自动溢出，等价于 JS 的 32-bit 位运算结果
	}
	return strconv.Itoa(int(hash))
}

// EncodeHash32UTF16Stream computes EncodeHash32 over a UTF-8 byte stream without
// buffering the whole content: bytes are incrementally decoded to code points
// and re-encoded as UTF-16 code units (surrogate pairs included), exactly the
// sequence JS charCodeAt would walk.
// EncodeHash32UTF16Stream 流式计算 EncodeHash32：增量解码 UTF-8 → UTF-16 code unit
// （含代理对），与 JS charCodeAt 遍历序列一致，无需整读内容。
func EncodeHash32UTF16Stream(r io.Reader) (string, error) {
	var hash int32 = 0
	feedOne := func(u uint16) {
		hash = (hash << 5) - hash + int32(u)
	}

	var cp rune // 累积中的码点
	var cont int
	buf := make([]byte, 64*1024)
	for {
		n, err := r.Read(buf)
		for i := 0; i < n; i++ {
			b := buf[i]
			if cont > 0 {
				if b&0xC0 == 0x80 {
					cp = cp<<6 | rune(b&0x3F)
					cont--
					if cont == 0 {
						if cp >= 0x10000 {
							for _, u := range utf16.Encode([]rune{cp}) {
								feedOne(u)
							}
						} else {
							feedOne(uint16(cp))
						}
					}
					continue
				}
				// 非法续字节：按 JS 解码语义落 U+FFFD，当前字节重新按首字节处理
				cont = 0
				feedOne(0xFFFD)
			}
			switch {
			case b < 0x80:
				feedOne(uint16(b))
			case b&0xE0 == 0xC0:
				cp, cont = rune(b&0x1F), 1
			case b&0xF0 == 0xE0:
				cp, cont = rune(b&0x0F), 2
			case b&0xF8 == 0xF0:
				cp, cont = rune(b&0x07), 3
			default:
				feedOne(0xFFFD)
			}
		}
		if err != nil {
			if err == io.EOF {
				if cont > 0 {
					feedOne(0xFFFD)
				}
				return strconv.Itoa(int(hash)), nil
			}
			return "", err
		}
	}
}

const (
	// FileHashThreshold defines the size threshold (10MB) above which partial hashing is used
	// FileHashThreshold 定义触发分段哈希的阈值 (10MB)
	FileHashThreshold = 10 * 1024 * 1024
	// FileHashSliceSize defines the size of slices taken from the beginning and end of large files (5MB)
	// FileHashSliceSize 定义大文件分段哈希时首尾读取的大小 (5MB)
	FileHashSliceSize = 5 * 1024 * 1024
)

// EncodeHash32Bytes performs 32-bit hash encoding on raw bytes.
// If the data exceeds 10MB, it only hashes the first 5MB and last 5MB.
// EncodeHash32Bytes 对原始字节进行 32 位哈希编码。
// 如果数据超过 10MB，则仅计算前 5MB 和后 5MB 的哈希。
//
// Deprecated: this head+tail sampling does NOT match the current Obsidian plugin
// (which samples head 5MB + middle 5MB + tail 5MB). Kept only for legacy-table
// backfill of hashes stored by old server versions; new code must use
// EncodeHash32FileJS.
func EncodeHash32Bytes(data []byte) string {
	size := len(data)
	var hash int32 = 0

	if size <= FileHashThreshold {
		// Small data: full hash // 小数据：全量哈希
		for _, b := range data {
			hash = (hash << 5) - hash + int32(b)
		}
	} else {
		// Large data: hash first 5MB + last 5MB // 大数据：哈希前 5MB + 后 5MB
		// Hash first 5MB
		for i := 0; i < FileHashSliceSize; i++ {
			hash = (hash << 5) - hash + int32(data[i])
		}
		// Hash last 5MB
		for i := size - FileHashSliceSize; i < size; i++ {
			hash = (hash << 5) - hash + int32(data[i])
		}
	}
	return strconv.Itoa(int(hash))
}

// EncodeHash32FileJS replicates the plugin's hashArrayBuffer/hashFileAsync
// byte rolling hash exactly: full bytes when size <= 10MB; otherwise the
// concatenation of head 5MB + middle 5MB + tail 5MB, where the middle slice is
// centered on the file midpoint and clamped to the valid range:
//
//	midStart = min(max(0, floor(size/2) - 2621440), max(0, size - 5MB))
//
// EncodeHash32FileJS 逐字节复刻插件 hashArrayBuffer 的滚动 32 位哈希：
// ≤10MB 全量；>10MB 取头 5MB + 中 5MB（以文件中点为中心并夹紧）+ 尾 5MB 拼接后计算。
func EncodeHash32FileJS(data []byte) string {
	size := len(data)
	var hash int32 = 0

	if size <= FileHashThreshold {
		for _, b := range data {
			hash = (hash << 5) - hash + int32(b)
		}
		return strconv.Itoa(int(hash))
	}

	// Large data: head 5MB + middle 5MB + tail 5MB (plugin computeMidSliceStart)
	// 大数据：头 5MB + 中 5MB（computeMidSliceStart）+ 尾 5MB
	midStart := size/2 - FileHashSliceSize/2
	if midStart < 0 {
		midStart = 0
	}
	if maxStart := size - FileHashSliceSize; maxStart >= 0 && midStart > maxStart {
		midStart = maxStart
	}
	midLen := size - midStart
	if midLen > FileHashSliceSize {
		midLen = FileHashSliceSize
	}

	for i := 0; i < FileHashSliceSize; i++ {
		hash = (hash << 5) - hash + int32(data[i])
	}
	for i := 0; i < midLen; i++ {
		hash = (hash << 5) - hash + int32(data[midStart+i])
	}
	for i := size - FileHashSliceSize; i < size; i++ {
		hash = (hash << 5) - hash + int32(data[i])
	}
	return strconv.Itoa(int(hash))
}

// EncodeHash32FileJSStream is the streaming variant of EncodeHash32FileJS for
// large blobs: it hashes head 5MB, then seeks to the middle slice, then the
// tail. For size <= threshold it hashes the whole stream.
// EncodeHash32FileJSStream 是 EncodeHash32FileJS 的流式版本，供大 blob 免整读。
func EncodeHash32FileJSStream(r io.ReadSeeker, size int64) (string, error) {
	if size <= int64(FileHashThreshold) {
		var hash int32 = 0
		buf := make([]byte, 64*1024)
		for {
			n, err := r.Read(buf)
			for i := 0; i < n; i++ {
				hash = (hash << 5) - hash + int32(buf[i])
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				return "", err
			}
		}
		return strconv.Itoa(int(hash)), nil
	}

	slice := int64(FileHashSliceSize)
	midStart := size/2 - slice/2
	if midStart < 0 {
		midStart = 0
	}
	if maxStart := size - slice; maxStart >= 0 && midStart > maxStart {
		midStart = maxStart
	}
	midLen := size - midStart
	if midLen > slice {
		midLen = slice
	}

	var hash int32 = 0
	buf := make([]byte, 64*1024)
	update := func(length int64) error {
		remaining := length
		for remaining > 0 {
			want := int64(len(buf))
			if remaining < want {
				want = remaining
			}
			n, err := io.ReadFull(r, buf[:want])
			for i := 0; i < n; i++ {
				hash = (hash << 5) - hash + int32(buf[i])
			}
			remaining -= int64(n)
			if err != nil {
				return err
			}
		}
		return nil
	}

	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	if err := update(slice); err != nil {
		return "", err
	}
	if _, err := r.Seek(midStart, io.SeekStart); err != nil {
		return "", err
	}
	if err := update(midLen); err != nil {
		return "", err
	}
	if _, err := r.Seek(size-slice, io.SeekStart); err != nil {
		return "", err
	}
	if err := update(slice); err != nil {
		return "", err
	}
	return strconv.Itoa(int(hash)), nil
}

// SHA256Bytes computes the full SHA-256 hex digest of raw bytes.
// SHA256Bytes 计算原始字节的完整 SHA-256 十六进制摘要。
func SHA256Bytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// SHA256Reader computes the full SHA-256 hex digest of a stream (for large-file streaming).
// SHA256Reader 以流式计算 SHA-256 十六进制摘要（用于大文件）。
func SHA256Reader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
