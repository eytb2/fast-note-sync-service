// Package dao: blob_store 属于 git 式快照同步（WS v3）的新数据层。
// 内容寻址存储：storage/vault/u_{uid}/blob/{sha256前2位}/{sha256}
package dao

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/haierkeys/fast-note-sync-service/pkg/util"
)

// BlobTempDir gets the staging directory for chunked uploads (already used by the old protocol, reused here)
// BlobTempDir 获取分块上传的暂存目录（旧协议已有，沿用）
func (d *Dao) BlobTempDir() string {
	return filepath.Join("storage", "temp")
}

// BlobPath gets the storage path of a content-addressed blob (it may not exist)
// BlobPath 获取内容寻址 blob 的存储路径（不一定存在）
func (d *Dao) BlobPath(uid int64, hash string) string {
	if len(hash) < 2 {
		return ""
	}
	return filepath.Join("storage", "vault", fmt.Sprintf("u_%d", uid), "blob", hash[:2], hash)
}

// BlobExists checks whether a blob already exists (for instant-upload detection)
// BlobExists 检查 blob 是否已存在（用于秒传判断）
func (d *Dao) BlobExists(uid int64, hash string) bool {
	p := d.BlobPath(uid, hash)
	if p == "" {
		return false
	}
	_, err := os.Stat(p)
	return err == nil
}

// BlobStoreFromBytes writes bytes into the blob store, returning the SHA-256.
// Idempotent: if content with the same hash already exists, it is overwritten (same content) and no error is returned.
// BlobStoreFromBytes 将字节写入 blob store，返回 SHA-256。
// 幂等：同哈希内容已存在时等同覆盖（内容相同），不报错。
func (d *Dao) BlobStoreFromBytes(uid int64, data []byte) (string, error) {
	hash := util.SHA256Bytes(data)
	dst := d.BlobPath(uid, hash)
	if dst == "" {
		return "", fmt.Errorf("invalid blob hash")
	}
	if d.BlobExists(uid, hash) {
		return hash, nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return "", err
	}
	// Write to a temporary file then rename, to avoid readers seeing partially-written content
	// 先写临时文件再改名，避免读方看到半写状态
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return hash, nil
}

// BlobStoreFromTemp moves a staging-area file into the blob store after verifying its SHA-256.
// expectedHash is empty or mismatched means an error; matching means the move is completed (or a blob with the same hash already exists).
// BlobStoreFromTemp 校验暂存文件 SHA-256 后移入 blob store。
// expectedHash 为空或不匹配即报错；匹配则完成移动（或同哈希 blob 已存在）。
func (d *Dao) BlobStoreFromTemp(uid int64, tempPath, expectedHash string) error {
	f, err := os.Open(tempPath)
	if err != nil {
		return err
	}
	hash, err := util.SHA256Reader(f)
	_ = f.Close()
	if err != nil {
		return err
	}
	if expectedHash != "" && hash != expectedHash {
		return fmt.Errorf("blob hash mismatch: expected %s got %s", expectedHash, hash)
	}
	dst := d.BlobPath(uid, hash)
	if d.BlobExists(uid, hash) {
		return os.Remove(tempPath)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	return os.Rename(tempPath, dst)
}

// BlobStoreFromReader streams content into the blob store (for large attachments), returning the SHA-256.
// The data source is not modified (during flat migration the old file is kept as-is).
// BlobStoreFromReader 以流式把内容写入 blob store（用于大附件），返回 SHA-256。
// 不改动数据源（平迁时旧文件原样保留）。
func (d *Dao) BlobStoreFromReader(uid int64, r io.Reader) (string, error) {
	tmp, err := os.CreateTemp(d.BlobTempDir(), "blob-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	hash, err := util.SHA256Reader(io.TeeReader(r, tmp))
	_ = tmp.Close()
	if err != nil {
		return "", err
	}
	if d.BlobExists(uid, hash) {
		return hash, nil
	}
	dst := d.BlobPath(uid, hash)
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return "", err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return "", err
	}
	return hash, nil
}

// BlobOpen opens a blob for reading; the caller is responsible for closing it.
// BlobOpen 打开 blob 供读取，调用方负责关闭。
func (d *Dao) BlobOpen(uid int64, hash string) (io.ReadCloser, error) {
	p := d.BlobPath(uid, hash)
	if p == "" || !d.BlobExists(uid, hash) {
		return nil, os.ErrNotExist
	}
	return os.Open(p)
}

// BlobReadAll reads the entire blob (for inlining note text).
// BlobReadAll 读取整个 blob（用于笔记文本内联）。
func (d *Dao) BlobReadAll(uid int64, hash string) ([]byte, error) {
	p := d.BlobPath(uid, hash)
	if p == "" {
		return nil, os.ErrNotExist
	}
	return os.ReadFile(p)
}

// BlobSize gets the blob size (in bytes); the second return value indicates existence.
// BlobSize 获取 blob 大小（字节）；第二个返回值表示是否存在。
func (d *Dao) BlobSize(uid int64, hash string) (int64, bool) {
	p := d.BlobPath(uid, hash)
	if p == "" {
		return 0, false
	}
	st, err := os.Stat(p)
	if err != nil {
		return 0, false
	}
	return st.Size(), true
}
