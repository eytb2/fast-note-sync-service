package api_router

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	svcmocks "github.com/haierkeys/fast-note-sync-service/internal/service/mocks"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/stretchr/testify/assert"
)

// newBaseURLTestHandler builds a ShareHandler whose storage root (derived from
// temp-path) points into a temp dir, so persistence tests don't touch the repo.
// newBaseURLTestHandler 构造一个 storage 根（由 temp-path 推出）指向临时目录的
// ShareHandler，落盘测试不碰仓库目录。
func newBaseURLTestHandler(t *testing.T, token string) *ShareHandler {
	t.Helper()
	h := newTestShareHandler(new(svcmocks.MockShareService), nil)
	dir := t.TempDir()
	h.App.Config().App.TempPath = filepath.Join(dir, "storage", "temp")
	h.App.Config().Server.ShareBaseUrlUpdateToken = token
	t.Cleanup(func() {
		shareBaseURLState.mu.Lock()
		shareBaseURLState.url = ""
		shareBaseURLState.mu.Unlock()
	})
	return h
}

func TestShareBaseUrl_TokenRequired(t *testing.T) {
	// 未配置令牌 → 接口禁用
	h := newBaseURLTestHandler(t, "")
	c, w := newShareTestContext("POST", "/api/admin/share-base-url?token=anything", "", 0)
	h.SetShareBaseUrl(c)
	assert.Equal(t, http.StatusOK, w.Code)
	assertResponseCode(t, w, code.ErrorUserIsNotAdmin.Code())

	// 令牌错误 → 拒绝
	h = newBaseURLTestHandler(t, "s3cret")
	c, w = newShareTestContext("POST", "/api/admin/share-base-url?token=wrong", "", 0)
	h.SetShareBaseUrl(c)
	assertResponseCode(t, w, code.ErrorUserIsNotAdmin.Code())
	assert.Equal(t, "", currentShareBaseURLOverride())
}

func TestShareBaseUrl_PushAndPersist(t *testing.T) {
	h := newBaseURLTestHandler(t, "s3cret")

	// 推送（query 参数形态，路由器脚本最顺手）
	c, w := newShareTestContext("POST", "/api/admin/share-base-url?url=http://223.85.176.17:10338&token=s3cret", "", 0)
	h.SetShareBaseUrl(c)
	assertResponseCode(t, w, code.Success.Code())
	assert.Equal(t, "http://223.85.176.17:10338", currentShareBaseURLOverride())

	// 已落盘，重启后 loadShareBaseURLOverride 能找回
	data, err := os.ReadFile(h.shareBaseURLFile())
	assert.NoError(t, err)
	assert.Equal(t, "http://223.85.176.17:10338", string(data))
	shareBaseURLState.mu.Lock()
	shareBaseURLState.url = ""
	shareBaseURLState.mu.Unlock()
	h.loadShareBaseURLOverride()
	assert.Equal(t, "http://223.85.176.17:10338", currentShareBaseURLOverride())

	// getShareBaseUrl 最高优先返回覆盖值
	c2, w2 := newShareTestContext("GET", "/api/share", "", 1)
	assert.Equal(t, "http://223.85.176.17:10338", h.getShareBaseUrl(c2))
	assert.Equal(t, http.StatusOK, w2.Code)
}

func TestShareBaseUrl_PushJSONBody(t *testing.T) {
	h := newBaseURLTestHandler(t, "s3cret")
	c, w := newShareTestContext("POST", "/api/admin/share-base-url", `{"url":"https://fns.example.com"}`, 0)
	c.Request.Header.Set("Authorization", "Bearer s3cret")
	h.SetShareBaseUrl(c)
	assertResponseCode(t, w, code.Success.Code())
	assert.Equal(t, "https://fns.example.com", currentShareBaseURLOverride())
}

func TestShareBaseUrl_InvalidURL(t *testing.T) {
	h := newBaseURLTestHandler(t, "s3cret")
	for _, bad := range []string{"ftp://x", "http://", "not-a-url", "http:///nohost"} {
		c, w := newShareTestContext("POST", "/api/admin/share-base-url?url="+bad+"&token=s3cret", "", 0)
		h.SetShareBaseUrl(c)
		assertResponseCode(t, w, code.ErrorInvalidParams.Code())
	}
	assert.Equal(t, "", currentShareBaseURLOverride())
}

func TestShareBaseUrl_Clear(t *testing.T) {
	h := newBaseURLTestHandler(t, "s3cret")
	c, _ := newShareTestContext("POST", "/api/admin/share-base-url?url=http://1.2.3.4:5&token=s3cret", "", 0)
	h.SetShareBaseUrl(c)
	assert.Equal(t, "http://1.2.3.4:5", currentShareBaseURLOverride())

	// 空 url 清除覆盖 + 删文件
	c, w := newShareTestContext("POST", "/api/admin/share-base-url?token=s3cret", "", 0)
	h.SetShareBaseUrl(c)
	assertResponseCode(t, w, code.Success.Code())
	assert.Equal(t, "", currentShareBaseURLOverride())
	_, err := os.Stat(h.shareBaseURLFile())
	assert.True(t, os.IsNotExist(err))
}

func TestShareBaseUrl_Status(t *testing.T) {
	h := newBaseURLTestHandler(t, "s3cret")
	c, _ := newShareTestContext("POST", "/api/admin/share-base-url?url=http://223.85.176.17:10338&token=s3cret", "", 0)
	h.SetShareBaseUrl(c)

	c, w := newShareTestContext("GET", "/api/admin/share-base-url?token=s3cret", "", 0)
	h.GetShareBaseUrlStatus(c)
	assertResponseCode(t, w, code.Success.Code())
	assert.Contains(t, w.Body.String(), "runtime-override")
	assert.Contains(t, w.Body.String(), "223.85.176.17")
}
