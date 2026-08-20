package api_router

import (
	"crypto/subtle"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"go.uber.org/zap"
)

// Runtime share-base-url override, pushed by router NAT-map notify scripts when
// the public ip:port changes (CGNAT rebinds on every router reboot, so any static
// config goes stale). Persisted to <storage>/share_base_url — inside the docker
// volume, survives restarts. Takes priority over yaml share-base-url / ext-api-url.
//
// 运行时分享基址覆盖：路由器 natmap notify 在公网 ip:port 变化时推送
// （CGNAT 每次路由器重启都会换地址，静态配置必然过期）。落盘于
// <storage>/share_base_url（docker volume 内，重启不丢），优先级高于
// yaml 的 share-base-url / ext-api-url。
var shareBaseURLState = struct {
	mu  sync.RWMutex
	url string
}{}

// shareBaseURLFile returns the persistence path: storage root derived from
// temp-path's parent ("storage/temp" → "storage"), falling back to "storage".
// shareBaseURLFile 返回落盘路径：storage 根由 temp-path 的父目录推出
// （"storage/temp" → "storage"），推不出时兜底 "storage"。
func (h *ShareHandler) shareBaseURLFile() string {
	tmp := strings.TrimSpace(h.App.Config().App.TempPath)
	root := "storage"
	if tmp != "" {
		root = filepath.Dir(filepath.Clean(tmp))
	}
	return filepath.Join(root, "share_base_url")
}

// loadShareBaseURLOverride reads the persisted override at startup; missing
// file or read errors simply leave the override empty (fall through to config).
// loadShareBaseURLOverride 启动时读取落盘覆盖值；文件缺失或读取失败仅保持为空
// （回落到静态配置）。
func (h *ShareHandler) loadShareBaseURLOverride() {
	data, err := os.ReadFile(h.shareBaseURLFile())
	if err != nil {
		return
	}
	u := strings.TrimSpace(string(data))
	if u == "" || !validShareBaseURL(u) {
		return
	}
	shareBaseURLState.mu.Lock()
	shareBaseURLState.url = u
	shareBaseURLState.mu.Unlock()
}

// currentShareBaseURLOverride returns the runtime override ("" when unset).
// currentShareBaseURLOverride 返回运行时覆盖值（未设置为空串）。
func currentShareBaseURLOverride() string {
	shareBaseURLState.mu.RLock()
	defer shareBaseURLState.mu.RUnlock()
	return shareBaseURLState.url
}

// validShareBaseURL accepts only absolute http(s) URLs with a host.
// validShareBaseURL 仅接受带 host 的绝对 http(s) 地址。
func validShareBaseURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

// shareBaseURLTokenOK validates the static push token. No token configured
// means the endpoint is disabled; comparison is constant-time. Accepts either
// an Authorization: Bearer header or a ?token= query param (shell-friendly).
// shareBaseURLTokenOK 校验静态推送令牌。未配置令牌即接口禁用；比较为常数时间。
// 支持 Authorization: Bearer 头或 ?token= 查询参数（方便 shell curl）。
func (h *ShareHandler) shareBaseURLTokenOK(c *gin.Context) bool {
	want := h.App.Config().Server.ShareBaseUrlUpdateToken
	if want == "" {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "))
	if got == "" {
		got = c.Query("token")
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// SetShareBaseUrl pushes a new runtime share base URL
// @Summary Push share base URL (static token)
// @Description Push the current public base URL, e.g. from a router NAT-map notify script; empty url clears the override. Requires share-base-url-update-token.
// @Tags Share
// @Accept json
// @Produce json
// @Param params body dto.ShareBaseUrlPushRequest true "Base URL"
// @Success 200 {object} pkgapp.Res "Success"
// @Failure 403 {object} pkgapp.Res "Token invalid or not configured"
// @Router /api/admin/share-base-url [post]
func (h *ShareHandler) SetShareBaseUrl(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	logger := h.App.Logger()

	if !h.shareBaseURLTokenOK(c) {
		logger.Warn("apiRouter.Share.SetShareBaseUrl token rejected",
			zap.String("ip", c.ClientIP()))
		response.ToResponse(code.ErrorUserIsNotAdmin.WithDetails("share-base-url token invalid or not configured"))
		return
	}

	raw := strings.TrimSpace(c.Query("url"))
	if raw == "" {
		params := &dto.ShareBaseUrlPushRequest{}
		if err := c.ShouldBindJSON(params); err == nil {
			raw = strings.TrimSpace(params.URL)
		}
	}

	// Empty push clears the override (back to config/动态推断).
	// 空值清除覆盖（回落静态配置/动态推断）。
	if raw == "" {
		shareBaseURLState.mu.Lock()
		shareBaseURLState.url = ""
		shareBaseURLState.mu.Unlock()
		_ = os.Remove(h.shareBaseURLFile())
		response.ToResponse(code.Success.WithData(gin.H{"url": "", "cleared": true}))
		return
	}

	if !validShareBaseURL(raw) {
		response.ToResponse(code.ErrorInvalidParams.WithDetails("url must be an absolute http(s) URL with host"))
		return
	}

	// 全新 volume 下 storage 根可能尚不存在，先建父目录
	if err := os.MkdirAll(filepath.Dir(h.shareBaseURLFile()), 0o755); err != nil {
		logger.Error("apiRouter.Share.SetShareBaseUrl mkdir failed", zap.Error(err))
		response.ToResponse(code.ErrorInvalidParams.WithDetails("persist failed"))
		return
	}
	if err := os.WriteFile(h.shareBaseURLFile(), []byte(raw), 0o600); err != nil {
		logger.Error("apiRouter.Share.SetShareBaseUrl persist failed", zap.Error(err))
		response.ToResponse(code.ErrorInvalidParams.WithDetails("persist failed"))
		return
	}

	shareBaseURLState.mu.Lock()
	shareBaseURLState.url = raw
	shareBaseURLState.mu.Unlock()

	logger.Info("apiRouter.Share.SetShareBaseUrl updated", zap.String("url", raw))
	response.ToResponse(code.Success.WithData(gin.H{"url": raw}))
}

// GetShareBaseUrlStatus reports the effective share base URL and its source
// @Summary Get effective share base URL (static token)
// @Description Returns the runtime override (if any) and the effective base URL used for new shares. Requires share-base-url-update-token.
// @Tags Share
// @Produce json
// @Success 200 {object} pkgapp.Res "Success"
// @Router /api/admin/share-base-url [get]
func (h *ShareHandler) GetShareBaseUrlStatus(c *gin.Context) {
	response := pkgapp.NewResponse(c)

	if !h.shareBaseURLTokenOK(c) {
		response.ToResponse(code.ErrorUserIsNotAdmin.WithDetails("share-base-url token invalid or not configured"))
		return
	}

	override := currentShareBaseURLOverride()
	cfg := h.App.Config().Server
	source := "request-host"
	switch {
	case override != "":
		source = "runtime-override"
	case cfg.WebGuiPort != "" && cfg.SharePort != "" && cfg.ShareBaseUrl != "":
		source = "share-base-url"
	case cfg.ExtApiUrl != "":
		source = "ext-api-url"
	}
	response.ToResponse(code.Success.WithData(gin.H{
		"override": override,
		"effective": h.getShareBaseUrl(c),
		"source":    source,
	}))
}
