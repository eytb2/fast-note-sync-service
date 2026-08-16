package websocket_router

import (
	"github.com/haierkeys/fast-note-sync-service/internal/app"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/json"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewMessageInterceptor 创建 WebSocket 业务消息前置拦截器，依次执行认证、Vault 和 RBAC 检查。
// NewMessageInterceptor creates a WebSocket business message pre-handler interceptor
// that sequentially enforces auth, vault access, and RBAC checks.
func NewMessageInterceptor(appContainer *app.App) func(*pkgapp.WebsocketClient, *pkgapp.WebSocketMessage) bool {
	logger := appContainer.Logger()
	return func(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage) bool {
		if !checkAuth(c, msg, logger) {
			return false
		}
		if !checkVaultAccess(c, msg, logger) {
			return false
		}
		if !checkRBAC(c, msg, logger) {
			return false
		}
		return true
	}
}

// checkAuth 验证用户是否已完成身份认证。
// checkAuth verifies that the client has a valid authenticated user session.
func checkAuth(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage, logger interface {
	Warn(string, ...zapcore.Field)
}) bool {
	if c.User == nil {
		logger.Warn("WS User not authenticated",
			zap.String("msgType", msg.Type),
			zap.String("traceId", c.TraceID))
		c.ToResponse(code.ErrorNotUserAuthToken)
		return false
	}
	return true
}

// checkVaultAccess 针对活跃 WebSocket 连接校验笔记库访问权限限制。
// checkVaultAccess validates that the requested vault is within the client's allowed scope.
func checkVaultAccess(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage, logger interface {
	Warn(string, ...zapcore.Field)
}) bool {
	if c.Vaults == "" {
		return true
	}
	var vaultInfo struct {
		Vault string `json:"vault"`
	}
	if err := json.Unmarshal(msg.Data, &vaultInfo); err != nil || vaultInfo.Vault == "" {
		return true
	}
	if !util.VerifyVaultAccess(c.Vaults, vaultInfo.Vault) {
		logger.Warn("WS OnMessage Vault Restricted",
			zap.String("Type", msg.Type),
			zap.String("uid", c.User.ID),
			zap.String("vault", vaultInfo.Vault))
		c.ToResponse(code.ErrorAuthTokenScopeRestricted.WithDetails("Vault access restricted: "+vaultInfo.Vault), msg.Type+"Ack")
		return false
	}
	return true
}

// checkRBAC 将操作映射到 RBAC 权限功能点并校验客户端权限。
// checkRBAC maps the message type to an RBAC function and verifies the client's scope.
func checkRBAC(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage, logger interface {
	Warn(string, ...zapcore.Field)
	Info(string, ...zapcore.Field)
}) bool {
	function := resolveRBACFunction(msg.Type)
	if function == "" {
		return true // 无需权限检查的消息类型，直接放行 / message type requires no RBAC check
	}
	if pkgapp.VerifyPermissions(c.Scope, "ws", c.ClientType(), function) {
		return true
	}
	logger.Warn("WS OnMessage Permission Denied",
		zap.String("Type", msg.Type),
		zap.String("uid", c.User.ID),
		zap.String("function", function))
	return handlePermissionDenied(c, msg, function, logger)
}

// resolveRBACFunction 将 WebSocket 消息类型映射到 RBAC 权限功能点字符串。
// resolveRBACFunction maps a WebSocket message type to its corresponding RBAC function key.
// Returns an empty string if no permission check is required for the given type.
func resolveRBACFunction(msgType string) string {
	switch msgType {
	case V3ReceiveSync, V3ReceiveBlobDownload:
		return "note_r"
	case V3ReceiveCommit, V3ReceiveBlobUploadOpen:
		// v3 提交统一读写全部资源类型；对账协议本身可自愈，权限拒绝无需回滚补偿
		return "note_w"
	}
	return ""
}

// handlePermissionDenied 在权限拒绝后向客户端发送错误响应。
// v3 快照协议以对账收敛，客户端收到拒绝后重新对账即可，无需旧协议的重命名回滚/重推补偿。
// handlePermissionDenied sends an error response and halts message processing.
// The v3 snapshot protocol self-heals via reconcile, so no compensating actions are needed.
func handlePermissionDenied(c *pkgapp.WebsocketClient, msg *pkgapp.WebSocketMessage, function string, logger interface {
	Info(string, ...zapcore.Field)
}) bool {
	resPath := resolveResourcePath(msg)
	c.ToResponse(code.ErrorAuthTokenScopeRestricted.WithDetails("Permission denied: "+resPath), msg.Type+"Ack")
	return false
}

// resolveResourcePath 从消息数据中提取资源路径用于错误描述，优先取 path，其次取 name，最后回退到消息类型。
// resolveResourcePath extracts a human-readable resource identifier from message data,
// falling back to the message type when neither path nor name is present.
func resolveResourcePath(msg *pkgapp.WebSocketMessage) string {
	var pathInfo struct {
		Path string `json:"path"`
		Name string `json:"name"`
	}
	_ = json.Unmarshal(msg.Data, &pathInfo)
	if pathInfo.Path != "" {
		return pathInfo.Path
	}
	if pathInfo.Name != "" {
		return pathInfo.Name
	}
	return msg.Type
}
