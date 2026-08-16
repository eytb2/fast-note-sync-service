// Package service: LateManifestBroadcaster —— 服务器侧写入的 NotifyManifest 广播出口。
// WS 服务器在 app 容器构建之后才创建（router 层 SetWSS 回填），而 ContentV3Service 在
// initServices 期就要拿到广播出口，因此做迟绑定：Bind 之前广播静默丢弃。
package service

import (
	"sync"

	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"go.uber.org/zap"
)

// ManifestBroadcastTarget 广播目标（*pkgapp.WebsocketServer 满足此接口）
type ManifestBroadcastTarget interface {
	BroadcastToUser(uid int64, c *code.Code, action string)
}

// manifestBroadcastAction 与 websocket_router.V3NotifyManifest 常量同值；
// 此处不引 routers 包（会造成 import cycle），以字面量同步。
const manifestBroadcastAction = "V3NotifyManifest"

// LateManifestBroadcaster 迟绑定的 ManifestBroadcaster 实现
type LateManifestBroadcaster struct {
	mu  sync.RWMutex
	wss ManifestBroadcastTarget
	log *zap.Logger
}

// NewLateManifestBroadcaster 创建迟绑定广播器
func NewLateManifestBroadcaster(logger *zap.Logger) *LateManifestBroadcaster {
	return &LateManifestBroadcaster{log: logger}
}

// Bind 绑定 WS 服务器（SetWSS 时机调用，之后广播生效）
func (b *LateManifestBroadcaster) Bind(target ManifestBroadcastTarget) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.wss = target
}

// BroadcastManifest 向该用户全部在线连接广播 v3 清单变更。
// 未绑定（服务启动早期）时静默丢弃——客户端下一轮对账仍会发现新 epoch，广播只是加速。
func (b *LateManifestBroadcaster) BroadcastManifest(uid int64, msg *dto.V3NotifyManifestMessage) {
	b.mu.RLock()
	target := b.wss
	b.mu.RUnlock()
	if target == nil {
		return
	}
	target.BroadcastToUser(uid, code.Success.WithData(msg).WithVault(msg.Vault), manifestBroadcastAction)
}
