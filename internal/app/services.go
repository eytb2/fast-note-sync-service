package app

import (
	"github.com/haierkeys/fast-note-sync-service/internal/service"
	"go.uber.org/zap"
)

// Services encapsulates all business service instances
type Services struct {
	VaultService      service.VaultService
	UserService       service.UserService
	TokenService      service.TokenService
	SettingService    service.SettingService
	ShareService      service.ShareService
	NoteLinkService   service.NoteLinkService
	StorageService    service.StorageService
	BackupService     service.BackupService
	GitSyncService    service.GitSyncService
	CloudflareService service.CloudflareService
	SyncLogService    service.SyncLogService
	OIDCService       service.OIDCService
	SyncV3Service     service.SyncV3Service

	// v3 功能回接（P5）：服务器侧内容门面 + NotifyManifest 广播出口（迟绑定 WS 服务器）
	ContentV3Service    service.ContentV3Service
	ManifestBroadcaster *service.LateManifestBroadcaster

	// v3 门面服务：REST/MCP/静态路由/后台任务统一使用（P7R 起旧 WS v1/v2 管线已删除）
	NoteServiceV3        service.NoteService
	FileServiceV3        service.FileService
	FolderServiceV3      service.FolderService
	NoteHistoryServiceV3 service.NoteHistoryService
}

// initServices initializes all services
func initServices(cfg *AppConfig, infra *Infra, repos *Repositories, logger *zap.Logger) *Services {
	svcConfig := &service.ServiceConfig{
		User: service.UserServiceConfig{
			RegisterIsEnable: cfg.User.RegisterIsEnable,
			AdminUID:         cfg.User.AdminUID,
		},
		Token: service.TokenServiceConfig{
			WebGUILoginTokenExpiry: cfg.Security.WebGUILoginTokenExpiry,
			WebGUILoginTokenBindIP: *cfg.Security.WebGUILoginTokenBindIP,
		},
		App: service.AppServiceConfig{
			SoftDeleteRetentionTime: cfg.App.SoftDeleteRetentionTime,
			HistoryKeepVersions:     cfg.App.HistoryKeepVersions,
			HistorySaveDelay:        cfg.App.HistorySaveDelay,
			ShareTokenExpiry:        cfg.Security.ShareTokenExpiry,
			ShortLink: service.ShortLinkServiceConfig{
				BaseURL:  cfg.ShortLink.BaseURL,
				APIKey:   cfg.ShortLink.APIKey,
				Password: cfg.ShortLink.Password,
				Cloaking: cfg.ShortLink.Cloaking,
			},
		},
	}

	s := &Services{}
	s.VaultService = service.NewVaultService(
		repos.VaultRepo,
		repos.FsEntryRepo,
		infra.Dao.BleveMgr,
		repos.BlobStore,
		repos.NoteRepo,
		repos.FileRepo,
		repos.FolderRepo,
		repos.SyncLogRepo,
		repos.NoteHistoryRepo,
		repos.NoteLinkRepo,
		repos.SettingRepo,
		repos.NoteFTSRepo,
		repos.ShareRepo,
		repos.GitSyncRepo,
		repos.BackupRepo,
		logger,
	)
	s.StorageService = service.NewStorageService(repos.StorageRepo, &cfg.Storage)
	s.BackupService = service.NewBackupService(repos.BackupRepo, repos.FsEntryRepo, repos.BlobStore, repos.VaultRepo, s.StorageService, &cfg.Storage, cfg.App.TempPath, logger)
	s.GitSyncService = service.NewGitSyncService(repos.GitSyncRepo, repos.FsEntryRepo, repos.BlobStore, repos.VaultRepo, repos.SettingRepo, &cfg.Git, logger)

	// Initialize SyncLogService first, as NoteService/FileService/SettingService depend on it
	// SyncLogService 必须最先初始化，因为其他服务依赖它
	s.SyncLogService = service.NewSyncLogService(repos.SyncLogRepo, logger)

	s.TokenService = service.NewTokenService(repos.AuthTokenRepo, repos.AuthTokenLogRepo, infra.TokenManager, logger, svcConfig.Token)
	s.UserService = service.NewUserService(repos.UserRepo, infra.TokenManager, s.TokenService, logger, svcConfig)
	s.OIDCService = service.NewOIDCService(repos.UserRepo, repos.OIDCIdentityRepo, s.TokenService)
	s.ShareService = service.NewShareService(repos.ShareRepo, infra.TokenManager, repos.VaultRepo, repos.FsEntryRepo, repos.BlobStore, logger, svcConfig)
	// NoteLinkService 延后到 v3 门面初始化之后构造（P5：链接查询改走清单计算，
	// 旧 note_links 索引表不再被 v3 提交填充）
	s.CloudflareService = service.NewCloudflareService(logger)
	s.SyncV3Service = service.NewSyncV3Service(repos.FsEntryRepo, repos.ManifestRepo, repos.EntryHistRepo, repos.BlobStore, s.VaultService, logger)

	// v3 功能回接（P5）：
	// 1) 内容门面——REST/MCP/Web 的服务器侧读写入口，写走 Commit 管线（epoch/广播/副作用同客户端提交）
	// 2) 提交副作用监听——旧层写路径的 sync_log/FTS/备份/Git/分享撤销 挂到 v3 提交上
	//    （分享撤销在 ShareService 回接 v3 后注入，暂传 nil）
	s.ManifestBroadcaster = service.NewLateManifestBroadcaster(logger)
	s.ContentV3Service = service.NewContentV3Service(
		repos.FsEntryRepo, repos.ManifestRepo, repos.EntryHistRepo, repos.BlobStore,
		s.VaultService, s.SyncV3Service, s.ManifestBroadcaster, logger,
	)
	s.SyncV3Service.AddCommitListener(service.NewV3SideEffects(
		s.SyncLogService, s.BackupService, s.GitSyncService,
		infra.Dao.BleveMgr, repos.BlobStore,
		s.ShareService, // V3ShareRevoker：条目删除 → 撤销分享
		logger,
	))

	// v3 门面版同名服务（REST/MCP/静态路由/后台任务用；构造顺序依赖 ContentV3Service）
	s.NoteServiceV3 = service.NewNoteServiceV3(s.ContentV3Service, repos.FsEntryRepo, repos.ManifestRepo, s.VaultService, infra.Dao.BleveMgr, logger)
	s.FileServiceV3 = service.NewFileServiceV3(s.ContentV3Service, repos.FsEntryRepo, repos.ManifestRepo, repos.BlobStore, s.VaultService, logger)
	s.FolderServiceV3 = service.NewFolderServiceV3(s.ContentV3Service, repos.ManifestRepo, s.VaultService, logger)
	s.NoteHistoryServiceV3 = service.NewNoteHistoryServiceV3(s.ContentV3Service, repos.EntryHistRepo, repos.FsEntryRepo, s.VaultService, logger)
	s.NoteLinkService = service.NewNoteLinkServiceV3(repos.FsEntryRepo, repos.ManifestRepo, s.ContentV3Service, s.VaultService, logger)
	s.SettingService = service.NewSettingServiceV3(s.ContentV3Service, repos.FsEntryRepo, s.VaultService, repos.BlobStore, s.SyncLogService, logger)

	return s
}
