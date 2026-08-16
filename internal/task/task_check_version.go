package task

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/app"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"go.uber.org/zap"
	"golang.org/x/mod/semver"
)

const (
	// 自建发布源：版本检查与升级下载全部走自有 GitHub 仓库（eytb2），不再依赖上游官方仓库。
	// Self-hosted release source: all version checks & upgrade downloads go through
	// our own GitHub repos (eytb2); the upstream official repos are no longer used.
	GitHubServiceReleaseURL = "https://api.github.com/repos/eytb2/fast-note-sync-service/releases"
	GitHubPluginReleaseURL  = "https://api.github.com/repos/eytb2/obsidian-fast-note-sync/releases"
	ServiceRepoPath         = "eytb2/fast-note-sync-service"
	ServiceRepoURL          = "https://github.com/" + ServiceRepoPath
	PluginRepoPath          = "eytb2/obsidian-fast-note-sync"
	PluginRepoURL           = "https://github.com/" + PluginRepoPath

	// fetchHTTPTimeout bounds each release-list HTTP request so a hung source
	// can't stall the whole task; the task-layer fallback then retries once.
	// fetchHTTPTimeout 限制单次 release 列表请求的耗时，防止请求卡死拖住整个任务；
	// task 层 fallback 会重试一次。
	fetchHTTPTimeout = 8 * time.Second
)

type GitHubAsset struct {
	Name  string `json:"name"`  // Asset name // 资源包名称
	State string `json:"state"` // Upload state // 上传状态
}

type GitHubRelease struct {
	TagName    string        `json:"tag_name"`
	Prerelease bool          `json:"prerelease"`
	Body       string        `json:"body"`   // Release description (changelog) // 版本发布说明（更新日志）
	Assets     []GitHubAsset `json:"assets"` // Release assets // 资源列表
}

type GitHubTag struct {
	Name string `json:"name"`
}

// releaseSource identifies which source a release list came from.
// releaseSource 标识 release 列表来自哪个源。
type releaseSource string

const sourceGitHub releaseSource = "github"

type CheckVersionTask struct {
	app *app.App
}

func init() {
	RegisterWithApp(func(appContainer *app.App) (Task, error) {
		return &CheckVersionTask{
			app: appContainer,
		}, nil
	})
}

func (t *CheckVersionTask) Name() string {
	return "check_version"
}

func (t *CheckVersionTask) Run(ctx context.Context) error {
	// All release data comes from the single self-hosted GitHub source; the
	// task-layer retry below gives one more attempt on transient failures.
	// 版本数据全部来自单一自建 GitHub 源；task 层重试对瞬时失败多给一次机会。
	// Service releases (with retry)
	// 服务端版本（带重试）
	serviceReleases, serviceLink, serviceChangelog, serviceChangelogContent, err :=
		t.fetchReleasesWithFallback(ctx, GitHubServiceReleaseURL, true)
	if err != nil {
		return fmt.Errorf("fetch service releases: %w", err)
	}

	// Plugin releases
	// 插件版本
	pluginReleases, pluginLink, pluginChangelog, pluginChangelogContent, perr :=
		t.fetchReleasesWithFallbackLinksOnly(ctx, GitHubPluginReleaseURL, false)
	if perr != nil {
		// Plugin version is best-effort; don't fail the whole task if only plugin fetch errors.
		// 插件版本尽力而为：仅插件抓取失败不应导致整个任务失败。
		t.app.Logger().Warn("check_version: plugin releases fetch failed, keeping service result only", zap.Error(perr))
		pluginReleases = nil
	}

	var serviceLatest, pluginLatest string
	if len(serviceReleases) > 0 {
		serviceLatest = serviceReleases[0].Version
	}
	if len(pluginReleases) > 0 {
		pluginLatest = pluginReleases[0].Version
	}

	currentServiceVersion := t.app.Version().Version
	if !strings.HasPrefix(currentServiceVersion, "v") {
		currentServiceVersion = "v" + currentServiceVersion
	}

	if serviceLatest != "" && !strings.HasPrefix(serviceLatest, "v") {
		serviceLatest = "v" + serviceLatest
	}

	if pluginLatest != "" && !strings.HasPrefix(pluginLatest, "v") {
		pluginLatest = "v" + pluginLatest
	}

	info := pkgapp.CheckVersionInfo{
		// 唯一发布源即自建 GitHub，升级下载地址（handler_admin_control.Upgrade）与
		// 版本数据来源天然一致。
		// The only release source is our own GitHub, so the upgrade download URL
		// (decided in handler_admin_control.Upgrade) always matches the version data source.
		GithubAvailable:                  true,
		VersionNewName:                   serviceLatest,
		VersionIsNew:                     serviceLatest != "" && semver.Compare(serviceLatest, currentServiceVersion) > 0,
		VersionNewLink:                   serviceLink,
		VersionNewChangelog:              serviceChangelog,
		VersionNewChangelogContent:       serviceChangelogContent,
		PluginVersionNewName:             pluginLatest,
		PluginVersionNewLink:             pluginLink,
		PluginVersionNewChangelog:        pluginChangelog,
		PluginVersionNewChangelogContent: pluginChangelogContent,
	}

	// 更新 App 中的版本信息和发布列表
	t.app.SetCheckVersionInfo(info)
	t.app.SetCheckVersionReleases(serviceReleases, pluginReleases)

	// 推送版本信息给所有已连接客户端
	t.app.BroadcastClientInfo()

	return nil
}

// fetchReleasesWithFallback fetches from the self-hosted GitHub source; on
// failure/empty it retries once. Returns the assembled release list and the
// link/changelog for the latest release.
// fetchReleasesWithFallback 从自建 GitHub 源抓取；失败或为空时重试一次。
// 返回已过滤的 release 列表，以及最新版的 link/changelog。
func (t *CheckVersionTask) fetchReleasesWithFallback(
	ctx context.Context, ghURL string, isService bool,
) (releases []pkgapp.HistoricalVersion, link, changelogLink, changelogContent string, err error) {
	releases, link, changelogLink, changelogContent, err = t.tryFetch(ctx, ghURL, isService)
	if err == nil && len(releases) > 0 {
		return releases, link, changelogLink, changelogContent, nil
	}
	if err != nil {
		t.app.Logger().Warn("check_version: source failed, retrying",
			zap.Bool("isService", isService),
			zap.Error(err))
	} else {
		t.app.Logger().Warn("check_version: source returned no releases, retrying",
			zap.Bool("isService", isService))
	}

	// retry once
	// 重试一次
	releases, link, changelogLink, changelogContent, ferr := t.tryFetch(ctx, ghURL, isService)
	if ferr != nil {
		if err == nil {
			err = ferr
		}
		return nil, "", "", "", fmt.Errorf("release fetch failed after retry: %w", ferr)
	}
	return releases, link, changelogLink, changelogContent, nil
}

// fetchReleasesWithFallbackLinksOnly is the variant for callers that don't need
// the usedGitHub flag. Kept the signature explicit for clarity in Run();
// see fetchReleasesWithFallback for behavior.
// fetchReleasesWithFallbackLinksOnly 与 fetchReleasesWithFallback 行为相同，
// 供不需要 usedGitHub 的调用点使用（语义清晰）。
func (t *CheckVersionTask) fetchReleasesWithFallbackLinksOnly(
	ctx context.Context, ghURL string, isService bool,
) ([]pkgapp.HistoricalVersion, string, string, string, error) {
	return t.fetchReleasesWithFallback(ctx, ghURL, isService)
}

// tryFetch fetches from the self-hosted GitHub source and assembles links.
// tryFetch 从自建 GitHub 源抓取并拼装链接。
func (t *CheckVersionTask) tryFetch(
	ctx context.Context, ghURL string, isService bool,
) (releases []pkgapp.HistoricalVersion, link, changelogLink, changelogContent string, err error) {
	releases, err = t.fetchGitHubReleasesCtx(ctx, ghURL)
	if err != nil {
		return nil, "", "", "", err
	}
	link, changelogLink, changelogContent = buildLinks(releases, isService)
	return releases, link, changelogLink, changelogContent, nil
}

// buildLinks assembles the release page / changelog URLs for the latest release.
// buildLinks 为最新版拼装 release 页面与 changelog 的 URL。
func buildLinks(releases []pkgapp.HistoricalVersion, isService bool) (link, changelogLink, changelogContent string) {
	if len(releases) == 0 {
		return "", "", ""
	}
	latest := releases[0].Version
	changelogContent = releases[0].ChangelogContent
	latestClean := strings.TrimPrefix(latest, "v")
	base := ServiceRepoURL
	if !isService {
		base = PluginRepoURL
	}
	link = base + "/releases/tag/" + latestClean
	changelogLink = base + "/releases/download/" + latestClean + "/changelog.txt"
	return link, changelogLink, changelogContent
}

// hasValidAssets checks if there is at least one uploaded zip or tar.gz file
// hasValidAssets 检查是否包含至少一个已成功上传的 zip 或 tar.gz 资源文件
func hasValidAssets(assets []GitHubAsset) bool {
	for _, asset := range assets {
		name := strings.ToLower(asset.Name)
		if strings.HasSuffix(name, ".zip") || strings.HasSuffix(name, ".tar.gz") {
			if asset.State == "" || asset.State == "uploaded" {
				return true
			}
		}
	}
	return false
}

func (t *CheckVersionTask) fetchGitHubReleasesCtx(ctx context.Context, url string) ([]pkgapp.HistoricalVersion, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	cli := &http.Client{Timeout: fetchHTTPTimeout}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub API returned status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var releases []GitHubRelease
	if err := json.Unmarshal(body, &releases); err != nil {
		return nil, err
	}

	releaseChannel := t.app.Config().App.PullReleaseChannel
	var result []pkgapp.HistoricalVersion
	for _, release := range releases {
		if releaseChannel == "stable" && release.Prerelease {
			continue
		}
		if !hasValidAssets(release.Assets) {
			continue
		}
		tagName := release.TagName
		if !strings.HasPrefix(tagName, "v") {
			tagName = "v" + tagName
		}
		result = append(result, pkgapp.HistoricalVersion{
			Version:          tagName,
			ChangelogContent: release.Body,
		})
	}

	return result, nil
}

func (t *CheckVersionTask) LoopInterval() time.Duration {
	return 10 * time.Minute
}

func (t *CheckVersionTask) IsStartupRun() bool {
	return true
}
