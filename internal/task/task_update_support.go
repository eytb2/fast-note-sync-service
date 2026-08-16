package task

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/app"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"go.uber.org/zap"
)

const (
	// 支持信息只从自建 GitHub 仓库拉取（eytb2），不再使用上游官方源。
	// Support info is fetched from the self-hosted GitHub repo (eytb2) only.
	SupportRawURL = "https://raw.githubusercontent.com/eytb2/fast-note-sync-service/refs/heads/master/docs/Support.%s.json"
)

type UpdateSupportTask struct {
	app *app.App
}

func init() {
	RegisterWithApp(func(appContainer *app.App) (Task, error) {
		return &UpdateSupportTask{
			app: appContainer,
		}, nil
	})
}

func (t *UpdateSupportTask) Name() string {
	return "update_support"
}

func (t *UpdateSupportTask) Run(ctx context.Context) error {

	recordsMap := t.app.GetSupportRecords()
	for lang := range recordsMap {
		var url string
		var remoteLang = lang
		// Some lang in files are zh-CN, zh-TW, but keys are zh-cn, zh-tw
		// We need to try matching the filename case if possible, but the raw URL usually follows the repo filename.
		// If the repo has Support.zh-CN.json, fetching Support.zh-cn.json might fail on case-sensitive FS.
		// However, our keys are already lowercase. Let's try to reconstruct potentially correct filename case for zh-CN/zh-TW.
		if lang == "zh-cn" {
			remoteLang = "zh-CN"
		} else if lang == "zh-tw" {
			remoteLang = "zh-TW"
		}

		url = fmt.Sprintf(SupportRawURL, remoteLang)

		records, err := t.fetchSupportRecords(url)
		if err != nil {
			t.app.Logger().Warn("Failed to fetch support records", zap.String("lang", lang), zap.String("url", url), zap.Error(err))
			continue
		}

		if len(records) > 0 {
			t.app.UpdateSupportRecords(lang, records)
		}
	}

	return nil
}

func (t *UpdateSupportTask) fetchSupportRecords(url string) ([]pkgapp.SupportRecord, error) {
	client := http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var records []pkgapp.SupportRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, err
	}

	return records, nil
}

func (t *UpdateSupportTask) LoopInterval() time.Duration {
	return 1 * time.Hour
}

func (t *UpdateSupportTask) IsStartupRun() bool {
	return true
}
