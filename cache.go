package main

import (
	"context"
	"errors"
	"strings"
	"time"
)

func (a *app) refreshPlayCache(ctx context.Context, videoID string, watchURL string) (playCacheEntry, error) {
	info, err := a.runYtDlpJSON(ctx, watchURL)
	if err != nil {
		return playCacheEntry{}, err
	}
	items := map[string]playQualityItem{}
	for _, f := range info.Formats {
		if !hasCodec(f.VCodec) || !isHTTPURL(f.URL) {
			continue
		}
		if !hasCodec(f.ACodec) {
			continue
		}
		label := qualityLabelForFormat(f)
		if label == "" {
			continue
		}
		key := strings.ToUpper(strings.TrimSpace(label))
		if _, exists := items[key]; exists {
			continue
		}
		items[key] = playQualityItem{Label: label, Mode: "m3u8", Format: f}
	}
	if len(items) == 0 {
		return playCacheEntry{}, errors.New("no playable qualities")
	}
	audio, _ := selectBestAudioTrack(info.Formats)
	entry := playCacheEntry{
		Meta: playMetaCache{
			WatchURL:    watchURL,
			DurationSec: info.Duration,
			Items:       items,
			Formats:     info.Formats,
			Audio:       audio,
		},
		Resolved: playResolvedCache{
			Video: map[string]mediaTrack{},
			Audio: map[string]mediaTrack{},
			HLS:   map[string]hlsResolved{},
		},
		ExpiresAt: time.Now().Add(playCacheTTL),
	}
	a.playCacheMu.Lock()
	a.playCache[videoID] = entry
	a.playCacheMu.Unlock()
	return entry, nil
}

func (a *app) ensurePlayCache(ctx context.Context, videoID, watchURL string, forceRefresh bool) (playCacheEntry, bool, error) {
	now := time.Now()
	a.playCacheMu.Lock()
	entry, ok := a.playCache[videoID]
	cacheValid := ok && now.Before(entry.ExpiresAt) && strings.TrimSpace(entry.Meta.WatchURL) != "" && len(entry.Meta.Items) > 0
	if cacheValid && !forceRefresh {
		entry.ExpiresAt = now.Add(playCacheTTL)
		a.playCache[videoID] = entry
		a.playCacheMu.Unlock()
		return entry, false, nil
	}
	a.playCacheMu.Unlock()

	entry, err := a.refreshPlayCache(ctx, videoID, watchURL)
	if err != nil {
		return playCacheEntry{}, true, err
	}
	return entry, true, nil
}
