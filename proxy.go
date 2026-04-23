package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ---- from proxy_handlers.go ----
func (a *app) handleProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w)
		return
	}
	prefix := externalURLPrefix(r)
	rest := strings.TrimPrefix(r.URL.Path, "/proxy/")
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "not found"})
		return
	}
	videoID := strings.TrimSpace(parts[0])
	kind := strings.ToLower(strings.TrimSpace(parts[1]))
	name := strings.TrimSpace(parts[2])
	if videoID == "" || kind == "" || name == "" {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "not found"})
		return
	}
	if kind == "mpd" {
		a.handleInternalStreamByVideoID(w, r, videoID, name)
		return
	}
	if kind != "manifest" {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "not found"})
		return
	}
	dot := strings.LastIndex(name, ".")
	if dot <= 0 || dot >= len(name)-1 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid quality name"})
		return
	}
	label := strings.TrimSpace(name[:dot])
	ext := strings.ToLower(strings.TrimSpace(name[dot+1:]))
	if label == "" || (ext != "m3u8" && ext != "mpd") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid quality route"})
		return
	}
	wantKey := strings.ToUpper(strings.TrimSpace(label))

	watchURL := "https://www.youtube.com/watch?v=" + videoID
	ctxWarm, cancelWarm := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancelWarm()
	entry, _, err := a.ensurePlayCache(ctxWarm, videoID, watchURL, false)
	if err != nil {
		writeJSON(w, statusForResolveErr(err), map[string]any{"ok": false, "message": err.Error()})
		return
	}
	item, ok := entry.Meta.Items[wantKey]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "quality not found"})
		return
	}
	if item.Mode != ext {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "quality mode mismatch"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), resolveTimeout)
	defer cancel()
	pick := item.Format
	if ext == "m3u8" {
		cacheKey := wantKey + "|" + strings.TrimSpace(r.Header.Get("User-Agent"))
		cachedHLS := hlsResolved{}
		hasCachedHLS := false
		a.playCacheMu.Lock()
		if cached, exists := a.playCache[videoID]; exists && cached.Resolved.HLS != nil {
			if v, ok := cached.Resolved.HLS[cacheKey]; ok {
				cachedHLS = v
				hasCachedHLS = true
			}
		}
		a.playCacheMu.Unlock()

		playlist := ""
		if hasCachedHLS {
			playlist = rewriteM3U8ToURLProxy(cachedHLS.Raw, cachedHLS.MediaURL, prefix)
		} else if isHLSURL(strings.TrimSpace(pick.URL)) {
			mediaURL, mediaRaw, e := a.resolveMediaPlaylist(ctx, strings.TrimSpace(pick.URL), strings.TrimSpace(r.Header.Get("User-Agent")))
			if e != nil {
				writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": e.Error()})
				return
			}
			a.playCacheMu.Lock()
			if cached, exists := a.playCache[videoID]; exists {
				if cached.Resolved.HLS == nil {
					cached.Resolved.HLS = map[string]hlsResolved{}
				}
				cached.Resolved.HLS[cacheKey] = hlsResolved{MediaURL: mediaURL, Raw: mediaRaw, UA: strings.TrimSpace(r.Header.Get("User-Agent"))}
				a.playCache[videoID] = cached
			}
			a.playCacheMu.Unlock()
			playlist = rewriteM3U8ToURLProxy(mediaRaw, mediaURL, prefix)
		} else {
			raw := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:3600\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:3600.0,\n" + strings.TrimSpace(pick.URL) + "\n#EXT-X-ENDLIST\n"
			playlist = rewriteM3U8ToURLProxy(raw, strings.TrimSpace(pick.URL), prefix)
		}
		writePlaylistText(w, playlist)
		return
	}

	videoTrack := mediaTrack{}
	audioTrack := mediaTrack{}
	a.playCacheMu.Lock()
	if cached, exists := a.playCache[videoID]; exists {
		if cached.Resolved.Video != nil {
			videoTrack = cached.Resolved.Video[wantKey]
		}
		if cached.Resolved.Audio != nil {
			audioTrack = cached.Resolved.Audio[wantKey]
		}
	}
	a.playCacheMu.Unlock()
	if strings.TrimSpace(trackPathExt(videoTrack)) != "" && !strings.EqualFold(trackPathExt(videoTrack), "webm") {
		videoTrack = mediaTrack{}
		audioTrack = mediaTrack{}
	}

	if !isResolvedDashPairUsable(videoTrack, audioTrack) {
		bestAudio := entry.Meta.Audio
		if !hasCodec(bestAudio.ACodec) || !isHTTPURL(bestAudio.URL) {
			info, err := a.runYtDlpJSON(ctx, entry.Meta.WatchURL)
			if err != nil {
				writeJSON(w, statusForResolveErr(err), map[string]any{"ok": false, "message": err.Error()})
				return
			}
			ba, ok := selectBestAudioTrack(info.Formats)
			if !ok {
				writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "audio track not found for dash"})
				return
			}
			bestAudio = ba
			a.playCacheMu.Lock()
			if cached, ok := a.playCache[videoID]; ok {
				cached.Meta.Audio = ba
				a.playCache[videoID] = cached
			}
			a.playCacheMu.Unlock()
		}

		vt, at, ok := chooseBestDashPair(ctx, item.Label, "", pick, bestAudio, entry.Meta.Formats)
		if !ok {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "dash index not available"})
			return
		}
		videoTrack, audioTrack = vt, at
	}
	if !hasTrackIndex(videoTrack) || !hasTrackIndex(audioTrack) {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "dash track index incomplete"})
		return
	}
	if strings.TrimSpace(trackPathExt(videoTrack)) == "" || strings.TrimSpace(trackPathExt(audioTrack)) == "" {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "dash track ext unresolved"})
		return
	}
	if strings.TrimSpace(videoTrack.MimeType) == "" || strings.TrimSpace(audioTrack.MimeType) == "" {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "dash track mime unresolved"})
		return
	}
	if err := validateDashTrackByFormat(videoTrack); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "invalid video track for dash: " + err.Error()})
		return
	}
	if err := validateDashTrackByFormat(audioTrack); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "invalid audio track for dash: " + err.Error()})
		return
	}
	if !isDashContainerAligned(videoTrack, audioTrack) {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "dash container mismatch"})
		return
	}
	log.Printf("[yt-bridge] mpd-track video quality=%s ext=%s mime=%s codec=%s ts=%d segs=%d init=%d-%d index=%d-%d", wantKey, trackPathExt(videoTrack), videoTrack.MimeType, videoTrack.Codec, videoTrack.Timescale, len(videoTrack.Segments), videoTrack.InitStart, videoTrack.InitEnd, videoTrack.IndexStart, videoTrack.IndexEnd)
	log.Printf("[yt-bridge] mpd-track audio quality=%s ext=%s mime=%s codec=%s ts=%d segs=%d init=%d-%d index=%d-%d", wantKey, trackPathExt(audioTrack), audioTrack.MimeType, audioTrack.Codec, audioTrack.Timescale, len(audioTrack.Segments), audioTrack.InitStart, audioTrack.InitEnd, audioTrack.IndexStart, audioTrack.IndexEnd)
	if len(videoTrack.Segments) > 0 {
		last := videoTrack.Segments[len(videoTrack.Segments)-1]
		log.Printf("[yt-bridge] mpd-track video-seg first=%d-%d d=%d last=%d-%d d=%d", videoTrack.Segments[0].Start, videoTrack.Segments[0].End, videoTrack.Segments[0].Duration, last.Start, last.End, last.Duration)
		if strings.EqualFold(trackPathExt(videoTrack), "webm") {
			log.Printf("[yt-bridge] mpd-track video-cue-time first=%s", summarizeSegmentTimes(videoTrack.Segments, 8))
		}
	}
	if len(audioTrack.Segments) > 0 {
		last := audioTrack.Segments[len(audioTrack.Segments)-1]
		log.Printf("[yt-bridge] mpd-track audio-seg first=%d-%d d=%d last=%d-%d d=%d", audioTrack.Segments[0].Start, audioTrack.Segments[0].End, audioTrack.Segments[0].Duration, last.Start, last.End, last.Duration)
		if strings.EqualFold(trackPathExt(audioTrack), "webm") {
			log.Printf("[yt-bridge] mpd-track audio-cue-time first=%s", summarizeSegmentTimes(audioTrack.Segments, 8))
		}
	}
	a.playCacheMu.Lock()
	if cached, exists := a.playCache[videoID]; exists {
		if cached.Resolved.Video == nil {
			cached.Resolved.Video = map[string]mediaTrack{}
		}
		if cached.Resolved.Audio == nil {
			cached.Resolved.Audio = map[string]mediaTrack{}
		}
		cached.Resolved.Video[wantKey] = videoTrack
		cached.Resolved.Audio[wantKey] = audioTrack
		a.playCache[videoID] = cached
	}
	a.playCacheMu.Unlock()
	webmTemplate := strings.EqualFold(trackPathExt(videoTrack), "webm") && strings.EqualFold(trackPathExt(audioTrack), "webm")
	mpdText, err := buildDashPlayText(videoID, item.Label, entry.Meta.DurationSec, videoTrack, audioTrack, webmTemplate, prefix)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": err.Error()})
		return
	}
	writeMPDText(w, mpdText)
}

func (a *app) handleImageProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/proxy/image/")
	if rest == r.URL.Path || strings.TrimSpace(rest) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid image route"})
		return
	}
	videoID := strings.TrimSpace(rest)
	if strings.Contains(videoID, "/") {
		videoID = strings.TrimSpace(strings.SplitN(videoID, "/", 2)[0])
	}
	if videoID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid image route"})
		return
	}
	if !a.tryProxyImageURL(w, r, youtubeThumbnailURL(videoID)) {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "image upstream not available"})
	}
}

func (a *app) handleURLProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w)
		return
	}
	prefix := "/proxy/url/"
	escaped := r.URL.EscapedPath()
	if strings.TrimSpace(escaped) == "" {
		escaped = r.URL.Path
	}
	if !strings.HasPrefix(escaped, prefix) || len(escaped) <= len(prefix) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid url route"})
		return
	}
	rawEscaped := strings.TrimSpace(escaped[len(prefix):])
	if rawEscaped == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid url route"})
		return
	}
	targetURL, err := url.PathUnescape(rawEscaped)
	if err != nil || strings.TrimSpace(targetURL) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid target url"})
		return
	}
	u, err := url.Parse(strings.TrimSpace(targetURL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || strings.TrimSpace(u.Hostname()) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid target url"})
		return
	}
	if !isAllowedVideoProxyHost(u.Hostname()) {
		writeJSON(w, http.StatusForbidden, map[string]any{"ok": false, "message": "forbidden target host"})
		return
	}
	a.proxyAssetWithRefresh(w, r, u.String(), nil)
}

func (a *app) tryProxyImageURL(w http.ResponseWriter, r *http.Request, raw string) bool {
	targetURL, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(targetURL.Hostname()))
	if host == "" || (host != "ytimg.com" && !strings.HasSuffix(host, ".ytimg.com")) {
		return false
	}
	if targetURL.Scheme != "http" && targetURL.Scheme != "https" {
		return false
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return false
	}
	resp, err := a.upstreamClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}
	if ct := strings.TrimSpace(resp.Header.Get("Content-Type")); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, resp.Body)
	return true
}

func youtubeThumbnailURL(videoID string) string {
	id := strings.TrimSpace(videoID)
	if id == "" {
		return ""
	}
	return "https://i.ytimg.com/vi/" + id + "/hqdefault.jpg"
}

func buildImagePathByVodID(base, prefix, vodID string) string {
	videoID, err := extractYouTubeVideoIDFromWatchID(strings.TrimSpace(vodID))
	if err != nil || strings.TrimSpace(videoID) == "" {
		return ""
	}
	p := withPathPrefix(prefix, "/proxy/image/"+url.PathEscape(videoID))
	if base != "" {
		return base + p
	}
	return p
}

func (a *app) handleInternalStreamByVideoID(w http.ResponseWriter, r *http.Request, videoID string, rest string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "message": "method not allowed"})
		return
	}
	videoID = strings.TrimSpace(videoID)
	rest = strings.TrimSpace(rest)
	if videoID == "" || rest == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid route"})
		return
	}

	if proxyReq, ok := parseInternalProxyPath(videoID, rest, r); ok {
		a.proxyAssetWithRefresh(w, r, proxyReq.TargetURL, func() (string, bool) {
			ctx2, cancel2 := context.WithTimeout(r.Context(), 45*time.Second)
			defer cancel2()
			entry, _, err := a.ensurePlayCache(ctx2, proxyReq.VideoID, "https://www.youtube.com/watch?v="+proxyReq.VideoID, true)
			if err != nil {
				return "", false
			}
			item, ok := entry.Meta.Items[proxyReq.QualityKey]
			if !ok {
				return "", false
			}
			if !isHLSURL(strings.TrimSpace(item.Format.URL)) {
				return "", false
			}
			mediaURL, mediaRaw, e := a.resolveMediaPlaylist(ctx2, strings.TrimSpace(item.Format.URL), proxyReq.UA)
			if e != nil {
				return "", false
			}
			target, ok := resolveProxyTargetFromMedia(mediaURL, mediaRaw, proxyReq.TargetURL)
			if !ok {
				return "", false
			}
			a.playCacheMu.Lock()
			if cached, exists := a.playCache[proxyReq.VideoID]; exists {
				if cached.Resolved.HLS == nil {
					cached.Resolved.HLS = map[string]hlsResolved{}
				}
				cacheKey := proxyReq.QualityKey + "|" + proxyReq.UA
				cached.Resolved.HLS[cacheKey] = hlsResolved{MediaURL: mediaURL, Raw: mediaRaw, UA: proxyReq.UA}
				a.playCache[proxyReq.VideoID] = cached
			}
			a.playCacheMu.Unlock()
			return target, true
		})
		return
	}

	if req, ok, _ := parseInternalMediaPath(videoID, rest); ok {
		watchURL := "https://www.youtube.com/watch?v=" + req.VideoID
		ctxWarm, cancelWarm := context.WithTimeout(r.Context(), 45*time.Second)
		entry, _, err := a.ensurePlayCache(ctxWarm, req.VideoID, watchURL, false)
		cancelWarm()
		if err != nil {
			writeJSON(w, statusForResolveErr(err), map[string]any{"ok": false, "message": err.Error()})
			return
		}
		if !req.IsAudio {
			if _, ok := entry.Meta.Items[req.QualityKey]; !ok {
				writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "quality not found"})
				return
			}
			if req.Ext != "" {
				hasWantedExt := false
				for _, f := range entry.Meta.Formats {
					if !hasCodec(f.VCodec) || !isHTTPURL(f.URL) {
						continue
					}
					if strings.ToUpper(strings.TrimSpace(qualityLabelForFormat(f))) != req.QualityKey {
						continue
					}
					if strings.ToLower(strings.TrimSpace(f.Ext)) == strings.ToLower(strings.TrimSpace(req.Ext)) {
						hasWantedExt = true
						break
					}
				}
				if !hasWantedExt {
					writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "quality ext not available"})
					return
				}
			}
		}

		if req.IsAudio {
			if req.Ext != "" {
				wantAudioExt := strings.ToLower(strings.TrimSpace(req.Ext))
				hasWanted := false
				for _, f := range entry.Meta.Formats {
					if hasCodec(f.VCodec) || !hasCodec(f.ACodec) || !isHTTPURL(f.URL) {
						continue
					}
					if strings.ToLower(strings.TrimSpace(f.Ext)) == wantAudioExt {
						hasWanted = true
						break
					}
				}
				if !hasWanted {
					writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "audio ext not available"})
					return
				}
			}
			audioTrack := mediaTrack{}
			a.playCacheMu.Lock()
			if cached, ok := a.playCache[req.VideoID]; ok && cached.Resolved.Audio != nil {
				audioTrack = cached.Resolved.Audio[req.QualityKey]
			}
			a.playCacheMu.Unlock()
			if strings.TrimSpace(audioTrack.URL) == "" {
				audioTrack = enrichTrackIndexStandalone(r.Context(), formatToMediaTrack(entry.Meta.Audio))
			}
			if strings.TrimSpace(audioTrack.URL) == "" {
				writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "audio range not available"})
				return
			}
			if req.Ext != "" && !strings.EqualFold(trackPathExt(audioTrack), req.Ext) {
				for _, f := range entry.Meta.Formats {
					if hasCodec(f.VCodec) || !hasCodec(f.ACodec) || !isHTTPURL(f.URL) {
						continue
					}
					if strings.ToLower(strings.TrimSpace(f.Ext)) != strings.ToLower(strings.TrimSpace(req.Ext)) {
						continue
					}
					t := enrichTrackIndexStandalone(r.Context(), formatToMediaTrack(f))
					if strings.TrimSpace(t.URL) != "" {
						audioTrack = t
						a.playCacheMu.Lock()
						if cached, ok := a.playCache[req.VideoID]; ok {
							if cached.Resolved.Audio == nil {
								cached.Resolved.Audio = map[string]mediaTrack{}
							}
							cached.Resolved.Audio[req.QualityKey] = audioTrack
							a.playCache[req.VideoID] = cached
						}
						a.playCacheMu.Unlock()
						break
					}
				}
			}
			if req.Ext != "" && !strings.EqualFold(trackPathExt(audioTrack), req.Ext) {
				writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "audio ext mismatch"})
				return
			}
			if start, end, ok := webMTemplateSliceForInternalReq(req, audioTrack); ok {
				if idx := locateSegmentByRange(audioTrack.Segments, start, end); idx >= 0 {
					if ts, te, ok := segmentTimeWindowMS(audioTrack.Segments, idx); ok {
						log.Printf("[yt-bridge] segmap audio idx=%d t_ms=%d-%d range=%d-%d", idx, ts, te, start, end)
					}
					a.noteSegTrace(req.VideoID+":audio:"+req.QualityKey+":"+trackPathExt(audioTrack), idx)
				}
				a.proxyAssetTemplateSlice(w, r, audioTrack.URL, start, end)
				return
			}
			if rs, re, ok := parseClosedBytesRangeHeader(r.Header.Get("Range")); ok {
				if idx := locateSegmentByRange(audioTrack.Segments, rs, re); idx >= 0 {
					if ts, te, ok := segmentTimeWindowMS(audioTrack.Segments, idx); ok {
						log.Printf("[yt-bridge] segmap audio idx=%d t_ms=%d-%d range=%d-%d", idx, ts, te, rs, re)
					}
					a.noteSegTrace(req.VideoID+":audio:"+req.QualityKey+":"+trackPathExt(audioTrack), idx)
				} else {
					log.Printf("[yt-bridge] segmap audio idx=MISS range=%d-%d", rs, re)
				}
			}
			a.proxyAssetWithRefresh(w, r, audioTrack.URL, func() (string, bool) {
				ctx2, cancel2 := context.WithTimeout(r.Context(), 45*time.Second)
				defer cancel2()
				fresh, _, err := a.ensurePlayCache(ctx2, req.VideoID, "https://www.youtube.com/watch?v="+req.VideoID, false)
				if err != nil {
					return "", false
				}
				if req.Ext != "" {
					for _, f := range fresh.Meta.Formats {
						if hasCodec(f.VCodec) || !hasCodec(f.ACodec) || !isHTTPURL(f.URL) {
							continue
						}
						if strings.ToLower(strings.TrimSpace(f.Ext)) != strings.ToLower(strings.TrimSpace(req.Ext)) {
							continue
						}
						return strings.TrimSpace(f.URL), true
					}
				}
				if hasCodec(fresh.Meta.Audio.ACodec) && isHTTPURL(fresh.Meta.Audio.URL) {
					return strings.TrimSpace(fresh.Meta.Audio.URL), true
				}
				return "", false
			})
			return
		}

		videoTrack := mediaTrack{}
		audioTrack := mediaTrack{}
		a.playCacheMu.Lock()
		if cached, ok := a.playCache[req.VideoID]; ok {
			if cached.Resolved.Video != nil {
				videoTrack = cached.Resolved.Video[req.QualityKey]
			}
			if cached.Resolved.Audio != nil {
				audioTrack = cached.Resolved.Audio[req.QualityKey]
			}
		}
		a.playCacheMu.Unlock()

		if strings.TrimSpace(videoTrack.URL) == "" {
			item, ok := entry.Meta.Items[req.QualityKey]
			if !ok {
				writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "quality not found"})
				return
			}
			vt, at, ok := chooseBestDashPair(r.Context(), item.Label, req.Ext, item.Format, entry.Meta.Audio, entry.Meta.Formats)
			if !ok {
				writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "dash index not available"})
				return
			}
			videoTrack, audioTrack = vt, at
			a.playCacheMu.Lock()
			if cached, ok := a.playCache[req.VideoID]; ok {
				if cached.Resolved.Video == nil {
					cached.Resolved.Video = map[string]mediaTrack{}
				}
				if cached.Resolved.Audio == nil {
					cached.Resolved.Audio = map[string]mediaTrack{}
				}
				cached.Resolved.Video[req.QualityKey] = videoTrack
				cached.Resolved.Audio[req.QualityKey] = audioTrack
				a.playCache[req.VideoID] = cached
			}
			a.playCacheMu.Unlock()
		}

		if strings.TrimSpace(videoTrack.URL) == "" {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "video range not available"})
			return
		}
		if req.Ext != "" && !strings.EqualFold(trackPathExt(videoTrack), req.Ext) {
			writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "video ext mismatch"})
			return
		}
		if start, end, ok := webMTemplateSliceForInternalReq(req, videoTrack); ok {
			if idx := locateSegmentByRange(videoTrack.Segments, start, end); idx >= 0 {
				if ts, te, ok := segmentTimeWindowMS(videoTrack.Segments, idx); ok {
					log.Printf("[yt-bridge] segmap video idx=%d t_ms=%d-%d range=%d-%d", idx, ts, te, start, end)
				}
				a.noteSegTrace(req.VideoID+":video:"+req.QualityKey+":"+trackPathExt(videoTrack), idx)
			}
			a.proxyAssetTemplateSlice(w, r, videoTrack.URL, start, end)
			return
		}
		if rs, re, ok := parseClosedBytesRangeHeader(r.Header.Get("Range")); ok {
			if idx := locateSegmentByRange(videoTrack.Segments, rs, re); idx >= 0 {
				if ts, te, ok := segmentTimeWindowMS(videoTrack.Segments, idx); ok {
					log.Printf("[yt-bridge] segmap video idx=%d t_ms=%d-%d range=%d-%d", idx, ts, te, rs, re)
				}
				a.noteSegTrace(req.VideoID+":video:"+req.QualityKey+":"+trackPathExt(videoTrack), idx)
			} else {
				log.Printf("[yt-bridge] segmap video idx=MISS range=%d-%d", rs, re)
			}
		}
		a.proxyAssetWithRefresh(w, r, videoTrack.URL, func() (string, bool) {
			ctx2, cancel2 := context.WithTimeout(r.Context(), 45*time.Second)
			defer cancel2()
			fresh, _, err := a.ensurePlayCache(ctx2, req.VideoID, "https://www.youtube.com/watch?v="+req.VideoID, false)
			if err != nil {
				return "", false
			}
			item, ok := fresh.Meta.Items[req.QualityKey]
			if !ok {
				return "", false
			}
			vt, _, ok := chooseBestDashPair(ctx2, item.Label, req.Ext, item.Format, fresh.Meta.Audio, fresh.Meta.Formats)
			if !ok || strings.TrimSpace(vt.URL) == "" {
				return "", false
			}
			a.playCacheMu.Lock()
			if cached, ok := a.playCache[req.VideoID]; ok {
				if cached.Resolved.Video == nil {
					cached.Resolved.Video = map[string]mediaTrack{}
				}
				cached.Resolved.Video[req.QualityKey] = vt
				a.playCache[req.VideoID] = cached
			}
			a.playCacheMu.Unlock()
			return strings.TrimSpace(vt.URL), true
		})
		return
	}

	if _, ok, parseErr := parseInternalMediaPath(videoID, rest); !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": parseErr})
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "not found"})
}

// ---- from proxy_parse.go ----
func parseInternalProxyPath(videoID string, rest string, r *http.Request) (internalProxyReq, bool) {
	kind := strings.ToLower(strings.TrimSpace(rest))
	if strings.TrimSpace(videoID) == "" || kind != "proxy" {
		return internalProxyReq{}, false
	}
	raw := strings.TrimSpace(r.URL.Query().Get("u"))
	if raw == "" {
		return internalProxyReq{}, false
	}
	target, err := url.Parse(raw)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || strings.TrimSpace(target.Hostname()) == "" {
		return internalProxyReq{}, false
	}
	if !isAllowedVideoProxyHost(target.Hostname()) {
		return internalProxyReq{}, false
	}
	q := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("q")))
	if q == "" {
		return internalProxyReq{}, false
	}
	ua := strings.TrimSpace(r.URL.Query().Get("ua"))
	return internalProxyReq{VideoID: videoID, QualityKey: q, UA: ua, TargetURL: target.String()}, true
}

func parseInternalMediaPath(videoID string, rest string) (internalMediaReq, bool, string) {
	parts := strings.Split(strings.TrimSpace(rest), "/")
	if len(parts) != 2 {
		return internalMediaReq{}, false, "invalid route"
	}
	videoID = strings.TrimSpace(videoID)
	streamKind := strings.ToLower(strings.TrimSpace(parts[0]))
	name := strings.TrimSpace(parts[1])
	if videoID == "" || streamKind == "" || name == "" {
		return internalMediaReq{}, false, "invalid route"
	}
	dot := strings.LastIndex(name, ".")
	if dot <= 0 || dot >= len(name)-1 {
		return internalMediaReq{}, false, "invalid route"
	}
	base := strings.TrimSpace(name[:dot])
	ext := strings.ToLower(strings.TrimSpace(name[dot+1:]))
	if !isPathExtAllowed(ext) {
		return internalMediaReq{}, false, "invalid ext"
	}

	if strings.HasPrefix(base, "init-stream") {
		rep := strings.TrimSpace(strings.TrimPrefix(base, "init-stream"))
		if rep == "" {
			return internalMediaReq{}, false, "invalid init route"
		}
		if streamKind == "video" {
			q := qualityKeyFromPathSegment(rep)
			if q == "" {
				return internalMediaReq{}, false, "invalid quality"
			}
			return internalMediaReq{VideoID: videoID, QualityKey: q, Ext: ext, IsAudio: false, TemplateInit: true}, true, ""
		}
		if streamKind == "audio" {
			return internalMediaReq{VideoID: videoID, QualityKey: "AUDIO", AudioName: "main", Ext: ext, IsAudio: true, TemplateInit: true}, true, ""
		}
		return internalMediaReq{}, false, "invalid stream kind"
	}

	if strings.HasPrefix(base, "chunk-stream") {
		payload := strings.TrimSpace(strings.TrimPrefix(base, "chunk-stream"))
		dash := strings.LastIndex(payload, "-")
		if dash <= 0 || dash >= len(payload)-1 {
			return internalMediaReq{}, false, "invalid chunk route"
		}
		rep := strings.TrimSpace(payload[:dash])
		seqRaw := strings.TrimSpace(payload[dash+1:])
		seq, err := strconv.Atoi(seqRaw)
		if err != nil || seq < 1 {
			return internalMediaReq{}, false, "invalid chunk number"
		}
		if streamKind == "video" {
			q := qualityKeyFromPathSegment(rep)
			if q == "" {
				return internalMediaReq{}, false, "invalid quality"
			}
			return internalMediaReq{VideoID: videoID, QualityKey: q, Ext: ext, IsAudio: false, TemplateSeg: seq}, true, ""
		}
		if streamKind == "audio" {
			return internalMediaReq{VideoID: videoID, QualityKey: "AUDIO", AudioName: "main", Ext: ext, IsAudio: true, TemplateSeg: seq}, true, ""
		}
		return internalMediaReq{}, false, "invalid stream kind"
	}

	switch streamKind {
	case "video":
		q := qualityKeyFromPathSegment(base)
		if q == "" {
			return internalMediaReq{}, false, "invalid quality"
		}
		return internalMediaReq{VideoID: videoID, QualityKey: q, Ext: ext, IsAudio: false}, true, ""
	case "audio":
		aName := strings.ToLower(strings.TrimSpace(base))
		if aName == "" {
			return internalMediaReq{}, false, "invalid audio name"
		}
		return internalMediaReq{VideoID: videoID, QualityKey: "AUDIO", AudioName: aName, Ext: ext, IsAudio: true}, true, ""
	default:
		return internalMediaReq{}, false, "invalid stream kind"
	}
}

func parseClosedBytesRangeHeader(raw string) (int64, int64, bool) {
	value := strings.TrimSpace(strings.ToLower(raw))
	if !strings.HasPrefix(value, "bytes=") {
		return 0, 0, false
	}
	payload := strings.TrimSpace(strings.TrimPrefix(value, "bytes="))
	if strings.Contains(payload, ",") {
		return 0, 0, false
	}
	parts := strings.SplitN(payload, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return 0, 0, false
	}
	start, err1 := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	end, err2 := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err1 != nil || err2 != nil || end < start {
		return 0, 0, false
	}
	return start, end, true
}

func qualityPathSegment(label string) string {
	return strings.ToLower(strings.TrimSpace(label))
}

func qualityKeyFromPathSegment(seg string) string {
	value := strings.ToUpper(strings.TrimSpace(seg))
	switch value {
	case "4K", "4K60", "2K", "2K60", "1080P", "1080P60", "720P", "720P60", "480P", "360P", "240P", "144P":
		return value
	default:
		return ""
	}
}

// ---- from m3u8_proxy.go ----
func rewriteM3U8ToURLProxy(raw, baseURL, prefix string) string {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	for i := range lines {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			lines[i] = rewriteM3U8TagURIAttrsToURLProxy(line, baseURL, prefix)
			continue
		}
		abs, ok := resolveM3U8Ref(baseURL, trimmed)
		if !ok {
			continue
		}
		lines[i] = buildURLProxyPath(abs, prefix)
	}
	return strings.Join(lines, "\n")
}

func rewriteM3U8TagURIAttrsToURLProxy(line, baseURL, prefix string) string {
	upper := strings.ToUpper(line)
	idx := 0
	out := line
	for {
		p := strings.Index(upper[idx:], "URI=")
		if p < 0 {
			break
		}
		p += idx
		valStart := p + len("URI=")
		if valStart >= len(out) {
			break
		}
		quoted := false
		start := valStart
		end := valStart
		if out[valStart] == '"' {
			quoted = true
			start = valStart + 1
			end = start
			for end < len(out) && out[end] != '"' {
				end++
			}
		} else {
			for end < len(out) && out[end] != ',' {
				end++
			}
		}
		if start >= len(out) || end <= start {
			idx = valStart + 1
			continue
		}
		uriVal := strings.TrimSpace(out[start:end])
		abs, ok := resolveM3U8Ref(baseURL, uriVal)
		if !ok {
			idx = end + 1
			continue
		}
		rewritten := buildURLProxyPath(abs, prefix)
		if quoted {
			out = out[:start] + rewritten + out[end:]
			idx = start + len(rewritten) + 1
		} else {
			out = out[:start] + rewritten + out[end:]
			idx = start + len(rewritten)
		}
		upper = strings.ToUpper(out)
	}
	return out
}

func resolveM3U8Ref(baseURL, ref string) (string, bool) {
	v := strings.TrimSpace(ref)
	if v == "" {
		return "", false
	}
	if strings.HasPrefix(v, "data:") {
		return "", false
	}
	if strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://") {
		return v, true
	}
	if strings.TrimSpace(baseURL) == "" {
		return "", false
	}
	u, err := resolveRefURL(baseURL, v)
	if err != nil {
		return "", false
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(u)), "http://") && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(u)), "https://") {
		return "", false
	}
	return u, true
}

func buildURLProxyPath(absURL, prefix string) string {
	u := strings.TrimSpace(absURL)
	if u == "" {
		return ""
	}
	return withPathPrefix(prefix, "/proxy/url/"+url.PathEscape(u))
}

type internalProxyReq struct {
	VideoID    string
	QualityKey string
	UA         string
	TargetURL  string
}

type internalMediaReq struct {
	VideoID      string
	QualityKey   string
	AudioName    string
	Ext          string
	IsAudio      bool
	TemplateInit bool
	TemplateSeg  int
}

func resolveProxyTargetFromMedia(mediaURL, mediaRaw, currentTarget string) (string, bool) {
	normalized := strings.ReplaceAll(mediaRaw, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		abs, ok := resolveM3U8Ref(mediaURL, trimmed)
		if ok && strings.TrimSpace(abs) == strings.TrimSpace(currentTarget) {
			return abs, true
		}
	}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		abs, ok := resolveM3U8Ref(mediaURL, trimmed)
		if ok {
			return abs, true
		}
	}
	return "", false
}

func isAllowedVideoProxyHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return false
	}
	allowed := []string{
		"googlevideo.com",
		"youtube.com",
		"youtubei.googleapis.com",
		"ytimg.com",
		"googleapis.com",
		"gvt1.com",
	}
	for _, d := range allowed {
		if h == d || strings.HasSuffix(h, "."+d) {
			return true
		}
	}
	return false
}

func (a *app) fetchPlaylistText(ctx context.Context, playlistURL, userAgent string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSpace(playlistURL), nil)
	if err != nil {
		return "", errors.New("invalid playlist url")
	}
	if ua := strings.TrimSpace(userAgent); ua != "" {
		req.Header.Set("User-Agent", ua)
	}

	resp, err := a.upstreamClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("upstream http %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPlaylistSize+1))
	if err != nil {
		return "", err
	}
	if len(body) > maxPlaylistSize {
		return "", errors.New("playlist too large")
	}
	return string(body), nil
}

func (a *app) resolveMediaPlaylist(ctx context.Context, playlistURL, userAgent string) (string, string, error) {
	curURL := strings.TrimSpace(playlistURL)
	for depth := 0; depth < 4; depth++ {
		raw, err := a.fetchPlaylistText(ctx, curURL, userAgent)
		if err != nil {
			return "", "", err
		}
		if !isMasterPlaylist(raw) {
			return curURL, raw, nil
		}
		nextRef, err := pickMasterVariant(raw)
		if err != nil {
			return "", "", err
		}
		nextURL, err := resolveRefURL(curURL, nextRef)
		if err != nil {
			return "", "", err
		}
		curURL = nextURL
	}
	return "", "", errors.New("master playlist nesting too deep")
}

func isMasterPlaylist(raw string) bool {
	body := strings.ToUpper(strings.ReplaceAll(raw, "\r\n", "\n"))
	return strings.Contains(body, "#EXT-X-STREAM-INF")
}

func pickMasterVariant(raw string) (string, error) {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")

	type candidate struct {
		URI       string
		Bandwidth int
		Height    int
	}
	candidates := make([]candidate, 0)
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(strings.ToUpper(line), "#EXT-X-STREAM-INF:") {
			continue
		}
		attr := parseM3U8AttrList(line)
		uri := ""
		for j := i + 1; j < len(lines); j++ {
			next := strings.TrimSpace(lines[j])
			if next == "" {
				continue
			}
			if strings.HasPrefix(next, "#") {
				break
			}
			uri = next
			break
		}
		if uri == "" {
			continue
		}
		candidates = append(candidates, candidate{
			URI:       uri,
			Bandwidth: parsePositiveInt(attr["BANDWIDTH"]),
			Height:    parseResolutionHeight(attr["RESOLUTION"]),
		})
	}
	if len(candidates) == 0 {
		return "", errors.New("master playlist has no variant uri")
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Height != candidates[j].Height {
			return candidates[i].Height > candidates[j].Height
		}
		return candidates[i].Bandwidth > candidates[j].Bandwidth
	})
	return candidates[0].URI, nil
}

func parseM3U8AttrList(line string) map[string]string {
	pos := strings.Index(line, ":")
	if pos < 0 || pos+1 >= len(line) {
		return map[string]string{}
	}
	raw := line[pos+1:]
	parts := splitAttrCSV(raw)
	out := make(map[string]string, len(parts))
	for _, p := range parts {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.ToUpper(strings.TrimSpace(kv[0]))
		v := strings.Trim(strings.TrimSpace(kv[1]), `"`)
		out[k] = v
	}
	return out
}

func splitAttrCSV(raw string) []string {
	out := make([]string, 0)
	var b strings.Builder
	inQuote := false
	for _, ch := range raw {
		if ch == '"' {
			inQuote = !inQuote
			b.WriteRune(ch)
			continue
		}
		if ch == ',' && !inQuote {
			item := strings.TrimSpace(b.String())
			if item != "" {
				out = append(out, item)
			}
			b.Reset()
			continue
		}
		b.WriteRune(ch)
	}
	item := strings.TrimSpace(b.String())
	if item != "" {
		out = append(out, item)
	}
	return out
}

func parsePositiveInt(raw string) int {
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || v < 0 {
		return 0
	}
	return v
}

func parseResolutionHeight(raw string) int {
	value := strings.ToLower(strings.TrimSpace(raw))
	parts := strings.Split(value, "x")
	if len(parts) != 2 {
		return 0
	}
	return parsePositiveInt(parts[1])
}

func resolveRefURL(baseURL, ref string) (string, error) {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", errors.New("invalid base url")
	}
	parsedRef, err := url.Parse(strings.TrimSpace(ref))
	if err != nil {
		return "", errors.New("invalid ref url")
	}
	return base.ResolveReference(parsedRef).String(), nil
}
