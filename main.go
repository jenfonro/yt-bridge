package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultAddr        = ":28080"
	resolveTimeout     = 60 * time.Second
	defaultChromeDebug = "http://127.0.0.1:9223"
	defaultBasePath    = ""
	ytdlpBinaryName    = "yt-dlp"
	cookiesDirName     = "cookies"
	probeStepBytes     = 512 * 1024
	probeMaxBytes      = 8 * 1024 * 1024
	maxPlaylistSize    = 8 << 20
	playCacheTTL       = 1 * time.Hour
)

type app struct {
	ytdlpPath      string
	cookiesPath    string
	basePath       string
	upstreamClient *http.Client
	configPath     string

	categoryMu          sync.Mutex
	categoryPage        int
	categoryPageSize    int
	categoryReady       bool
	categoryCacheMu     sync.Mutex
	categoryCacheUntil  time.Time
	categoryCacheByPage map[int][]map[string]string

	playCacheMu sync.Mutex
	playCache   map[string]playCacheEntry

	segTraceMu sync.Mutex
	segTrace   map[string]int
}

type playMetaCache struct {
	WatchURL    string
	DurationSec float64
	Items       map[string]playQualityItem
	Formats     []ytdlpFormat
	Audio       ytdlpFormat
}

type playResolvedCache struct {
	Video map[string]mediaTrack
	Audio map[string]mediaTrack
	HLS   map[string]hlsResolved
}

// playCacheEntry caches quality map for /proxy lazy resolve.
type playCacheEntry struct {
	Meta      playMetaCache
	Resolved  playResolvedCache
	ExpiresAt time.Time
}

type hlsResolved struct {
	MediaURL string
	Raw      string
	UA       string
}

type playQualityItem struct {
	Label  string
	Mode   string // mpd | m3u8
	Format ytdlpFormat
}

type mediaTrack struct {
	URL       string
	Bandwidth int
	Width     int
	Height    int
	FPS       float64
	Codec     string
	Ext       string
	MimeType  string

	InitStart  int64
	InitEnd    int64
	IndexStart int64
	IndexEnd   int64
	Timescale  uint32
	Segments   []dashSegment
}

type mediaTrackIndex struct {
	InitStart  int64
	InitEnd    int64
	IndexStart int64
	IndexEnd   int64
	Timescale  uint32
	Segments   []dashSegment
}

type dashSegment struct {
	Start    int64
	End      int64
	Time     uint64
	Duration uint32
}

type categoryReq struct {
	Filter  bool           `json:"filter"`
	Filters map[string]any `json:"filters"`
	ID      string         `json:"id"`
	Page    int            `json:"page"`
}

type detailReq struct {
	ID string `json:"id"`
}

type playReq struct {
	Flag string `json:"flag"`
	ID   string `json:"id"`
}

type ytdlpInfo struct {
	Formats  []ytdlpFormat `json:"formats"`
	Duration float64       `json:"duration"`
}

type ytdlpFormat struct {
	FormatID string  `json:"format_id"`
	Protocol string  `json:"protocol"`
	Ext      string  `json:"ext"`
	URL      string  `json:"url"`
	VCodec   string  `json:"vcodec"`
	ACodec   string  `json:"acodec"`
	Width    int     `json:"width"`
	Height   int     `json:"height"`
	TBR      float64 `json:"tbr"`
	ABR      float64 `json:"abr"`
	FPS      float64 `json:"fps"`
}

type runtimeConfig struct {
	ServerAddr     string `json:"server_addr"`
	ChromeDebugURL string `json:"chrome_debug_url"`
	ProbeTimeoutMS int    `json:"probe_timeout_ms"`
	BasePath       string `json:"base_path"`
}

func main() {
	runtimeRoot, err := os.Getwd()
	if err != nil || strings.TrimSpace(runtimeRoot) == "" {
		log.Fatal("runtime root not resolved from current working directory")
	}

	ytdlpPath := filepath.Join(runtimeRoot, ytdlpBinaryName)
	if !fileExists(ytdlpPath) {
		log.Fatalf("yt-dlp binary not found: %s", ytdlpPath)
	}
	cookiesPath, err := resolveCookiesTxtPath(runtimeRoot)
	if err != nil {
		log.Fatal(err)
	}

	cfg, cfgPath, created, err := ensureConfigExistsAndPatched(runtimeRoot)
	if err != nil {
		log.Fatal(err)
	}
	if created {
		log.Printf("[yt-bridge] config initialized: %s", cfgPath)
	}

	a := &app{
		ytdlpPath:           ytdlpPath,
		cookiesPath:         cookiesPath,
		basePath:            normalizePathPrefix(cfg.BasePath),
		upstreamClient:      newUpstreamClient(),
		configPath:          cfgPath,
		categoryCacheByPage: make(map[int][]map[string]string),
		playCache:           make(map[string]playCacheEntry),
		segTrace:            make(map[string]int),
	}

	mux := http.NewServeMux()
	registerRoutes(mux, a)

	addr := strings.TrimSpace(cfg.ServerAddr)
	h := withBasePath(mux, a.basePath)
	log.Printf("[yt-bridge] start addr=%s yt-dlp=%s cookies=%s config=%s", addr, a.ytdlpPath, a.cookiesPath, a.configPath)
	if err := http.ListenAndServe(addr, withCORS(loggingMiddleware(h))); err != nil {
		log.Fatal(err)
	}
}

func (a *app) handleHome(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"class": []map[string]any{
			{"type_id": "1", "type_name": "为你推荐"},
		},
	})
}

func (a *app) handleCategory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

	var in categoryReq
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid request body"})
		return
	}
	if strings.TrimSpace(in.ID) != "1" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid category id"})
		return
	}
	page := in.Page
	if page <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid page"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	now := time.Now()
	base := externalBaseURL(r)
	prefix := a.basePath

	a.categoryCacheMu.Lock()
	cacheValid := now.Before(a.categoryCacheUntil)
	if cacheValid {
		if cached, ok := a.categoryCacheByPage[page]; ok {
			list := cloneVodList(cached)
			a.categoryCacheMu.Unlock()
			applyImageProxyBase(list, base, prefix)
			writeJSON(w, http.StatusOK, map[string]any{"list": list})
			return
		}
	}
	a.categoryCacheMu.Unlock()

	// 缓存已过期时，只允许重建 1~5 页窗口；>5 直接空。
	if !cacheValid && page > 5 {
		writeJSON(w, http.StatusOK, map[string]any{"list": []map[string]string{}})
		return
	}

	var (
		list []map[string]string
		err  error
	)

	if !cacheValid && page <= 5 {
		fresh := make(map[int][]map[string]string, page)
		for i := 1; i <= page; i++ {
			list, err = a.fetchYouTubeHomeVodListFromChromeDebug(ctx, i)
			if err != nil {
				writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
				return
			}
			fresh[i] = cloneVodList(list)
		}
		a.categoryCacheMu.Lock()
		a.categoryCacheByPage = fresh
		a.categoryCacheUntil = time.Now().Add(5 * time.Minute)
		a.categoryCacheMu.Unlock()
	} else {
		list, err = a.fetchYouTubeHomeVodListFromChromeDebug(ctx, page)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		a.categoryCacheMu.Lock()
		if a.categoryCacheByPage == nil {
			a.categoryCacheByPage = make(map[int][]map[string]string)
		}
		a.categoryCacheByPage[page] = cloneVodList(list)
		if !now.Before(a.categoryCacheUntil) {
			a.categoryCacheUntil = time.Now().Add(5 * time.Minute)
		}
		a.categoryCacheMu.Unlock()
	}

	applyImageProxyBase(list, base, prefix)
	writeJSON(w, http.StatusOK, map[string]any{"list": list})
}

func (a *app) handleDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

	var in detailReq
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid request body"})
		return
	}
	vodID := strings.TrimSpace(in.ID)
	if !strings.HasPrefix(vodID, "watch?") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid id, must start with watch?"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	base := externalBaseURL(r)
	prefix := a.basePath
	detail, err := a.fetchYouTubeDetailFromChromeDebug(ctx, vodID, base, prefix)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}

	if pic := buildImagePathByVodID(base, prefix, strings.TrimSpace(detail["vod_id"])); pic != "" {
		detail["vod_pic"] = pic
	}

	writeJSON(w, http.StatusOK, map[string]any{"list": []map[string]string{detail}})
}

func (a *app) handlePlay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

	var in playReq
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid request body"})
		return
	}
	if strings.TrimSpace(in.Flag) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "missing flag"})
		return
	}
	rawID := strings.TrimSpace(in.ID)
	watchID, ok := normalizePlayWatchID(rawID)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "invalid id, must contain watch?"})
		return
	}
	watchURL := "https://www.youtube.com/" + watchID
	videoID, err := extractYouTubeVideoIDFromWatchID(watchID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	entry, _, err := a.ensurePlayCache(ctx, videoID, watchURL, false)
	if err != nil {
		writeJSON(w, statusForResolveErr(err), map[string]any{"ok": false, "message": err.Error()})
		return
	}

	base := externalBaseURL(r)
	prefix := a.basePath
	sorted := sortPlayItems(entry.Meta.Items)
	// Keep cat API play payload compatible: [label1, url1, label2, url2, ...]
	urls := make([]any, 0, len(sorted)*2)
	for _, it := range sorted {
		label := strings.TrimSpace(it.Label)
		if label == "" {
			continue
		}
		p := withPathPrefix(prefix, "/proxy/"+url.PathEscape(videoID)+"/manifest/"+url.PathEscape(qualityPathSegment(label))+"."+it.Mode)
		u := p
		if base != "" {
			u = base + p
		}
		urls = append(urls, label, u)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "parse": 0, "url": urls})
}

func defaultRuntimeConfig() runtimeConfig {
	return runtimeConfig{
		ServerAddr:     defaultAddr,
		ChromeDebugURL: defaultChromeDebug,
		ProbeTimeoutMS: 5000,
		BasePath:       defaultBasePath,
	}
}

func mergeRuntimeConfig(raw runtimeConfig) runtimeConfig {
	cfg := raw
	def := defaultRuntimeConfig()
	if strings.TrimSpace(cfg.ServerAddr) == "" {
		cfg.ServerAddr = def.ServerAddr
	}
	if strings.TrimSpace(cfg.ChromeDebugURL) == "" {
		cfg.ChromeDebugURL = def.ChromeDebugURL
	}
	if cfg.ProbeTimeoutMS <= 0 {
		cfg.ProbeTimeoutMS = def.ProbeTimeoutMS
	}
	cfg.BasePath = normalizePathPrefix(cfg.BasePath)
	return cfg
}

func ensureConfigExistsAndPatched(runtimeRoot string) (runtimeConfig, string, bool, error) {
	cfgPath := filepath.Join(runtimeRoot, "config.json")
	if !fileExists(cfgPath) {
		cfg := defaultRuntimeConfig()
		bs, _ := json.MarshalIndent(cfg, "", "  ")
		bs = append(bs, '\n')
		if err := os.WriteFile(cfgPath, bs, 0o644); err != nil {
			return runtimeConfig{}, cfgPath, false, err
		}
		return cfg, cfgPath, true, nil
	}

	bs, err := os.ReadFile(cfgPath)
	if err != nil {
		return runtimeConfig{}, cfgPath, false, err
	}
	var raw runtimeConfig
	if err := json.Unmarshal(bs, &raw); err != nil {
		return runtimeConfig{}, cfgPath, false, fmt.Errorf("invalid config.json: %w", err)
	}
	cfg := mergeRuntimeConfig(raw)
	if cfg != raw {
		patched, _ := json.MarshalIndent(cfg, "", "  ")
		patched = append(patched, '\n')
		if err := os.WriteFile(cfgPath, patched, 0o644); err != nil {
			return runtimeConfig{}, cfgPath, false, err
		}
	}
	return cfg, cfgPath, false, nil
}

func (a *app) loadRuntimeConfig() (runtimeConfig, error) {
	if strings.TrimSpace(a.configPath) == "" {
		return runtimeConfig{}, errors.New("config path is empty")
	}
	bs, err := os.ReadFile(a.configPath)
	if err != nil {
		return runtimeConfig{}, err
	}
	var raw runtimeConfig
	if err := json.Unmarshal(bs, &raw); err != nil {
		return runtimeConfig{}, fmt.Errorf("invalid config.json: %w", err)
	}
	cfg := mergeRuntimeConfig(raw)
	if cfg != raw {
		patched, _ := json.MarshalIndent(cfg, "", "  ")
		patched = append(patched, '\n')
		_ = os.WriteFile(a.configPath, patched, 0o644)
	}
	return cfg, nil
}

func statusForResolveErr(err error) int {
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if msg == "" {
		return http.StatusBadGateway
	}
	if strings.Contains(msg, "video unavailable") || strings.Contains(msg, "private video") || strings.Contains(msg, "sign in") || strings.Contains(msg, "no such video") || strings.Contains(msg, "not available") {
		return http.StatusNotFound
	}
	return http.StatusBadGateway
}

func fileExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	st, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !st.IsDir()
}

func externalBaseURL(r *http.Request) string {
	host := firstHeaderToken(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = strings.TrimSpace(r.Host)
	}
	if host == "" {
		return ""
	}

	scheme := firstHeaderToken(r.Header.Get("X-Forwarded-Proto"))
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	return scheme + "://" + host
}

func normalizePathPrefix(raw string) string {
	p := strings.TrimSpace(raw)
	if p == "" || p == "/" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = strings.TrimRight(p, "/")
	if p == "" || p == "/" {
		return ""
	}
	return p
}

func withPathPrefix(prefix, path string) string {
	p := normalizePathPrefix(prefix)
	v := strings.TrimSpace(path)
	if v == "" {
		return p
	}
	if !strings.HasPrefix(v, "/") {
		v = "/" + v
	}
	if p == "" {
		return v
	}
	return p + v
}

func withBasePath(next http.Handler, basePath string) http.Handler {
	prefix := normalizePathPrefix(basePath)
	if prefix == "" {
		return next
	}
	want := prefix + "/"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := ""
		if r != nil && r.URL != nil {
			path = r.URL.Path
		}
		if !strings.HasPrefix(path, want) {
			http.NotFound(w, r)
			return
		}
		nr := r.Clone(r.Context())
		nr.URL = cloneURL(r.URL)
		nr.URL.Path = strings.TrimPrefix(path, prefix)
		if nr.URL.Path == "" {
			nr.URL.Path = "/"
		}
		if raw := strings.TrimSpace(r.URL.RawPath); raw != "" {
			nr.URL.RawPath = strings.TrimPrefix(raw, prefix)
		}
		next.ServeHTTP(w, nr)
	})
}

func cloneURL(u *url.URL) *url.URL {
	if u == nil {
		return &url.URL{}
	}
	cp := *u
	return &cp
}

func resolveCookiesTxtPath(runtimeRoot string) (string, error) {
	cookiesDir := filepath.Join(runtimeRoot, cookiesDirName)
	entries, err := os.ReadDir(cookiesDir)
	if err != nil {
		return "", fmt.Errorf("cookies dir not available: %s", cookiesDir)
	}
	candidates := make([]string, 0)
	for _, e := range entries {
		if e == nil || e.IsDir() {
			continue
		}
		name := strings.TrimSpace(e.Name())
		if !strings.HasSuffix(strings.ToLower(name), ".txt") {
			continue
		}
		full := filepath.Join(cookiesDir, name)
		if fileExists(full) {
			candidates = append(candidates, full)
		}
	}
	if len(candidates) == 0 {
		return "", errors.New("cookies txt file not found in cookies/")
	}
	sort.Strings(candidates)
	return candidates[0], nil
}

func firstHeaderToken(raw string) string {
	return strings.TrimSpace(strings.Split(raw, ",")[0])
}
