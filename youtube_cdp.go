package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"nhooyr.io/websocket"
)

func (a *app) fetchYouTubeHomeVodListFromChromeDebug(ctx context.Context, page int) ([]map[string]string, error) {
	a.categoryMu.Lock()
	defer a.categoryMu.Unlock()

	cfg, err := a.loadRuntimeConfig()
	if err != nil {
		return nil, err
	}
	chromeDebugURL := strings.TrimSpace(cfg.ChromeDebugURL)
	jsonListURL := strings.TrimRight(chromeDebugURL, "/") + "/json/list"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, jsonListURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.upstreamClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("chrome debug /json/list status=%d", resp.StatusCode)
	}

	var tabs []map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&tabs); err != nil {
		return nil, err
	}

	wsURL := ""
	for _, t := range tabs {
		if strings.TrimSpace(anyToString(t["type"])) != "page" {
			continue
		}
		tabURL := strings.TrimSpace(anyToString(t["url"]))
		if !strings.Contains(tabURL, "youtube.com") {
			continue
		}
		if strings.Contains(tabURL, "youtube.com/watch") {
			continue
		}
		wsURL = strings.TrimSpace(anyToString(t["webSocketDebuggerUrl"]))
		if wsURL != "" {
			break
		}
	}
	if wsURL == "" {
		for _, t := range tabs {
			if strings.TrimSpace(anyToString(t["type"])) != "page" {
				continue
			}
			tabURL := strings.TrimSpace(anyToString(t["url"]))
			if !strings.Contains(tabURL, "youtube.com") {
				continue
			}
			wsURL = strings.TrimSpace(anyToString(t["webSocketDebuggerUrl"]))
			if wsURL != "" {
				break
			}
		}
	}
	if wsURL == "" {
		newTabURL := strings.TrimRight(chromeDebugURL, "/") + "/json/new?" + url.QueryEscape("https://www.youtube.com/?app=desktop&hl=zh-CN")
		newReq, err := http.NewRequestWithContext(ctx, http.MethodPut, newTabURL, nil)
		if err != nil {
			return nil, err
		}
		newResp, err := a.upstreamClient.Do(newReq)
		if err != nil {
			return nil, err
		}
		defer newResp.Body.Close()
		if newResp.StatusCode < 200 || newResp.StatusCode >= 300 {
			return nil, fmt.Errorf("chrome debug /json/new status=%d", newResp.StatusCode)
		}
		var newTab map[string]any
		if err := json.NewDecoder(io.LimitReader(newResp.Body, 1<<20)).Decode(&newTab); err != nil {
			return nil, err
		}
		wsURL = strings.TrimSpace(anyToString(newTab["webSocketDebuggerUrl"]))
		if wsURL == "" {
			return nil, errors.New("no youtube page websocket found from chrome debug")
		}
	}

	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, err
	}
	defer c.Close(websocket.StatusNormalClosure, "done")

	nextID := 0
	call := func(method string, params map[string]any) (map[string]any, error) {
		nextID++
		id := nextID
		reqMsg := map[string]any{"id": id, "method": method}
		if params != nil {
			reqMsg["params"] = params
		}
		bs, _ := json.Marshal(reqMsg)
		if err := c.Write(ctx, websocket.MessageText, bs); err != nil {
			return nil, err
		}
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return nil, err
			}
			var msg map[string]any
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if int(toFloat(msg["id"])) != id {
				continue
			}
			if ev, ok := msg["error"].(map[string]any); ok && ev != nil {
				return nil, errors.New(anyToString(ev["message"]))
			}
			res, _ := msg["result"].(map[string]any)
			if res == nil {
				res = map[string]any{}
			}
			return res, nil
		}
	}

	if _, err := call("Runtime.enable", nil); err != nil {
		return nil, err
	}
	if _, err := call("Page.enable", nil); err != nil {
		return nil, err
	}

	getWatchLinkCount := func() int {
		expr := `(() => document.querySelectorAll('a[href*="/watch?"]').length)()`
		res, err := call("Runtime.evaluate", map[string]any{"expression": expr, "returnByValue": true})
		if err != nil {
			return 0
		}
		if ro, _ := res["result"].(map[string]any); ro != nil {
			return int(toFloat(ro["value"]))
		}
		return 0
	}

	waitWatchLinks := func(minCount int, maxAttempts int) {
		if minCount <= 0 {
			minCount = 1
		}
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		for i := 0; i < maxAttempts; i++ {
			if getWatchLinkCount() >= minCount {
				return
			}
			_, _ = call("Runtime.evaluate", map[string]any{"expression": `new Promise(r=>setTimeout(r,200))`, "awaitPromise": true, "returnByValue": true})
		}
	}

	goHome := func() error {
		if _, err := call("Page.navigate", map[string]any{"url": "https://www.youtube.com/?app=desktop&hl=zh-CN"}); err != nil {
			return err
		}
		waitWatchLinks(1, 34)
		a.categoryPage = 1
		a.categoryPageSize = 0
		a.categoryReady = true
		return nil
	}

	pressEnd := func(prevCount int) int {
		_, _ = call("Input.dispatchKeyEvent", map[string]any{"type": "keyDown", "key": "End", "code": "End", "windowsVirtualKeyCode": 35, "nativeVirtualKeyCode": 35})
		_, _ = call("Input.dispatchKeyEvent", map[string]any{"type": "keyUp", "key": "End", "code": "End", "windowsVirtualKeyCode": 35, "nativeVirtualKeyCode": 35})
		deadline := time.Now().Add(3 * time.Second)
		last := prevCount
		for time.Now().Before(deadline) {
			_, _ = call("Runtime.evaluate", map[string]any{"expression": `new Promise(r=>setTimeout(r,200))`, "awaitPromise": true, "returnByValue": true})
			curr := getWatchLinkCount()
			if curr > prevCount {
				return curr
			}
			last = curr
		}
		return last
	}

	targetPage := page
	if targetPage <= 1 {
		targetPage = 1
		if err := goHome(); err != nil {
			return nil, err
		}
	} else {
		if !a.categoryReady || a.categoryPage <= 0 || targetPage < a.categoryPage {
			if err := goHome(); err != nil {
				return nil, err
			}
		}
		for a.categoryPage < targetPage {
			prev := getWatchLinkCount()
			pressEnd(prev)
			a.categoryPage++
		}
	}

	expr := `(() => {
	  const out = [];

	  const normRel = (href) => {
	    let abs = String(href || '').trim();
	    if (!abs) return '';
	    if (abs.startsWith('/')) abs = location.origin + abs;
	    if (!abs.includes('/watch?')) return '';
	    const rel = abs.replace(/^https?:\/\/www\.youtube\.com\//, '').replace(/^\//, '');
	    return rel.startsWith('watch?') ? rel : '';
	  };

	  const cleanTitle = (name) => String(name || '').replace(/\s+/g, ' ').trim();

	  const addItem = (href, title, pic) => {
	    const rel = normRel(href);
	    if (!rel) return;
	    const name = cleanTitle(title);
	    if (!name) return;
	    let image = String(pic || '').trim();
	    if (image.startsWith('//')) image = 'https:' + image;
	    out.push({ vod_id: rel, vod_name: name, vod_pic: image, vod_remarks: '' });
	  };

	  // A) DOM 主提取（链接取 a，标题优先取 a 下级 span；仅按广告容器过滤）
	  const cards = Array.from(document.querySelectorAll('ytd-rich-item-renderer[rendered-from-rich-grid], ytd-rich-item-renderer, ytd-rich-grid-media, ytd-video-renderer, ytd-grid-video-renderer, ytd-compact-video-renderer'));
	  for (const card of cards) {
	    // 剔除短视频栏目（rich shelf）中的卡片
	    if (card.closest('ytd-rich-shelf-renderer')) continue;

	    // 仅过滤明确广告容器
	    const adBadge = card.querySelector('ytd-display-ad-renderer, ytd-ad-slot-renderer, ytd-promoted-video-renderer, ad-badge-view-model, .ytBadgeShapeAd, .ytwFeedAdMetadataViewModelHostMetadataAdBadgeDetailsLineContainerStyleStandard');
	    if (adBadge) continue;

	    const titleA = card.querySelector('h3 a.ytLockupMetadataViewModelTitle[href*="/watch?"], h3 a[href*="/watch?"]');
	    const linkA = titleA || card.querySelector('a[href*="/watch?"]');
	    if (!linkA) continue;

	    const spanTitle = titleA ? titleA.querySelector('span') : null;
	    const title = cleanTitle(
	      (spanTitle && spanTitle.textContent) ||
	      (titleA && titleA.getAttribute('title')) ||
	      (titleA && titleA.textContent) ||
	      linkA.getAttribute('title') ||
	      linkA.textContent ||
	      linkA.getAttribute('aria-label') ||
	      ''
	    );
	    if (!title) continue;

	    const img = card.querySelector('img');
	    const pic = img ? (img.getAttribute('src') || img.getAttribute('data-src') || img.getAttribute('data-thumb') || '') : '';
	    addItem(linkA.getAttribute('href') || linkA.href || '', title, pic);
	  }

	  // B) 轻兜底（A 结构没命中时，仍按 a+span/title/text/aria 取标题）
	  if (out.length < 12) {
	    const links = Array.from(document.querySelectorAll('a[href*="/watch?"]'));
	    for (const a of links) {
	      if (a.closest('ytd-rich-shelf-renderer')) {
	        continue;
	      }
	      if (a.closest('ytd-display-ad-renderer, ytd-ad-slot-renderer, ytd-promoted-video-renderer, ad-badge-view-model, .ytBadgeShapeAd, .ytwFeedAdMetadataViewModelHostMetadataAdBadgeDetailsLineContainerStyleStandard')) {
	        continue;
	      }
	      const span = a.querySelector('span');
	      const title = cleanTitle(
	        (span && span.textContent) ||
	        a.getAttribute('title') ||
	        a.textContent ||
	        a.getAttribute('aria-label') ||
	        ''
	      );
	      if (!title) continue;
	      const card = a.closest('ytd-rich-grid-media, ytd-rich-item-renderer, ytd-video-renderer, ytd-grid-video-renderer, ytd-compact-video-renderer') || a.parentElement;
	      const img = card ? card.querySelector('img') : null;
	      const pic = img ? (img.getAttribute('src') || img.getAttribute('data-src') || img.getAttribute('data-thumb') || '') : '';
	      addItem(a.getAttribute('href') || a.href || '', title, pic);
	    }
	  }

	  return out;
	})()`

	parseList := func(res map[string]any) []map[string]string {
		resultObj, _ := res["result"].(map[string]any)
		if resultObj == nil {
			return []map[string]string{}
		}
		val, _ := resultObj["value"].([]any)
		outList := make([]map[string]string, 0, len(val))
		for _, it := range val {
			m, _ := it.(map[string]any)
			if m == nil {
				continue
			}
			vodID := strings.TrimSpace(anyToString(m["vod_id"]))
			vodName := strings.TrimSpace(anyToString(m["vod_name"]))
			if vodID == "" || vodName == "" {
				continue
			}
			outList = append(outList, map[string]string{
				"vod_id":      vodID,
				"vod_name":    vodName,
				"vod_pic":     strings.TrimSpace(anyToString(m["vod_pic"])),
				"vod_remarks": "",
			})
		}
		return outList
	}

	fetchList := func() ([]map[string]string, error) {
		res, err := call("Runtime.evaluate", map[string]any{"expression": expr, "returnByValue": true})
		if err != nil {
			return nil, err
		}
		return parseList(res), nil
	}

	out, err := fetchList()
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		waitWatchLinks(1, 20)
		if out2, err2 := fetchList(); err2 == nil {
			out = out2
		}
	}

	// 分页窗口：第一页实际解析多少就多少；后续页按第一页实际数量偏移
	if targetPage == 1 {
		a.categoryPageSize = len(out)
		return out, nil
	}

	if a.categoryPageSize <= 0 {
		a.categoryPageSize = len(out)
		if a.categoryPageSize <= 0 {
			return []map[string]string{}, nil
		}
	}

	// 对翻页抓取做兜底：若当前列表长度不足覆盖目标页窗口，继续向下滚动并重取。
	needCount := targetPage * a.categoryPageSize
	for i := 0; i < 2 && len(out) < needCount; i++ {
		prev := getWatchLinkCount()
		curr := pressEnd(prev)
		a.categoryPage++
		if curr <= prev {
			break
		}
		if out2, err2 := fetchList(); err2 == nil {
			out = out2
		}
	}

	start := (targetPage - 1) * a.categoryPageSize
	if start >= len(out) {
		return []map[string]string{}, nil
	}
	end := start + a.categoryPageSize
	if end > len(out) {
		end = len(out)
	}
	return out[start:end], nil
}

func (a *app) fetchYouTubeDetailFromChromeDebug(ctx context.Context, vodID string, imageBase string, imagePrefix string) (map[string]string, error) {
	cfg, err := a.loadRuntimeConfig()
	if err != nil {
		return nil, err
	}
	chromeDebugURL := strings.TrimSpace(cfg.ChromeDebugURL)

	watchRel := strings.TrimSpace(vodID)
	if !strings.HasPrefix(watchRel, "watch?") {
		if strings.Contains(watchRel, "youtube.com/watch?") {
			u, _ := url.Parse(watchRel)
			watchRel = strings.TrimPrefix(u.RequestURI(), "/")
		}
	}
	if !strings.HasPrefix(watchRel, "watch?") {
		return nil, errors.New("invalid detail id, expect watch?... ")
	}
	watchURL := "https://www.youtube.com/" + watchRel

	newTabURL := strings.TrimRight(chromeDebugURL, "/") + "/json/new?" + url.QueryEscape("about:blank")
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, newTabURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.upstreamClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("chrome debug /json/new status=%d", resp.StatusCode)
	}

	var tab map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tab); err != nil {
		return nil, err
	}
	wsURL := strings.TrimSpace(anyToString(tab["webSocketDebuggerUrl"]))
	tabID := strings.TrimSpace(anyToString(tab["id"]))
	if wsURL == "" || tabID == "" {
		return nil, errors.New("create detail tab failed: missing ws/id")
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		closeURL := strings.TrimRight(chromeDebugURL, "/") + "/json/close/" + url.PathEscape(tabID)
		closeReq, err := http.NewRequestWithContext(closeCtx, http.MethodGet, closeURL, nil)
		if err != nil {
			return
		}
		closeResp, err := a.upstreamClient.Do(closeReq)
		if err == nil && closeResp != nil {
			closeResp.Body.Close()
		}
	}()

	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, err
	}
	defer c.Close(websocket.StatusNormalClosure, "done")

	nextID := 0
	call := func(method string, params map[string]any) (map[string]any, error) {
		nextID++
		id := nextID
		reqMsg := map[string]any{"id": id, "method": method}
		if params != nil {
			reqMsg["params"] = params
		}
		bs, _ := json.Marshal(reqMsg)
		if err := c.Write(ctx, websocket.MessageText, bs); err != nil {
			return nil, err
		}
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return nil, err
			}
			var msg map[string]any
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if int(toFloat(msg["id"])) != id {
				continue
			}
			if ev, ok := msg["error"].(map[string]any); ok && ev != nil {
				return nil, errors.New(anyToString(ev["message"]))
			}
			res, _ := msg["result"].(map[string]any)
			if res == nil {
				res = map[string]any{}
			}
			return res, nil
		}
	}

	if _, err := call("Runtime.enable", nil); err != nil {
		return nil, err
	}
	if _, err := call("Page.enable", nil); err != nil {
		return nil, err
	}

	if _, err := call("Page.navigate", map[string]any{"url": watchURL}); err != nil {
		return nil, err
	}

	expr := `(() => {
	  const normRel = (href) => {
	    let abs = String(href || '').trim();
	    if (!abs) return '';
	    if (abs.startsWith('/')) abs = location.origin + abs;
	    if (!abs.includes('/watch?')) return '';
	    const rel = abs.replace(/^https?:\/\/www\.youtube\.com\//, '').replace(/^\//, '');
	    return rel.startsWith('watch?') ? rel : '';
	  };
	  const clean = (s) => String(s || '').replace(/\s+/g, ' ').trim();
	  const readPicFromCard = (el) => {
	    if (!el) return '';
	    const img = el.querySelector('img');
	    if (!img) return '';
	    return clean(img.getAttribute('src') || img.getAttribute('data-src') || img.getAttribute('data-thumb') || '');
	  };
	  const title = clean((document.querySelector('#title h1') || document.querySelector('h1 yt-formatted-string') || {}).textContent || document.title || '');
	  const author = clean((document.querySelector('#text a') || document.querySelector('ytd-channel-name #text a') || {}).textContent || '');
	  const player = ((window || {}).ytInitialPlayerResponse || {});
	  const videoDetails = (player.videoDetails || {});
	  const videoId = clean(videoDetails.videoId || new URLSearchParams(location.search).get('v') || '');
	  const lengthSeconds = Number(videoDetails.lengthSeconds || 0);
	  const fmtDuration = (sec) => {
	    const s = Math.max(0, Number(sec) || 0);
	    const h = Math.floor(s / 3600);
	    const m = Math.floor((s % 3600) / 60);
	    const ss = Math.floor(s % 60);
	    const pad = (n) => String(n).padStart(2, '0');
	    return h > 0 ? (String(h) + ':' + pad(m) + ':' + pad(ss)) : (String(m) + ':' + pad(ss));
	  };
	  const mainDuration = lengthSeconds > 0 ? fmtDuration(lengthSeconds) : '';
	  const playerThumbs = (videoDetails.thumbnail || {});
	  const thumbs = (playerThumbs && Array.isArray(playerThumbs.thumbnails)) ? playerThumbs.thumbnails : [];
	  const imageSrcHref = clean(((document.querySelector('link[rel="image_src"]') || {}).href || ''));
	  const cover = clean(
	    imageSrcHref ||
	    (videoId ? ('https://i.ytimg.com/vi/' + videoId + '/maxresdefault.jpg') : '') ||
	    (videoId ? ('https://i.ytimg.com/vi/' + videoId + '/hqdefault.jpg') : '') ||
	    (thumbs.length ? (thumbs[thumbs.length - 1].url || '') : '') ||
	    readPicFromCard(document)
	  );
	
	  const rec = [];
	  const recNodes = document.querySelectorAll('div#contents > yt-lockup-view-model');
	  for (const n of recNodes) {
	    // 对齐推荐标题路径：//*[@id="contents"]/yt-lockup-view-model[X]/.../h3/a/span
	    const titleA = n.querySelector('yt-lockup-metadata-view-model h3 a[href*="/watch?"]');
	    if (!titleA) continue;
	    const id = normRel(titleA.getAttribute('href') || titleA.href || '');
	    if (!id) continue;
	    const titleSpan = titleA.querySelector('span');
	    const name = clean((titleSpan && titleSpan.textContent) || titleA.getAttribute('title') || titleA.textContent || titleA.getAttribute('aria-label') || '');
	    if (!name) continue;
	    const authorSpan = n.querySelector('yt-lockup-metadata-view-model > div:nth-child(2) > div > yt-content-metadata-view-model > div:nth-child(1) > span') ||
	      n.querySelector('yt-lockup-metadata-view-model yt-content-metadata-view-model div:nth-child(1) span');
	    const author = clean((authorSpan && authorSpan.textContent) || '');
	    const recImg = n.querySelector('div > a yt-thumbnail-view-model div img') || n.querySelector('a yt-thumbnail-view-model div img');
	    const pic = clean((recImg && (recImg.getAttribute('src') || recImg.getAttribute('data-src') || recImg.getAttribute('data-thumb'))) || '');
	    const badge = n.querySelector('div > a yt-thumbnail-view-model yt-thumbnail-bottom-overlay-view-model div yt-thumbnail-badge-view-model badge-shape') ||
	      n.querySelector('a yt-thumbnail-view-model yt-thumbnail-bottom-overlay-view-model yt-thumbnail-badge-view-model badge-shape');
	    const duration = clean((badge && badge.textContent) || '');
	    const up = duration.toUpperCase();
	    if (duration.includes('直播') || up.includes('LIVE')) continue;
	    rec.push({vod_id:id, vod_name:name, vod_pic:pic, vod_author:author, vod_duration:duration});
	  }
	
	  const playlist = [];
	  const plNodes = document.querySelectorAll('#items > ytd-playlist-panel-video-renderer, ytd-playlist-panel-renderer #items > ytd-playlist-panel-video-renderer');
	  for (const n of plNodes) {
	    const a = n.querySelector('a[href*="/watch?"]');
	    if (!a) continue;
	    const id = normRel(a.getAttribute('href') || a.href || '');
	    if (!id) continue;
	    const titleSpan = n.querySelector('a div div h4 span') || n.querySelector('a h4 span') || n.querySelector('h4 span');
	    const name = clean((titleSpan && titleSpan.textContent) || a.getAttribute('title') || a.textContent || '');
	    if (!name) continue;
	    const img = n.querySelector('a div div ytd-thumbnail a yt-image img') || n.querySelector('a ytd-thumbnail a yt-image img') || n.querySelector('img');
	    const pic = clean((img && (img.getAttribute('src') || img.getAttribute('data-src') || img.getAttribute('data-thumb'))) || '');
	    const byline = n.querySelector('#byline a, #byline span, .byline a, .byline span');
	    const author = clean((byline && byline.textContent) || '');
	    const badge = n.querySelector('a div div ytd-thumbnail a ytd-thumbnail-overlay-time-status-renderer badge-shape div') ||
	      n.querySelector('a ytd-thumbnail a ytd-thumbnail-overlay-time-status-renderer badge-shape div') ||
	      n.querySelector('ytd-thumbnail-overlay-time-status-renderer badge-shape div');
	    const duration = clean((badge && badge.textContent) || '');
	    playlist.push({vod_id:id, vod_name:name, vod_pic:pic, vod_author:author, vod_duration:duration});
	    if (playlist.length >= 200) break;
	  }
	
	  return {title, author, cover, mainDuration, rec, playlist};
	})()`

	evalDetailValue := func() (map[string]any, error) {
		res, err := call("Runtime.evaluate", map[string]any{"expression": expr, "returnByValue": true})
		if err != nil {
			return nil, err
		}
		resultObj, _ := res["result"].(map[string]any)
		value, _ := resultObj["value"].(map[string]any)
		if value == nil {
			value = map[string]any{}
		}
		return value, nil
	}

	countItems := func(v any) int {
		arr, _ := v.([]any)
		return len(arr)
	}

	value, err := evalDetailValue()
	if err != nil {
		return nil, err
	}
	// 详情页按“推荐内容 rec”判定是否加载完成：最多轮询 3 秒。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		recCount := countItems(value["rec"])
		if recCount > 0 {
			break
		}
		_, _ = call("Runtime.evaluate", map[string]any{"expression": `new Promise(r=>setTimeout(r,200))`, "awaitPromise": true, "returnByValue": true})
		if v2, err2 := evalDetailValue(); err2 == nil {
			value = v2
		}
	}

	title := strings.TrimSpace(anyToString(value["title"]))
	if title == "" {
		title = watchRel
	}
	author := strings.TrimSpace(anyToString(value["author"]))
	mainDuration := strings.TrimSpace(anyToString(value["mainDuration"]))
	cover := strings.TrimSpace(anyToString(value["cover"]))

	parseItems := func(v any) []map[string]string {
		arr, _ := v.([]any)
		out := make([]map[string]string, 0, len(arr))
		for _, it := range arr {
			m, _ := it.(map[string]any)
			if m == nil {
				continue
			}
			id := strings.TrimSpace(anyToString(m["vod_id"]))
			name := strings.TrimSpace(anyToString(m["vod_name"]))
			if id == "" || name == "" {
				continue
			}
			out = append(out, map[string]string{
				"vod_id":       id,
				"vod_name":     name,
				"vod_pic":      strings.TrimSpace(anyToString(m["vod_pic"])),
				"vod_author":   strings.TrimSpace(anyToString(m["vod_author"])),
				"vod_duration": strings.TrimSpace(anyToString(m["vod_duration"])),
			})
		}
		return out
	}

	rec := parseItems(value["rec"])
	playlist := parseItems(value["playlist"])

	buildEpisodeID := func(id string, pic string, author string, duration string) string {
		cleanField := func(s string) string {
			v := strings.TrimSpace(s)
			v = strings.ReplaceAll(v, "*", "%2A")
			return v
		}
		// 固定七段：第1段图片，第2段作者（可空），第3段角标时长（可空），第7段链接
		parts := []string{cleanField(pic), cleanField(author), cleanField(duration), "", "", "", cleanField(id)}
		return strings.Join(parts, "*")
	}
	buildLine := func(items []map[string]string) string {
		parts := make([]string, 0, len(items))
		ep := 1
		for _, it := range items {
			name := strings.TrimSpace(it["vod_name"])
			name = strings.ReplaceAll(name, "$", "")
			name = strings.ReplaceAll(name, "#", " ")
			id := strings.TrimSpace(it["vod_id"])
			author := strings.TrimSpace(it["vod_author"])
			duration := strings.TrimSpace(it["vod_duration"])
			if name == "" || id == "" {
				continue
			}
			taggedName := fmt.Sprintf("%s.S01E%02d", name, ep)
			ep++
			parts = append(parts, taggedName+"$"+buildEpisodeID(id, buildImagePathByVodID(imageBase, imagePrefix, id), author, duration))
		}
		return strings.Join(parts, "#")
	}

	youtubeItems := playlist
	if len(youtubeItems) == 0 {
		youtubeItems = []map[string]string{{"vod_name": title, "vod_id": watchRel, "vod_pic": cover, "vod_author": author, "vod_duration": mainDuration}}
	}
	recommendItems := rec

	playFrom := []string{"youtube"}
	playURL := []string{buildLine(youtubeItems)}
	recommendLine := ""
	if len(recommendItems) > 0 {
		recommendLine = buildLine(recommendItems)
	}
	if strings.TrimSpace(recommendLine) != "" {
		playFrom = append(playFrom, "youtube-recommend")
		playURL = append(playURL, recommendLine)
	}

	return map[string]string{
		"vod_id":        watchRel,
		"vod_name":      title,
		"vod_pic":       cover,
		"vod_remarks":   "",
		"vod_play_from": strings.Join(playFrom, "$$$"),
		"vod_play_url":  strings.Join(playURL, "$$$"),
	}, nil
}

func cloneVodList(in []map[string]string) []map[string]string {
	out := make([]map[string]string, 0, len(in))
	for _, it := range in {
		cp := make(map[string]string, len(it))
		for k, v := range it {
			cp[k] = v
		}
		out = append(out, cp)
	}
	return out
}

func applyImageProxyBase(list []map[string]string, base, prefix string) {
	for i := range list {
		if p := buildImagePathByVodID(base, prefix, strings.TrimSpace(list[i]["vod_id"])); p != "" {
			list[i]["vod_pic"] = p
		}
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

func anyToString(v any) string {
	switch vv := v.(type) {
	case string:
		return vv
	case json.Number:
		return vv.String()
	default:
		return ""
	}
}

func toFloat(v any) float64 {
	switch vv := v.(type) {
	case float64:
		return vv
	case int:
		return float64(vv)
	case int64:
		return float64(vv)
	default:
		return 0
	}
}
