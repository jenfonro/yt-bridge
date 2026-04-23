package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func buildDashPlayText(videoID, qualityLabel string, durationSec float64, videoTrack mediaTrack, audioTrack mediaTrack, webmTemplate bool, prefix string) (string, error) {
	videoExt := strings.ToLower(strings.TrimSpace(trackPathExt(videoTrack)))
	audioExt := strings.ToLower(strings.TrimSpace(trackPathExt(audioTrack)))
	videoPath := withPathPrefix(prefix, "/proxy/"+videoID+"/mpd/video/"+qualityPathSegment(qualityLabel)+"."+videoExt)
	audioPath := withPathPrefix(prefix, "/proxy/"+videoID+"/mpd/audio/main."+audioExt)
	if durationSec <= 0 {
		durationSec = deriveDashDurationSec(videoTrack, audioTrack)
	}

	switch {
	case videoExt == "webm" && audioExt == "webm":
		text := buildWebMDashPlayText(durationSec, videoTrack, audioTrack, videoPath, audioPath, webmTemplate)
		if err := validateBuiltWebMMPD(text); err != nil {
			return "", err
		}
		return text, nil
	case isMP4LikeExt(videoExt) && (audioExt == "m4a" || audioExt == "mp4"):
		text := buildISOFFDashPlayText(durationSec, videoTrack, audioTrack, videoPath, audioPath)
		if err := validateBuiltISOFFMPD(text); err != nil {
			return "", err
		}
		return text, nil
	default:
		return "", errors.New("dash mpd build route mismatch")
	}
}

func buildISOFFDashPlayText(durationSec float64, videoTrack mediaTrack, audioTrack mediaTrack, videoPath, audioPath string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static" profiles="urn:mpeg:dash:profile:isoff-on-demand:2011"`)
	if durationSec > 0 {
		b.WriteString(` mediaPresentationDuration="`)
		b.WriteString(xmlEscape(formatDashDuration(durationSec)))
		b.WriteString(`"`)
	}
	b.WriteString(` minBufferTime="PT1.5S">` + "\n")
	b.WriteString(`  <ProgramInformation>` + "\n")
	b.WriteString(`  </ProgramInformation>` + "\n")
	b.WriteString(`  <ServiceDescription id="0">` + "\n")
	b.WriteString(`  </ServiceDescription>` + "\n")
	b.WriteString(`  <Period id="0" start="PT0S">` + "\n")
	writeMPDVideoAdaptationSet(&b, videoTrack, videoPath)
	if strings.TrimSpace(audioTrack.URL) != "" {
		writeMPDAudioAdaptationSet(&b, audioTrack, audioPath)
	}
	b.WriteString(`  </Period>` + "\n")
	b.WriteString(`</MPD>` + "\n")
	return b.String()
}

func buildWebMDashPlayText(durationSec float64, videoTrack mediaTrack, audioTrack mediaTrack, videoPath, audioPath string, webmTemplate bool) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static" profiles="urn:mpeg:dash:profile:webm-on-demand:2012"`)
	if durationSec > 0 {
		b.WriteString(` mediaPresentationDuration="`)
		b.WriteString(xmlEscape(formatDashDuration(durationSec)))
		b.WriteString(`"`)
	}
	b.WriteString(` minBufferTime="PT1.5S">` + "\n")
	b.WriteString(`  <ProgramInformation>` + "\n")
	b.WriteString(`  </ProgramInformation>` + "\n")
	b.WriteString(`  <ServiceDescription id="0">` + "\n")
	b.WriteString(`  </ServiceDescription>` + "\n")
	b.WriteString(`  <Period id="0" start="PT0S">` + "\n")
	if webmTemplate {
		writeMPDVideoAdaptationSetWebMTemplate(&b, videoTrack, videoPath)
		if strings.TrimSpace(audioTrack.URL) != "" {
			writeMPDAudioAdaptationSetWebMTemplate(&b, audioTrack, audioPath)
		}
	} else {
		writeMPDVideoAdaptationSet(&b, videoTrack, videoPath)
		if strings.TrimSpace(audioTrack.URL) != "" {
			writeMPDAudioAdaptationSet(&b, audioTrack, audioPath)
		}
	}
	b.WriteString(`  </Period>` + "\n")
	b.WriteString(`</MPD>` + "\n")
	return b.String()
}

func writeMPDVideoAdaptationSet(b *strings.Builder, track mediaTrack, baseURL string) {
	bw := track.Bandwidth
	if bw <= 0 {
		bw = 1
	}
	b.WriteString(`    <AdaptationSet id="1" contentType="video" mimeType="`)
	b.WriteString(xmlEscape(track.MimeType))
	b.WriteString(`">` + "\n")
	b.WriteString(`      <Representation id="video" bandwidth="`)
	b.WriteString(strconv.Itoa(bw))
	b.WriteString(`"`)
	if track.Width > 0 {
		b.WriteString(` width="`)
		b.WriteString(strconv.Itoa(track.Width))
		b.WriteString(`"`)
	}
	if track.Height > 0 {
		b.WriteString(` height="`)
		b.WriteString(strconv.Itoa(track.Height))
		b.WriteString(`"`)
	}
	if strings.TrimSpace(track.Codec) != "" {
		b.WriteString(` codecs="`)
		b.WriteString(xmlEscape(track.Codec))
		b.WriteString(`"`)
	}
	b.WriteString(`>` + "\n")
	b.WriteString(`        <BaseURL>`)
	b.WriteString(xmlEscape(baseURL))
	b.WriteString(`</BaseURL>` + "\n")
	writeMPDSegmentBase(b, track)
	b.WriteString(`      </Representation>` + "\n")
	b.WriteString(`    </AdaptationSet>` + "\n")
}

func writeMPDAudioAdaptationSet(b *strings.Builder, track mediaTrack, baseURL string) {
	bw := track.Bandwidth
	if bw <= 0 {
		bw = 1
	}
	b.WriteString(`    <AdaptationSet id="2" contentType="audio" mimeType="`)
	b.WriteString(xmlEscape(track.MimeType))
	b.WriteString(`">` + "\n")
	b.WriteString(`      <Representation id="audio" bandwidth="`)
	b.WriteString(strconv.Itoa(bw))
	b.WriteString(`"`)
	if strings.TrimSpace(track.Codec) != "" {
		b.WriteString(` codecs="`)
		b.WriteString(xmlEscape(track.Codec))
		b.WriteString(`"`)
	}
	b.WriteString(`>` + "\n")
	b.WriteString(`        <BaseURL>`)
	b.WriteString(xmlEscape(baseURL))
	b.WriteString(`</BaseURL>` + "\n")
	writeMPDSegmentBase(b, track)
	b.WriteString(`      </Representation>` + "\n")
	b.WriteString(`    </AdaptationSet>` + "\n")
}

func writeMPDVideoAdaptationSetWebMTemplate(b *strings.Builder, track mediaTrack, mediaPath string) {
	bw := track.Bandwidth
	if bw <= 0 {
		bw = 1
	}
	b.WriteString(`    <AdaptationSet id="1" contentType="video" mimeType="video/webm" startWithSAP="1" segmentAlignment="true" bitstreamSwitching="true"`)
	if fr := formatFrameRate(track.FPS); fr != "" {
		b.WriteString(` frameRate="`)
		b.WriteString(xmlEscape(fr))
		b.WriteString(`"`)
	}
	if track.Width > 0 {
		b.WriteString(` maxWidth="`)
		b.WriteString(strconv.Itoa(track.Width))
		b.WriteString(`"`)
	}
	if track.Height > 0 {
		b.WriteString(` maxHeight="`)
		b.WriteString(strconv.Itoa(track.Height))
		b.WriteString(`"`)
	}
	if par := pixelAspectRatio(track.Width, track.Height); par != "" {
		b.WriteString(` par="`)
		b.WriteString(xmlEscape(par))
		b.WriteString(`"`)
	}
	b.WriteString(` lang="eng">` + "\n")
	b.WriteString(`      <Representation id="0" mimeType="video/webm" bandwidth="`)
	b.WriteString(strconv.Itoa(bw))
	b.WriteString(`"`)
	if track.Width > 0 {
		b.WriteString(` width="`)
		b.WriteString(strconv.Itoa(track.Width))
		b.WriteString(`"`)
	}
	if track.Height > 0 {
		b.WriteString(` height="`)
		b.WriteString(strconv.Itoa(track.Height))
		b.WriteString(`"`)
	}
	if c := canonicalWebMCodec(track.Codec); c != "" {
		b.WriteString(` codecs="`)
		b.WriteString(xmlEscape(c))
		b.WriteString(`"`)
	}
	b.WriteString(` scanType="unknown"`)
	if par := pixelAspectRatio(track.Width, track.Height); par != "" {
		b.WriteString(` sar="`)
		b.WriteString(xmlEscape(par))
		b.WriteString(`"`)
	}
	b.WriteString(`>` + "\n")
	writeWebMSegmentTemplate(b, track, mediaPath)
	b.WriteString(`      </Representation>` + "\n")
	b.WriteString(`    </AdaptationSet>` + "\n")
}

func writeMPDAudioAdaptationSetWebMTemplate(b *strings.Builder, track mediaTrack, mediaPath string) {
	bw := track.Bandwidth
	if bw <= 0 {
		bw = 1
	}
	b.WriteString(`    <AdaptationSet id="2" contentType="audio" mimeType="audio/webm" startWithSAP="1" segmentAlignment="true" bitstreamSwitching="true" lang="eng">` + "\n")
	b.WriteString(`      <Representation id="1" mimeType="audio/webm" bandwidth="`)
	b.WriteString(strconv.Itoa(bw))
	b.WriteString(`"`)
	if c := canonicalWebMCodec(track.Codec); c != "" {
		b.WriteString(` codecs="`)
		b.WriteString(xmlEscape(c))
		b.WriteString(`"`)
	}
	b.WriteString(`>` + "\n")
	writeWebMSegmentTemplate(b, track, mediaPath)
	b.WriteString(`      </Representation>` + "\n")
	b.WriteString(`    </AdaptationSet>` + "\n")
}

func writeWebMSegmentTemplate(b *strings.Builder, track mediaTrack, mediaPath string) {
	if !hasRange(track.InitStart, track.InitEnd) || !hasSegmentList(track) {
		return
	}
	prefix, repID := webMTemplateRouteParts(mediaPath)
	if prefix == "" || repID == "" {
		return
	}
	b.WriteString(`        <SegmentTemplate timescale="`)
	b.WriteString(strconv.FormatUint(uint64(track.Timescale), 10))
	b.WriteString(`" startNumber="1" initialization="`)
	b.WriteString(xmlEscape(prefix + `/init-stream` + repID + `.webm`))
	b.WriteString(`" media="`)
	b.WriteString(xmlEscape(prefix + `/chunk-stream` + repID + `-$Number%05d$.webm`))
	b.WriteString(`">` + "\n")
	b.WriteString(`          <SegmentTimeline>` + "\n")
	for i, seg := range track.Segments {
		if seg.Duration == 0 {
			continue
		}
		b.WriteString(`            <S`)
		if i == 0 {
			b.WriteString(` t="`)
			b.WriteString(strconv.FormatUint(seg.Time, 10))
			b.WriteString(`"`)
		}
		b.WriteString(` d="`)
		b.WriteString(strconv.FormatUint(uint64(seg.Duration), 10))
		b.WriteString(`"/>` + "\n")
	}
	b.WriteString(`          </SegmentTimeline>` + "\n")
	b.WriteString(`        </SegmentTemplate>` + "\n")
}

func webMTemplateRouteParts(mediaPath string) (string, string) {
	trimmed := strings.TrimSpace(mediaPath)
	if trimmed == "" {
		return "", ""
	}
	dot := strings.LastIndex(trimmed, ".")
	slash := strings.LastIndex(trimmed, "/")
	if dot <= slash || slash <= 0 {
		return "", ""
	}
	repID := strings.ToLower(strings.TrimSpace(trimmed[slash+1 : dot]))
	if repID == "" {
		return "", ""
	}
	prefix := strings.TrimSpace(trimmed[:slash])
	if prefix == "" {
		return "", ""
	}
	return prefix, repID
}

func writeMPDSegmentBase(b *strings.Builder, track mediaTrack) {
	ext := strings.ToLower(strings.TrimSpace(track.Ext))
	if ext == "webm" {
		if hasSegmentList(track) {
			writeMPDSegmentList(b, track)
			return
		}
		if hasTrackIndex(track) {
			b.WriteString(`        <SegmentBase`)
			if hasRange(track.IndexStart, track.IndexEnd) {
				b.WriteString(` indexRange="`)
				b.WriteString(strconv.FormatInt(track.IndexStart, 10))
				b.WriteString(`-`)
				b.WriteString(strconv.FormatInt(track.IndexEnd, 10))
				b.WriteString(`"`)
			}
			b.WriteString(`>` + "\n")
			if hasRange(track.InitStart, track.InitEnd) {
				b.WriteString(`          <Initialization range="`)
				b.WriteString(strconv.FormatInt(track.InitStart, 10))
				b.WriteString(`-`)
				b.WriteString(strconv.FormatInt(track.InitEnd, 10))
				b.WriteString(`"/>` + "\n")
			}
			b.WriteString(`        </SegmentBase>` + "\n")
		}
		return
	}

	if hasSegmentList(track) {
		writeMPDSegmentList(b, track)
		return
	}
	if !hasTrackIndex(track) {
		return
	}
	b.WriteString(`        <SegmentBase`)
	if hasRange(track.IndexStart, track.IndexEnd) {
		b.WriteString(` indexRange="`)
		b.WriteString(strconv.FormatInt(track.IndexStart, 10))
		b.WriteString(`-`)
		b.WriteString(strconv.FormatInt(track.IndexEnd, 10))
		b.WriteString(`"`)
	}
	b.WriteString(`>` + "\n")
	if hasRange(track.InitStart, track.InitEnd) {
		b.WriteString(`          <Initialization range="`)
		b.WriteString(strconv.FormatInt(track.InitStart, 10))
		b.WriteString(`-`)
		b.WriteString(strconv.FormatInt(track.InitEnd, 10))
		b.WriteString(`"/>` + "\n")
	}
	b.WriteString(`        </SegmentBase>` + "\n")
}

func writeMPDSegmentList(b *strings.Builder, track mediaTrack) {
	if !hasSegmentList(track) {
		return
	}
	webm := strings.EqualFold(strings.TrimSpace(track.Ext), "webm")
	b.WriteString(`        <SegmentList timescale="`)
	b.WriteString(strconv.FormatUint(uint64(track.Timescale), 10))
	if !webm {
		if avg := averageSegmentDuration(track.Segments); avg > 0 {
			b.WriteString(`" duration="`)
			b.WriteString(strconv.FormatUint(uint64(avg), 10))
			b.WriteString(`">` + "\n")
		} else {
			b.WriteString(`">` + "\n")
		}
	} else {
		b.WriteString(`">` + "\n")
	}
	if hasRange(track.InitStart, track.InitEnd) {
		b.WriteString(`          <Initialization range="`)
		b.WriteString(strconv.FormatInt(track.InitStart, 10))
		b.WriteString(`-`)
		b.WriteString(strconv.FormatInt(track.InitEnd, 10))
		b.WriteString(`"/>` + "\n")
	}
	if !webm || allSegmentDurationsKnown(track.Segments) {
		b.WriteString(`          <SegmentTimeline>` + "\n")
		for _, seg := range track.Segments {
			if seg.Duration == 0 {
				continue
			}
			b.WriteString(`            <S d="`)
			b.WriteString(strconv.FormatUint(uint64(seg.Duration), 10))
			b.WriteString(`"/>` + "\n")
		}
		b.WriteString(`          </SegmentTimeline>` + "\n")
	}
	for _, seg := range track.Segments {
		if !hasRange(seg.Start, seg.End) {
			continue
		}
		b.WriteString(`          <SegmentURL mediaRange="`)
		b.WriteString(strconv.FormatInt(seg.Start, 10))
		b.WriteString(`-`)
		b.WriteString(strconv.FormatInt(seg.End, 10))
		b.WriteString(`"/>` + "\n")
	}
	b.WriteString(`        </SegmentList>` + "\n")
}

func deriveDashDurationSec(videoTrack mediaTrack, audioTrack mediaTrack) float64 {
	if sec := trackDurationSec(videoTrack); sec > 0 {
		return sec
	}
	if sec := trackDurationSec(audioTrack); sec > 0 {
		return sec
	}
	return 0
}

func trackDurationSec(track mediaTrack) float64 {
	if track.Timescale == 0 || len(track.Segments) == 0 {
		return 0
	}
	var total uint64
	for _, seg := range track.Segments {
		total += uint64(seg.Duration)
	}
	if total == 0 {
		return 0
	}
	return float64(total) / float64(track.Timescale)
}

func hasTrackIndex(track mediaTrack) bool {
	return hasSegmentList(track) || hasRange(track.InitStart, track.InitEnd) || hasRange(track.IndexStart, track.IndexEnd)
}

func requiresTrackIndex(track mediaTrack) bool {
	_ = track
	return true
}

func hasSegmentList(track mediaTrack) bool {
	return track.Timescale > 0 && len(track.Segments) > 0
}

func allSegmentDurationsKnown(segments []dashSegment) bool {
	if len(segments) == 0 {
		return false
	}
	for _, seg := range segments {
		if seg.Duration == 0 {
			return false
		}
	}
	return true
}

func averageSegmentDuration(segments []dashSegment) uint32 {
	if len(segments) == 0 {
		return 0
	}
	var total uint64
	var count uint64
	for _, seg := range segments {
		if seg.Duration == 0 {
			continue
		}
		total += uint64(seg.Duration)
		count++
	}
	if count == 0 {
		return 0
	}
	return uint32(total / count)
}

func isResolvedDashPairUsable(videoTrack mediaTrack, audioTrack mediaTrack) bool {
	if strings.TrimSpace(videoTrack.URL) == "" || strings.TrimSpace(audioTrack.URL) == "" {
		return false
	}
	if !hasTrackIndex(videoTrack) || !hasTrackIndex(audioTrack) {
		return false
	}
	if strings.TrimSpace(trackPathExt(videoTrack)) == "" || strings.TrimSpace(trackPathExt(audioTrack)) == "" {
		return false
	}
	if strings.TrimSpace(videoTrack.MimeType) == "" || strings.TrimSpace(audioTrack.MimeType) == "" {
		return false
	}
	if err := validateDashTrackByFormat(videoTrack); err != nil {
		return false
	}
	if err := validateDashTrackByFormat(audioTrack); err != nil {
		return false
	}
	return isDashContainerAligned(videoTrack, audioTrack)
}

func isDashContainerAligned(videoTrack mediaTrack, audioTrack mediaTrack) bool {
	v := strings.ToLower(strings.TrimSpace(videoTrack.Ext))
	a := strings.ToLower(strings.TrimSpace(audioTrack.Ext))
	if v == "webm" {
		return a == "webm"
	}
	if isMP4LikeExt(v) {
		return a == "m4a" || a == "mp4"
	}
	return false
}

func validateDashTrackByFormat(track mediaTrack) error {
	ext := strings.ToLower(strings.TrimSpace(track.Ext))
	mime := strings.ToLower(strings.TrimSpace(track.MimeType))
	if ext == "" || mime == "" {
		return errors.New("missing ext or mime")
	}
	if ext == "webm" {
		if mime != "audio/webm" && mime != "video/webm" {
			return errors.New("webm mime mismatch")
		}
		if !hasRange(track.InitStart, track.InitEnd) {
			return errors.New("webm track missing init range")
		}
		if hasSegmentList(track) || hasRange(track.IndexStart, track.IndexEnd) {
			return nil
		}
		return errors.New("webm track missing index")
	}
	if hasSegmentList(track) {
		return nil
	}
	if isMP4LikeExt(ext) {
		if mime != "audio/mp4" && mime != "video/mp4" {
			return errors.New("mp4 mime mismatch")
		}
		if !hasRange(track.InitStart, track.InitEnd) {
			return errors.New("mp4 track missing init range")
		}
		if !hasRange(track.IndexStart, track.IndexEnd) {
			return errors.New("mp4 track missing index range")
		}
		return nil
	}
	return errors.New("unsupported ext for dash")
}

func hasRange(start, end int64) bool {
	return start >= 0 && end > start
}

func webMTemplateSliceForInternalReq(req internalMediaReq, track mediaTrack) (int64, int64, bool) {
	if !strings.EqualFold(trackPathExt(track), "webm") {
		return 0, 0, false
	}
	if req.TemplateInit {
		if hasRange(track.InitStart, track.InitEnd) {
			return track.InitStart, track.InitEnd, true
		}
		return 0, 0, false
	}
	if req.TemplateSeg <= 0 {
		return 0, 0, false
	}
	n := req.TemplateSeg
	if n > len(track.Segments) {
		return 0, 0, false
	}
	seg := track.Segments[n-1]
	if !hasRange(seg.Start, seg.End) {
		return 0, 0, false
	}
	return seg.Start, seg.End, true
}

func locateSegmentByRange(segments []dashSegment, reqStart, reqEnd int64) int {
	for i, seg := range segments {
		if reqStart >= seg.Start && reqEnd <= seg.End {
			return i
		}
	}
	return -1
}

func segmentTimeWindowMS(segments []dashSegment, idx int) (int64, int64, bool) {
	if idx < 0 || idx >= len(segments) {
		return 0, 0, false
	}
	var acc int64
	for i := 0; i < idx; i++ {
		acc += int64(segments[i].Duration)
	}
	start := acc
	end := acc + int64(segments[idx].Duration)
	return start, end, true
}

func summarizeSegmentTimes(segments []dashSegment, n int) string {
	if len(segments) == 0 || n <= 0 {
		return ""
	}
	if n > len(segments) {
		n = len(segments)
	}
	parts := make([]string, 0, n)
	for i := 0; i < n; i++ {
		parts = append(parts, strconv.FormatUint(segments[i].Time, 10))
	}
	return strings.Join(parts, ",")
}

func (a *app) noteSegTrace(streamKey string, idx int) {
	a.segTraceMu.Lock()
	prev, ok := a.segTrace[streamKey]
	a.segTrace[streamKey] = idx
	a.segTraceMu.Unlock()
	if !ok {
		return
	}
	if idx < prev {
		log.Printf("[yt-bridge] segtrace key=%s reset prev=%d now=%d", streamKey, prev, idx)
		return
	}
	if idx > prev+1 {
		log.Printf("[yt-bridge] segtrace key=%s jump prev=%d now=%d", streamKey, prev, idx)
	}
}

func writePlaylistText(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, text)
}

func writeMPDText(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "application/dash+xml")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, text)
}

func (a *app) proxyAssetTemplateSlice(w http.ResponseWriter, r *http.Request, targetURL string, absStart, absEnd int64) {
	method := r.Method
	if method != http.MethodGet && method != http.MethodHead {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(r.Context(), method, strings.TrimSpace(targetURL), nil)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "proxy request failed"})
		return
	}
	if v := strings.TrimSpace(r.Header.Get("User-Agent")); v != "" {
		req.Header.Set("User-Agent", v)
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", absStart, absEnd))
	if v := strings.TrimSpace(r.Header.Get("Accept")); v != "" {
		req.Header.Set("Accept", v)
	}

	resp, err := a.upstreamClient.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "proxy request failed"})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": fmt.Sprintf("upstream http %d", resp.StatusCode)})
		return
	}

	sliceLen := absEnd - absStart + 1
	if sliceLen <= 0 {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "invalid template slice"})
		return
	}
	if method == http.MethodHead {
		for k, vals := range resp.Header {
			lk := strings.ToLower(strings.TrimSpace(k))
			if lk == "content-range" || lk == "content-length" || lk == "accept-ranges" || lk == "transfer-encoding" {
				continue
			}
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", strconv.FormatInt(sliceLen, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", sliceLen-1, sliceLen))
		w.WriteHeader(http.StatusPartialContent)
		return
	}
	buf, err := io.ReadAll(io.LimitReader(resp.Body, sliceLen+1))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "proxy read failed"})
		return
	}
	if int64(len(buf)) > sliceLen {
		buf = buf[:sliceLen]
	}

	for k, vals := range resp.Header {
		lk := strings.ToLower(strings.TrimSpace(k))
		if lk == "content-range" || lk == "content-length" || lk == "accept-ranges" || lk == "transfer-encoding" {
			continue
		}
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(buf)), 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(buf)-1, len(buf)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(buf)
}

func (a *app) proxyAssetWithRefresh(w http.ResponseWriter, r *http.Request, targetURL string, refresh func() (string, bool)) {
	method := r.Method
	if method != http.MethodGet && method != http.MethodHead {
		method = http.MethodGet
	}
	doRequest := func(u string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(r.Context(), method, strings.TrimSpace(u), nil)
		if err != nil {
			return nil, err
		}
		if v := strings.TrimSpace(r.Header.Get("User-Agent")); v != "" {
			req.Header.Set("User-Agent", v)
		}
		rangeHeader := strings.TrimSpace(r.Header.Get("Range"))
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
			req.Header.Set("Accept-Encoding", "identity")
		}
		if v := strings.TrimSpace(r.Header.Get("Accept")); v != "" {
			req.Header.Set("Accept", v)
		}
		if rangeHeader == "" {
			if v := strings.TrimSpace(r.Header.Get("Accept-Encoding")); v != "" {
				req.Header.Set("Accept-Encoding", v)
			}
		}
		return a.upstreamClient.Do(req)
	}

	resp, err := doRequest(targetURL)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "proxy request failed"})
		return
	}
	if resp.StatusCode == http.StatusForbidden && refresh != nil {
		resp.Body.Close()
		if newURL, ok := refresh(); ok && strings.TrimSpace(newURL) != "" && strings.TrimSpace(newURL) != strings.TrimSpace(targetURL) {
			resp, err = doRequest(newURL)
			if err != nil {
				writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "proxy refresh failed"})
				return
			}
		} else {
			// keep original 403 semantics
			resp, err = doRequest(targetURL)
			if err != nil {
				writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "message": "proxy request failed"})
				return
			}
		}
	}
	defer resp.Body.Close()

	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if method == http.MethodHead {
		return
	}
	_, _ = io.Copy(w, resp.Body)
}

func newUpstreamClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   32,
		MaxConnsPerHost:       64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
	}
	return &http.Client{Transport: transport}
}

func isDigits(raw string) bool {
	if raw == "" {
		return false
	}
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func isHTTPURL(raw string) bool {
	s := strings.ToLower(strings.TrimSpace(raw))
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

func isHLSURL(raw string) bool {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" {
		return false
	}
	if strings.Contains(s, ".m3u8?") || strings.HasSuffix(s, ".m3u8") {
		return true
	}
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(u.Path)), ".m3u8")
}

func hasCodec(raw string) bool {
	v := strings.ToLower(strings.TrimSpace(raw))
	return v != "" && v != "none"
}

func videoCodecRank(raw string) int {
	codec := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case strings.HasPrefix(codec, "av01"):
		return 3
	case strings.HasPrefix(codec, "vp9"):
		return 2
	case strings.HasPrefix(codec, "avc1"):
		return 1
	default:
		return 0
	}
}

func audioCodecRank(raw string) int {
	codec := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case strings.HasPrefix(codec, "opus"):
		return 3
	case strings.HasPrefix(codec, "mp4a"):
		return 2
	default:
		return 0
	}
}

func bitrateToBandwidth(v float64) int {
	if v <= 0 {
		return 0
	}
	bw := int(v * 1000)
	if bw < 1 {
		return 0
	}
	return bw
}

func trackToBandwidth(track ytdlpFormat) int {
	if bw := bitrateToBandwidth(track.TBR); bw > 0 {
		return bw
	}
	return bitrateToBandwidth(track.ABR)
}

func isMP4LikeExt(ext string) bool {
	switch strings.ToLower(strings.TrimSpace(ext)) {
	case "mp4", "m4v", "m4a":
		return true
	default:
		return false
	}
}

func videoMimeType(ext, rawURL string) string {
	if strings.TrimSpace(ext) == "" {
		ext = trackExtFromURL(rawURL)
	}
	switch strings.ToLower(strings.TrimSpace(ext)) {
	case "webm":
		return "video/webm"
	case "mp4", "m4v":
		return "video/mp4"
	default:
		return ""
	}
}

func audioMimeType(ext, rawURL string) string {
	if strings.TrimSpace(ext) == "" {
		ext = trackExtFromURL(rawURL)
	}
	switch strings.ToLower(strings.TrimSpace(ext)) {
	case "webm":
		return "audio/webm"
	case "m4a", "mp4":
		return "audio/mp4"
	default:
		return ""
	}
}

func trackExtFromURL(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	p := strings.ToLower(strings.TrimSpace(u.Path))
	dot := strings.LastIndexByte(p, '.')
	if dot < 0 || dot+1 >= len(p) {
		return ""
	}
	return p[dot+1:]
}

func formatDashDuration(seconds float64) string {
	if seconds <= 0 {
		return "PT0S"
	}
	return fmt.Sprintf("PT%.3fS", seconds)
}

func canonicalWebMCodec(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case strings.HasPrefix(v, "vp9") || strings.HasPrefix(v, "vp09"):
		return "vp09.00.50.08"
	case strings.HasPrefix(v, "opus"):
		return "opus"
	default:
		return strings.TrimSpace(raw)
	}
}

func formatFrameRate(fps float64) string {
	if fps <= 0 {
		return ""
	}
	return fmt.Sprintf("%d/1", int(math.Round(fps)))
}

func pixelAspectRatio(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	g := gcd(width, height)
	if g <= 0 {
		return ""
	}
	return fmt.Sprintf("%d:%d", width/g, height/g)
}

func gcd(a, b int) int {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

var xmlEscaper = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	`"`, "&quot;",
	"'", "&apos;",
)

func xmlEscape(raw string) string {
	return xmlEscaper.Replace(raw)
}

func validateBuiltWebMMPD(text string) error {
	body := strings.TrimSpace(text)
	if body == "" {
		return errors.New("empty webm mpd")
	}
	if !strings.Contains(body, `profiles="urn:mpeg:dash:profile:webm-on-demand:2012"`) {
		return errors.New("webm profile mismatch")
	}
	if strings.Contains(body, `profiles="urn:mpeg:dash:profile:isoff-on-demand:2011"`) {
		return errors.New("unexpected isoff profile in webm mpd")
	}
	if !strings.Contains(body, `mimeType="video/webm"`) {
		return errors.New("webm video mime missing in mpd")
	}
	if strings.Contains(body, `mimeType="audio/`) && !strings.Contains(body, `mimeType="audio/webm"`) {
		return errors.New("webm audio mime mismatch in mpd")
	}
	if !strings.Contains(body, `<SegmentList`) && !strings.Contains(body, `<SegmentBase indexRange="`) && !strings.Contains(body, `<SegmentTemplate`) {
		return errors.New("webm mpd missing segment index")
	}
	// SegmentTimeline is optional for webm SegmentList, both forms are acceptable.
	if strings.Contains(body, `<SegmentTemplate`) {
		if !strings.Contains(body, ` initialization="`) {
			return errors.New("webm mpd missing template initialization")
		}
	} else {
		if !strings.Contains(body, `<Initialization range="`) {
			return errors.New("webm mpd missing initialization range")
		}
	}
	return nil
}

func validateBuiltISOFFMPD(text string) error {
	body := strings.TrimSpace(text)
	if body == "" {
		return errors.New("empty isoff mpd")
	}
	if !strings.Contains(body, `profiles="urn:mpeg:dash:profile:isoff-on-demand:2011"`) {
		return errors.New("isoff profile mismatch")
	}
	if strings.Contains(body, `profiles="urn:mpeg:dash:profile:webm-on-demand:2012"`) {
		return errors.New("unexpected webm profile in isoff mpd")
	}
	if strings.Contains(body, `mimeType="video/webm"`) || strings.Contains(body, `mimeType="audio/webm"`) {
		return errors.New("unexpected webm mime in isoff mpd")
	}
	return nil
}

func normalizePlayWatchID(raw string) (string, bool) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return "", false
	}
	if strings.HasPrefix(v, "watch?") {
		return v, true
	}
	if idx := strings.Index(v, "watch?"); idx >= 0 {
		return strings.TrimSpace(v[idx:]), true
	}
	return "", false
}

func extractYouTubeVideoIDFromWatchID(watchID string) (string, error) {
	raw := strings.TrimSpace(strings.TrimPrefix(watchID, "/"))
	if !strings.HasPrefix(raw, "watch?") {
		return "", errors.New("invalid watch id")
	}
	u, err := url.Parse("https://www.youtube.com/" + raw)
	if err != nil {
		return "", errors.New("invalid watch id")
	}
	id := strings.TrimSpace(u.Query().Get("v"))
	if id == "" {
		return "", errors.New("missing youtube video id")
	}
	return id, nil
}

func qualityOrder(label string) int {
	s := strings.ToUpper(strings.TrimSpace(label))
	switch s {
	case "8K60":
		return 1600
	case "8K":
		return 1500
	case "4K60":
		return 1400
	case "4K":
		return 1300
	case "2K60":
		return 1200
	case "2K":
		return 1100
	case "1080P60":
		return 1000
	case "1080P":
		return 900
	case "720P60":
		return 800
	case "720P":
		return 700
	case "480P":
		return 600
	case "360P":
		return 500
	case "240P":
		return 400
	case "144P":
		return 300
	default:
		return 0
	}
}

func sortPlayItems(items map[string]playQualityItem) []playQualityItem {
	arr := make([]playQualityItem, 0, len(items))
	for _, it := range items {
		arr = append(arr, it)
	}
	sort.SliceStable(arr, func(i, j int) bool {
		ri := qualityOrder(arr[i].Label)
		rj := qualityOrder(arr[j].Label)
		if ri != rj {
			return ri > rj
		}
		return strings.ToUpper(strings.TrimSpace(arr[i].Label)) > strings.ToUpper(strings.TrimSpace(arr[j].Label))
	})
	return arr
}

func qualityLabelForFormat(f ytdlpFormat) string {
	h := f.Height
	if h <= 0 {
		return ""
	}
	fps := int(f.FPS + 0.5)
	is60 := fps >= 50
	if h >= 2160 {
		if is60 {
			return "4K60"
		}
		return "4K"
	}
	if h >= 1440 {
		if is60 {
			return "2K60"
		}
		return "2K"
	}
	if h >= 1080 {
		if is60 {
			return "1080P60"
		}
		return "1080P"
	}
	if h >= 720 {
		if is60 {
			return "720P60"
		}
		return "720P"
	}
	if h >= 480 {
		return "480P"
	}
	if h >= 360 {
		return "360P"
	}
	if h >= 240 {
		return "240P"
	}
	return "144P"
}

func trackPathExt(track mediaTrack) string {
	ext := strings.ToLower(strings.TrimSpace(track.Ext))
	if isPathExtAllowed(ext) {
		return ext
	}
	mime := strings.ToLower(strings.TrimSpace(track.MimeType))
	switch mime {
	case "video/webm", "audio/webm":
		return "webm"
	case "video/mp4", "audio/mp4":
		return "mp4"
	default:
		return ""
	}
}

func isPathExtAllowed(ext string) bool {
	switch strings.ToLower(strings.TrimSpace(ext)) {
	case "mp4", "webm", "m4a", "m4v":
		return true
	default:
		return false
	}
}

func chooseBestDashPair(ctx context.Context, qualityLabel string, preferredVideoExt string, pick ytdlpFormat, _ ytdlpFormat, allFormats []ytdlpFormat) (mediaTrack, mediaTrack, bool) {
	cands := []ytdlpFormat{}
	target := strings.ToUpper(strings.TrimSpace(qualityLabel))
	wantExt := strings.ToLower(strings.TrimSpace(preferredVideoExt))
	addCand := func(f ytdlpFormat) {
		if !hasCodec(f.VCodec) || !isHTTPURL(f.URL) {
			return
		}
		if strings.ToUpper(strings.TrimSpace(qualityLabelForFormat(f))) != target {
			return
		}
		if wantExt != "" && strings.ToLower(strings.TrimSpace(f.Ext)) != wantExt {
			return
		}
		cands = append(cands, f)
	}
	addCand(pick)
	for _, f := range allFormats {
		if strings.TrimSpace(f.URL) == strings.TrimSpace(pick.URL) {
			continue
		}
		addCand(f)
	}
	sort.SliceStable(cands, func(i, j int) bool {
		ri := videoCodecRank(cands[i].VCodec)
		rj := videoCodecRank(cands[j].VCodec)
		if ri != rj {
			return ri > rj
		}
		if cands[i].TBR != cands[j].TBR {
			return cands[i].TBR > cands[j].TBR
		}
		return strings.Compare(cands[i].FormatID, cands[j].FormatID) < 0
	})

	for _, cand := range cands {
		video := enrichTrackIndexStandalone(ctx, formatToMediaTrack(cand))
		videoExt := strings.ToLower(strings.TrimSpace(video.Ext))
		if strings.TrimSpace(video.MimeType) == "" || strings.TrimSpace(trackPathExt(video)) == "" {
			continue
		}
		if requiresTrackIndex(video) && !hasTrackIndex(video) {
			continue
		}

		audioCand, ok := selectBestAudioTrackForVideoExt(allFormats, videoExt)
		if !ok {
			continue
		}
		audio := enrichTrackIndexStandalone(ctx, formatToMediaTrack(audioCand))
		if strings.TrimSpace(audio.MimeType) == "" || strings.TrimSpace(trackPathExt(audio)) == "" {
			continue
		}
		if requiresTrackIndex(audio) && !hasTrackIndex(audio) {
			continue
		}
		if !isAudioCompatibleWithVideoExt(audioCand.Ext, videoExt) {
			continue
		}
		return video, audio, true
	}
	return mediaTrack{}, mediaTrack{}, false
}

func enrichTrackIndexStandalone(ctx context.Context, track mediaTrack) mediaTrack {
	if strings.TrimSpace(track.URL) == "" {
		return track
	}
	idx, ok := probeMediaTrackIndex(ctx, track.URL, track.Ext)
	if !ok {
		return track
	}
	track.InitStart = idx.InitStart
	track.InitEnd = idx.InitEnd
	track.IndexStart = idx.IndexStart
	track.IndexEnd = idx.IndexEnd
	track.Timescale = idx.Timescale
	track.Segments = idx.Segments
	return track
}
