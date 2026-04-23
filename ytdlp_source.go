package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

func (a *app) runYtDlpJSON(ctx context.Context, watchURL string) (*ytdlpInfo, error) {
	args := []string{
		"--js-runtimes", "node",
		"--no-playlist", "--no-warnings", "-J",
	}
	args = append(args, "--cookies", a.cookiesPath)
	args = append(args, watchURL)

	cmd := exec.CommandContext(ctx, a.ytdlpPath, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, errors.New(msg)
	}

	raw := bytes.TrimSpace(stdout)
	if len(raw) == 0 {
		return nil, errors.New("yt-dlp returned empty json")
	}

	var info ytdlpInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("yt-dlp json decode failed: %w", err)
	}
	return &info, nil
}

func selectBestVideoTrack(formats []ytdlpFormat) (ytdlpFormat, bool) {
	candidates := make([]ytdlpFormat, 0, len(formats))
	for i := range formats {
		f := formats[i]
		if !hasCodec(f.VCodec) {
			continue
		}
		if !isHTTPURL(f.URL) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(f.Protocol), "mhtml") {
			continue
		}
		candidates = append(candidates, f)
	}
	if len(candidates) == 0 {
		return ytdlpFormat{}, false
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Height != candidates[j].Height {
			return candidates[i].Height > candidates[j].Height
		}
		if candidates[i].Width != candidates[j].Width {
			return candidates[i].Width > candidates[j].Width
		}
		iCodecRank := videoCodecRank(candidates[i].VCodec)
		jCodecRank := videoCodecRank(candidates[j].VCodec)
		if iCodecRank != jCodecRank {
			return iCodecRank > jCodecRank
		}
		iMP4 := isMP4LikeExt(candidates[i].Ext)
		jMP4 := isMP4LikeExt(candidates[j].Ext)
		if iMP4 != jMP4 {
			return iMP4
		}
		return candidates[i].TBR > candidates[j].TBR
	})
	return candidates[0], true
}

func selectBestAudioTrack(formats []ytdlpFormat) (ytdlpFormat, bool) {
	candidates := make([]ytdlpFormat, 0, len(formats))
	for i := range formats {
		f := formats[i]
		if hasCodec(f.VCodec) || !hasCodec(f.ACodec) {
			continue
		}
		if !isHTTPURL(f.URL) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(f.Protocol), "mhtml") {
			continue
		}
		candidates = append(candidates, f)
	}
	if len(candidates) == 0 {
		return ytdlpFormat{}, false
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		iCodecRank := audioCodecRank(candidates[i].ACodec)
		jCodecRank := audioCodecRank(candidates[j].ACodec)
		if iCodecRank != jCodecRank {
			return iCodecRank > jCodecRank
		}
		if candidates[i].ABR != candidates[j].ABR {
			return candidates[i].ABR > candidates[j].ABR
		}
		return candidates[i].TBR > candidates[j].TBR
	})
	return candidates[0], true
}

func selectBestAudioTrackForVideoExt(formats []ytdlpFormat, videoExt string) (ytdlpFormat, bool) {
	candidates := make([]ytdlpFormat, 0, len(formats))
	for i := range formats {
		f := formats[i]
		if hasCodec(f.VCodec) || !hasCodec(f.ACodec) {
			continue
		}
		if !isHTTPURL(f.URL) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(f.Protocol), "mhtml") {
			continue
		}
		if !isAudioCompatibleWithVideoExt(f.Ext, videoExt) {
			continue
		}
		candidates = append(candidates, f)
	}
	if len(candidates) == 0 {
		return ytdlpFormat{}, false
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		iCodecRank := audioCodecRank(candidates[i].ACodec)
		jCodecRank := audioCodecRank(candidates[j].ACodec)
		if iCodecRank != jCodecRank {
			return iCodecRank > jCodecRank
		}
		if candidates[i].ABR != candidates[j].ABR {
			return candidates[i].ABR > candidates[j].ABR
		}
		return candidates[i].TBR > candidates[j].TBR
	})
	return candidates[0], true
}

func isAudioCompatibleWithVideoExt(audioExt, videoExt string) bool {
	a := strings.ToLower(strings.TrimSpace(audioExt))
	v := strings.ToLower(strings.TrimSpace(videoExt))
	if v == "webm" {
		return a == "webm"
	}
	if isMP4LikeExt(v) {
		return a == "m4a" || a == "mp4"
	}
	return false
}

func formatToMediaTrack(f ytdlpFormat) mediaTrack {
	isAudio := !hasCodec(f.VCodec) && hasCodec(f.ACodec)
	codec := strings.TrimSpace(f.VCodec)
	if isAudio {
		codec = strings.TrimSpace(f.ACodec)
	}
	mime := videoMimeType(strings.TrimSpace(f.Ext), strings.TrimSpace(f.URL))
	if isAudio {
		mime = audioMimeType(strings.TrimSpace(f.Ext), strings.TrimSpace(f.URL))
	}
	return mediaTrack{
		URL:       strings.TrimSpace(f.URL),
		Bandwidth: trackToBandwidth(f),
		Width:     f.Width,
		Height:    f.Height,
		FPS:       f.FPS,
		Codec:     codec,
		Ext:       strings.ToLower(strings.TrimSpace(f.Ext)),
		MimeType:  mime,
	}
}
