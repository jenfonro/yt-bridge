package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
)

func parseMP4IndexFromPrefix(prefix []byte) (mediaTrackIndex, bool, error) {
	out := mediaTrackIndex{}
	if len(prefix) < 8 {
		return out, false, errors.New("mp4 probe too short")
	}

	var moovStart int64 = -1
	var moovEnd int64 = -1
	var firstMdat int64 = -1
	var firstSidxStart int64 = -1
	var firstSidxEnd int64 = -1
	var sidxTimescale uint32
	var sidxSegments []dashSegment

	complete := true
	pos := int64(0)
	for {
		if pos == int64(len(prefix)) {
			break
		}
		if pos+8 > int64(len(prefix)) {
			complete = false
			break
		}
		size32 := binary.BigEndian.Uint32(prefix[pos : pos+4])
		boxType := string(prefix[pos+4 : pos+8])
		headerSize := int64(8)
		boxSize := int64(size32)

		if size32 == 1 {
			if pos+16 > int64(len(prefix)) {
				complete = false
				break
			}
			boxSize = int64(binary.BigEndian.Uint64(prefix[pos+8 : pos+16]))
			headerSize = 16
		} else if size32 == 0 {
			break
		}
		if boxSize < headerSize {
			return out, false, errors.New("invalid mp4 box size")
		}
		boxEnd := pos + boxSize
		if boxEnd > int64(len(prefix)) {
			complete = false
			break
		}

		switch boxType {
		case "moov":
			if moovStart < 0 {
				moovStart = pos
				moovEnd = boxEnd - 1
			}
		case "sidx":
			if firstSidxStart < 0 {
				firstSidxStart = pos
				firstSidxEnd = boxEnd - 1
				timescale, segments, parseErr := parseSidxBox(prefix[pos:boxEnd], boxEnd)
				if parseErr == nil {
					sidxTimescale = timescale
					sidxSegments = segments
				}
			}
		case "mdat":
			if firstMdat < 0 {
				firstMdat = pos
			}
		}
		pos = boxEnd
	}

	if moovStart >= 0 && moovEnd >= moovStart {
		if firstMdat < 0 || firstMdat > moovStart {
			out.InitStart = 0
			out.InitEnd = moovEnd
		}
	}
	if firstSidxStart >= 0 && firstSidxEnd >= firstSidxStart {
		out.IndexStart = firstSidxStart
		out.IndexEnd = firstSidxEnd
		out.Timescale = sidxTimescale
		out.Segments = sidxSegments
	}
	return out, complete, nil
}

func parseSidxBox(box []byte, boxEnd int64) (uint32, []dashSegment, error) {
	if len(box) < 32 {
		return 0, nil, errors.New("sidx box too short")
	}
	if string(box[4:8]) != "sidx" {
		return 0, nil, errors.New("not sidx box")
	}

	version := box[8]
	cursor := 12
	if cursor+8 > len(box) {
		return 0, nil, errors.New("invalid sidx header")
	}
	timescale := binary.BigEndian.Uint32(box[cursor+4 : cursor+8])
	cursor += 8

	var firstOffset uint64
	if version == 0 {
		if cursor+8 > len(box) {
			return 0, nil, errors.New("invalid sidx v0 fields")
		}
		firstOffset = uint64(binary.BigEndian.Uint32(box[cursor+4 : cursor+8]))
		cursor += 8
	} else if version == 1 {
		if cursor+16 > len(box) {
			return 0, nil, errors.New("invalid sidx v1 fields")
		}
		firstOffset = binary.BigEndian.Uint64(box[cursor+8 : cursor+16])
		cursor += 16
	} else {
		return 0, nil, errors.New("unsupported sidx version")
	}

	if cursor+4 > len(box) {
		return 0, nil, errors.New("invalid sidx ref count")
	}
	referenceCount := int(binary.BigEndian.Uint16(box[cursor+2 : cursor+4]))
	cursor += 4

	dataStart := uint64(boxEnd) + firstOffset
	segments := make([]dashSegment, 0, referenceCount)
	for i := 0; i < referenceCount; i++ {
		if cursor+12 > len(box) {
			return 0, nil, errors.New("truncated sidx entries")
		}
		refField := binary.BigEndian.Uint32(box[cursor : cursor+4])
		referenceType := (refField >> 31) & 0x1
		referencedSize := refField & 0x7fffffff
		subsegmentDuration := binary.BigEndian.Uint32(box[cursor+4 : cursor+8])
		cursor += 12

		if referenceType != 0 || referencedSize == 0 {
			dataStart += uint64(referencedSize)
			continue
		}
		start := int64(dataStart)
		end := start + int64(referencedSize) - 1
		if end <= start {
			dataStart += uint64(referencedSize)
			continue
		}
		segments = append(segments, dashSegment{Start: start, End: end, Time: 0, Duration: subsegmentDuration})
		dataStart += uint64(referencedSize)
	}
	if timescale == 0 || len(segments) == 0 {
		return timescale, nil, errors.New("sidx has no media references")
	}
	return timescale, segments, nil
}

func probeMediaTrackIndex(ctx context.Context, trackURL, trackExt string) (mediaTrackIndex, bool) {
	client := newUpstreamClient()
	ext := strings.ToLower(strings.TrimSpace(trackExt))

	if strings.Contains(ext, "webm") {
		totalSize, ok := fetchContentLengthStandalone(ctx, client, trackURL)
		if !ok || totalSize <= 0 {
			return mediaTrackIndex{}, false
		}
		for size := int64(probeStepBytes); size <= int64(probeMaxBytes); size += int64(probeStepBytes) {
			chunk, err := fetchRangeBytesStandalone(ctx, client, trackURL, 0, size-1)
			if err != nil {
				return mediaTrackIndex{}, false
			}
			index, state, complete, err := parseWebMIndexFromPrefix(chunk, totalSize)
			if err != nil {
				return mediaTrackIndex{}, false
			}
			if hasRange(index.InitStart, index.InitEnd) && len(index.Segments) > 0 {
				return index, true
			}
			if state.CuesOffsetRel >= 0 {
				cuesStart := state.SegmentDataStart + state.CuesOffsetRel
				if cuesStart >= 0 && cuesStart < totalSize {
					cuesEnd := cuesStart + int64(probeMaxBytes) - 1
					if cuesEnd >= totalSize {
						cuesEnd = totalSize - 1
					}
					if cuesEnd >= cuesStart {
						cuesChunk, e := fetchRangeBytesStandalone(ctx, client, trackURL, cuesStart, cuesEnd)
						if e == nil {
							cueTimes, cuePos, cueIndexStart, cueIndexEnd := parseWebMCuesFromChunk(cuesChunk, cuesStart)
							if len(cuePos) > 1 {
								idx := buildWebMIndexFromCues(cueTimes, cuePos, state.TimecodeScale, state.DurationMs, state.SegmentDataStart, totalSize, state.FirstClusterRel, cueIndexStart, cueIndexEnd)
								if hasRange(idx.InitStart, idx.InitEnd) && len(idx.Segments) > 0 {
									return idx, true
								}
							}
						}
					}
				}
			}
			if complete {
				break
			}
		}
		return mediaTrackIndex{}, false
	}

	if !isMP4LikeExt(ext) {
		return mediaTrackIndex{}, false
	}
	for size := int64(probeStepBytes); size <= int64(probeMaxBytes); size += int64(probeStepBytes) {
		chunk, err := fetchRangeBytesStandalone(ctx, client, trackURL, 0, size-1)
		if err != nil {
			return mediaTrackIndex{}, false
		}
		index, complete, err := parseMP4IndexFromPrefix(chunk)
		if err != nil {
			return mediaTrackIndex{}, false
		}
		if hasRange(index.InitStart, index.InitEnd) {
			return index, true
		}
		if complete {
			break
		}
	}
	return mediaTrackIndex{}, false
}

func fetchRangeBytesStandalone(ctx context.Context, client *http.Client, targetURL string, start, end int64) ([]byte, error) {
	if start < 0 || end < start {
		return nil, errors.New("invalid range")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSpace(targetURL), nil)
	if err != nil {
		return nil, errors.New("invalid target url")
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upstream http %d", resp.StatusCode)
	}
	limit := end - start + 1
	if limit <= 0 {
		return nil, errors.New("invalid read limit")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		data = data[:limit]
	}
	return data, nil
}

func fetchContentLengthStandalone(ctx context.Context, client *http.Client, targetURL string) (int64, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, strings.TrimSpace(targetURL), nil)
	if err != nil {
		return 0, false
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, false
	}
	if resp.ContentLength > 0 {
		return resp.ContentLength, true
	}
	return 0, false
}

type webmProbeState struct {
	SegmentDataStart int64
	CuesOffsetRel    int64
	TimecodeScale    uint64
	DurationMs       uint64
	FirstClusterRel  int64
}

func parseWebMIndexFromPrefix(prefix []byte, totalSize int64) (mediaTrackIndex, webmProbeState, bool, error) {
	_ = totalSize
	state := webmProbeState{SegmentDataStart: -1, CuesOffsetRel: -1, TimecodeScale: 1000000, DurationMs: 0, FirstClusterRel: -1}
	if len(prefix) < 16 {
		return mediaTrackIndex{}, state, false, errors.New("webm probe too short")
	}

	segIDPos := bytes.Index(prefix, []byte{0x18, 0x53, 0x80, 0x67})
	if segIDPos < 0 {
		return mediaTrackIndex{}, state, false, errors.New("webm segment not found")
	}
	_, segIDLen, ok := readEBMLID(prefix, segIDPos)
	if !ok {
		return mediaTrackIndex{}, state, false, errors.New("invalid webm segment id")
	}
	segSize, segSizeLen, segUnknown, ok := readEBMLSize(prefix, segIDPos+segIDLen)
	if !ok {
		return mediaTrackIndex{}, state, false, errors.New("invalid webm segment size")
	}
	segmentDataStart := int64(segIDPos + segIDLen + segSizeLen)
	state.SegmentDataStart = segmentDataStart

	segEnd := int64(len(prefix))
	if !segUnknown {
		segEnd = segmentDataStart + int64(segSize)
		if segEnd > int64(len(prefix)) {
			segEnd = int64(len(prefix))
		}
	}
	complete := !segUnknown
	if !segUnknown && segmentDataStart+int64(segSize) > int64(len(prefix)) {
		complete = false
	}

	var cueTimes []uint64
	var cuePos []uint64
	var cueIndexStart int64 = -1
	var cueIndexEnd int64 = -1
	cursor := segmentDataStart
	for cursor < segEnd {
		elem, ok := readEBMLElement(prefix, cursor)
		if !ok {
			complete = false
			break
		}
		if elem.End > segEnd {
			complete = false
			break
		}
		rel := elem.Offset - segmentDataStart
		switch elem.ID {
		case 0x1549A966: // Info
			if ts, dur, ok := parseWebMInfoFields(prefix, elem); ok {
				if ts > 0 {
					state.TimecodeScale = ts
				}
				if dur > 0 {
					state.DurationMs = uint64((dur * float64(state.TimecodeScale)) / 1000000.0)
				}
			}
		case 0x114D9B74: // SeekHead
			if v, ok := parseWebMCuesOffsetFromSeekHead(prefix, elem); ok {
				state.CuesOffsetRel = int64(v)
			}
		case 0x1C53BB6B: // Cues
			cueTimes, cuePos, cueIndexStart, cueIndexEnd = parseWebMCuesFromChunk(prefix[elem.Offset:elem.End], elem.Offset)
		case 0x1F43B675: // Cluster
			if state.FirstClusterRel < 0 {
				state.FirstClusterRel = rel
			}
		}
		cursor = elem.End
	}

	if len(cuePos) > 0 {
		idx := buildWebMIndexFromCues(cueTimes, cuePos, state.TimecodeScale, state.DurationMs, segmentDataStart, totalSize, state.FirstClusterRel, cueIndexStart, cueIndexEnd)
		if hasRange(idx.InitStart, idx.InitEnd) && len(idx.Segments) > 0 {
			return idx, state, complete, nil
		}
	}
	return mediaTrackIndex{}, state, complete, nil
}

type ebmlElem struct {
	ID     uint64
	Offset int64
	Data   int64
	End    int64
}

func readEBMLElement(data []byte, off int64) (ebmlElem, bool) {
	if off < 0 || off >= int64(len(data)) {
		return ebmlElem{}, false
	}
	id, idLen, ok := readEBMLID(data, int(off))
	if !ok {
		return ebmlElem{}, false
	}
	size, sizeLen, unknown, ok := readEBMLSize(data, int(off)+idLen)
	if !ok {
		return ebmlElem{}, false
	}
	dataStart := off + int64(idLen+sizeLen)
	if dataStart > int64(len(data)) {
		return ebmlElem{}, false
	}
	end := dataStart + int64(size)
	if unknown {
		end = int64(len(data))
	}
	if end < dataStart || end > int64(len(data)) {
		return ebmlElem{}, false
	}
	return ebmlElem{ID: id, Offset: off, Data: dataStart, End: end}, true
}

func readEBMLID(data []byte, off int) (uint64, int, bool) {
	if off < 0 || off >= len(data) {
		return 0, 0, false
	}
	first := data[off]
	if first == 0 {
		return 0, 0, false
	}
	mask := byte(0x80)
	length := 1
	for length <= 4 && (first&mask) == 0 {
		mask >>= 1
		length++
	}
	if length > 4 || off+length > len(data) {
		return 0, 0, false
	}
	var id uint64
	for i := 0; i < length; i++ {
		id = (id << 8) | uint64(data[off+i])
	}
	return id, length, true
}

func readEBMLSize(data []byte, off int) (uint64, int, bool, bool) {
	if off < 0 || off >= len(data) {
		return 0, 0, false, false
	}
	first := data[off]
	if first == 0 {
		return 0, 0, false, false
	}
	mask := byte(0x80)
	length := 1
	for length <= 8 && (first&mask) == 0 {
		mask >>= 1
		length++
	}
	if length > 8 || off+length > len(data) {
		return 0, 0, false, false
	}
	value := uint64(first &^ mask)
	allOnes := value == uint64(mask-1)
	for i := 1; i < length; i++ {
		b := data[off+i]
		value = (value << 8) | uint64(b)
		if b != 0xFF {
			allOnes = false
		}
	}
	return value, length, allOnes, true
}

func parseWebMInfoFields(data []byte, elem ebmlElem) (uint64, float64, bool) {
	var timescale uint64
	var duration float64
	cursor := elem.Data
	for cursor < elem.End {
		child, ok := readEBMLElement(data, cursor)
		if !ok {
			return 0, 0, false
		}
		switch child.ID {
		case 0x2AD7B1: // TimecodeScale
			timescale = parseEBMLUInt(data[child.Data:child.End])
		case 0x4489: // Duration (float)
			duration = parseEBMLFloat(data[child.Data:child.End])
		}
		cursor = child.End
	}
	if timescale == 0 {
		timescale = 1000000
	}
	return timescale, duration, true
}

func parseWebMCuesOffsetFromSeekHead(data []byte, elem ebmlElem) (uint64, bool) {
	cursor := elem.Data
	for cursor < elem.End {
		seek, ok := readEBMLElement(data, cursor)
		if !ok {
			return 0, false
		}
		if seek.ID == 0x4DBB { // Seek
			var seekID uint64
			var seekPos uint64
			inner := seek.Data
			for inner < seek.End {
				child, ok := readEBMLElement(data, inner)
				if !ok {
					break
				}
				switch child.ID {
				case 0x53AB: // SeekID
					seekID = parseEBMLUInt(data[child.Data:child.End])
				case 0x53AC: // SeekPosition
					seekPos = parseEBMLUInt(data[child.Data:child.End])
				}
				inner = child.End
			}
			if seekID == 0x1C53BB6B && seekPos > 0 {
				return seekPos, true
			}
		}
		cursor = seek.End
	}
	return 0, false
}

func parseWebMCuesFromChunk(data []byte, chunkStart int64) ([]uint64, []uint64, int64, int64) {
	cuesPos := bytes.Index(data, []byte{0x1C, 0x53, 0xBB, 0x6B})
	if cuesPos < 0 {
		return nil, nil, -1, -1
	}
	cuesElem, ok := readEBMLElement(data, int64(cuesPos))
	if !ok {
		return nil, nil, -1, -1
	}
	type cueEntry struct {
		time  uint64
		pos   uint64
		track uint64
	}
	entries := make([]cueEntry, 0, 1024)
	cursor := cuesElem.Data
	for cursor < cuesElem.End {
		point, ok := readEBMLElement(data, cursor)
		if !ok {
			break
		}
		if point.ID == 0xBB { // CuePoint
			var t uint64
			inner := point.Data
			for inner < point.End {
				child, ok := readEBMLElement(data, inner)
				if !ok {
					break
				}
				switch child.ID {
				case 0xB3: // CueTime
					t = parseEBMLUInt(data[child.Data:child.End])
				case 0xB7: // CueTrackPositions
					pp := child.Data
					var clusterPos uint64
					var relPos uint64
					var trackNo uint64
					hasClusterPos := false
					hasRelPos := false
					for pp < child.End {
						cp, ok := readEBMLElement(data, pp)
						if !ok {
							break
						}
						switch cp.ID {
						case 0xF1: // CueClusterPosition
							clusterPos = parseEBMLUInt(data[cp.Data:cp.End])
							hasClusterPos = true
						case 0xF0: // CueRelativePosition
							relPos = parseEBMLUInt(data[cp.Data:cp.End])
							hasRelPos = true
						case 0xF7: // CueTrack
							trackNo = parseEBMLUInt(data[cp.Data:cp.End])
						}
						pp = cp.End
					}
					if hasClusterPos {
						pos := clusterPos
						if hasRelPos {
							pos += relPos
						}
						entries = append(entries, cueEntry{time: t, pos: pos, track: trackNo})
					}
				}
				inner = child.End
			}
		}
		cursor = point.End
	}

	if len(entries) == 0 {
		return nil, nil, chunkStart + int64(cuesElem.Offset), chunkStart + int64(cuesElem.End-1)
	}
	trackCount := map[uint64]int{}
	for _, e := range entries {
		if e.track == 0 {
			continue
		}
		trackCount[e.track]++
	}
	selectedTrack := uint64(0)
	best := -1
	for t, c := range trackCount {
		if c > best {
			selectedTrack = t
			best = c
		}
	}

	times := make([]uint64, 0, len(entries))
	pos := make([]uint64, 0, len(entries))
	filtered := make([]cueEntry, 0, len(entries))
	for _, e := range entries {
		if selectedTrack != 0 && e.track != selectedTrack {
			continue
		}
		filtered = append(filtered, e)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].time != filtered[j].time {
			return filtered[i].time < filtered[j].time
		}
		return filtered[i].pos < filtered[j].pos
	})
	lastPos := uint64(0)
	for i, e := range filtered {
		if i > 0 && e.pos == lastPos {
			continue
		}
		times = append(times, e.time)
		pos = append(pos, e.pos)
		lastPos = e.pos
	}
	return times, pos, chunkStart + int64(cuesElem.Offset), chunkStart + int64(cuesElem.End-1)
}

func parseEBMLUInt(raw []byte) uint64 {
	var out uint64
	for _, b := range raw {
		out = (out << 8) | uint64(b)
	}
	return out
}

func parseEBMLFloat(raw []byte) float64 {
	switch len(raw) {
	case 4:
		bits := binary.BigEndian.Uint32(raw)
		return float64(math.Float32frombits(bits))
	case 8:
		bits := binary.BigEndian.Uint64(raw)
		return math.Float64frombits(bits)
	default:
		return 0
	}
}

func buildWebMIndexFromCues(cueTimes, cuePos []uint64, timecodeScale uint64, durationMs uint64, segmentDataStart, totalSize int64, firstClusterRel int64, cueIndexStart, cueIndexEnd int64) mediaTrackIndex {
	if len(cuePos) == 0 {
		return mediaTrackIndex{}
	}
	if timecodeScale == 0 {
		timecodeScale = 1000000
	}
	segments := make([]dashSegment, 0, len(cuePos))
	for i := 0; i < len(cuePos); i++ {
		startRel := int64(cuePos[i])
		if i == 0 && firstClusterRel >= 0 && firstClusterRel < startRel {
			startRel = firstClusterRel
		}
		start := segmentDataStart + startRel
		if start < 0 || start >= totalSize {
			continue
		}
		end := totalSize - 1
		if i+1 < len(cuePos) {
			nextStart := segmentDataStart + int64(cuePos[i+1])
			if nextStart > start {
				end = nextStart - 1
			}
		}
		var dur uint32
		if i+1 < len(cueTimes) && i < len(cueTimes) {
			delta := cueTimes[i+1] - cueTimes[i]
			if delta > 0 {
				dur = uint32((delta * timecodeScale) / 1000000)
			}
		} else if i < len(cueTimes) && durationMs > 0 {
			curMs := (cueTimes[i] * timecodeScale) / 1000000
			if durationMs > curMs {
				dur = uint32(durationMs - curMs)
			}
		}
		segTime := uint64(0)
		if i < len(cueTimes) {
			segTime = cueTimes[i]
		}
		segments = append(segments, dashSegment{Start: start, End: end, Time: segTime, Duration: dur})
	}
	if len(segments) == 0 {
		return mediaTrackIndex{}
	}
	initEnd := segments[0].Start - 1
	if firstClusterRel >= 0 {
		clusterInitEnd := (segmentDataStart + firstClusterRel) - 1
		if clusterInitEnd > initEnd {
			initEnd = clusterInitEnd
		}
	}
	if initEnd < 0 {
		initEnd = 0
	}
	idx := mediaTrackIndex{
		InitStart: 0,
		InitEnd:   initEnd,
		Timescale: 1000,
		Segments:  segments,
	}
	if hasRange(cueIndexStart, cueIndexEnd) {
		idx.IndexStart = cueIndexStart
		idx.IndexEnd = cueIndexEnd
	}
	return idx
}
