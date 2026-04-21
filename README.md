# yt-bridge

单入口播放桥接（Go）。

核心规则（单链路）：
- `yt-dlp -J` 只执行一次（每个 token 初始化一次）。
- 按最高画质（分辨率优先，其次码率；同分辨率优先 MP4）选主视频轨。
- 若主视频轨自带音频：输出 HLS（`play.m3u8`）。
- 若主视频轨是纯视频：输出 DASH（`play.mpd`，`isoff-on-demand` + `SegmentList/Timeline/mediaRange`）。

## 路由

1. `POST /register`（或 `GET /register?url=...`）
- 立即完成初始化（不是延迟到首次播放）
- 返回：`url`/`play_url`，只返回一个最终播放地址：
  - HLS 模式：`/{token}/play.m3u8`
  - DASH 模式：`/{token}/play.mpd`

2. `GET|HEAD /{token}/play.m3u8`
- 仅 HLS 模式可用

3. `GET|HEAD /{token}/play.mpd`
- 仅 DASH 模式可用

4. `GET|HEAD /{token}/video_range`
- 视频直链透传（支持 `Range`，仅 DASH 可用）

5. `GET|HEAD /{token}/audio_range`
- 音频直链透传（支持 `Range`，仅 DASH 可用）

6. `GET|HEAD /{token}/segment/{id}`
- 分片透传（仅 HLS 可用）

7. `GET /healthz`
- 返回运行信息

## 运行目录（固定）

运行目录固定为进程当前工作目录（`cwd`）。

需要文件：
- `./yt-dlp`
- `./www.youtube.com_cookies.txt`（可选）

## 启动

```bash
cd /root/mycode/yt-bridge
go build -o build/yt-bridge .
cd build
./yt-bridge
```

## 环境变量

- `YT_BRIDGE_ADDR`（可选，默认 `:8080`）

## 快速测试

```bash
curl -sS -X POST 'http://127.0.0.1:8080/register' \
  -H 'content-type: application/json' \
  -d '{"url":"https://www.youtube.com/watch?v=Q90Oea5MpOA"}'
```
