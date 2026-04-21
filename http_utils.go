package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

func methodNotAllowed(w http.ResponseWriter) {
	writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "message": "method not allowed"})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,HEAD,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization,Range")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *loggingResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *loggingResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w}
		next.ServeHTTP(lrw, r)
		if lrw.status == 0 {
			lrw.status = http.StatusOK
		}
		costMS := time.Since(start).Milliseconds()
		reqRange := strings.TrimSpace(r.Header.Get("Range"))
		respRange := strings.TrimSpace(lrw.Header().Get("Content-Range"))
		respLen := strings.TrimSpace(lrw.Header().Get("Content-Length"))
		if reqRange != "" || respRange != "" {
			log.Printf("[yt-bridge] method=%s path=%s status=%d cost_ms=%d req_range=%q resp_range=%q resp_len=%q", r.Method, r.URL.Path, lrw.status, costMS, reqRange, respRange, respLen)
			return
		}
		log.Printf("[yt-bridge] method=%s path=%s status=%d cost_ms=%d", r.Method, r.URL.Path, lrw.status, costMS)
	})
}
