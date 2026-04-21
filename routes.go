package main

import "net/http"

func registerRoutes(mux *http.ServeMux, a *app) {
	mux.HandleFunc("/home", a.handleHome)
	mux.HandleFunc("/category", a.handleCategory)
	mux.HandleFunc("/detail", a.handleDetail)
	mux.HandleFunc("/play", a.handlePlay)
	mux.HandleFunc("/proxy/image", writeNotFound)
	mux.HandleFunc("/proxy/image/", a.handleImageProxy)
	mux.HandleFunc("/proxy/url/", a.handleURLProxy)
	mux.HandleFunc("/proxy/", a.handleProxy)
}

func writeNotFound(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "message": "not found"})
}
