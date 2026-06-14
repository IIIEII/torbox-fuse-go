// Package dashboard provides a web-based real-time visualization of the
// TorBox FUSE streaming cache state. It serves an HTML dashboard on the
// existing metrics HTTP server with SSE-based live updates.
package dashboard

import (
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

//go:embed static/index.html static/style.css static/app.js
var staticFS embed.FS

var (
	indexHTML = mustReadStatic("static/index.html")
	styleCSS  = mustReadStatic("static/style.css")
	appJS     = mustReadStatic("static/app.js")
)

func mustReadStatic(name string) []byte {
	data, err := staticFS.ReadFile(name)
	if err != nil {
		panic("dashboard: embed " + name + ": " + err.Error())
	}
	return data
}

// Server handles HTTP requests for the dashboard UI and API endpoints.
type Server struct {
	dashboard *Dashboard
}

// NewServer creates a dashboard server backed by the given Dashboard.
func NewServer(d *Dashboard) *Server {
	return &Server{dashboard: d}
}

// RegisterRoutes registers dashboard routes on the given ServeMux.
// This adds:
//   - GET /              → HTML dashboard page
//   - GET /api/state     → SSE stream of JSON snapshots (updates every 500ms)
//   - GET /api/snapshot  → single JSON snapshot (for debugging)
//   - GET /api/hidden    → list downloads with hidden files
//   - POST /api/unhide   → unhide a download's files {download_kind, download_id}
//   - POST /api/delete   → force-delete a download from TorBox {download_kind, download_id}
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/style.css", s.handleCSS)
	mux.HandleFunc("/app.js", s.handleJS)
	mux.HandleFunc("/api/state", s.handleSSE)
	mux.HandleFunc("/api/snapshot", s.handleSnapshot)
	mux.HandleFunc("/api/hidden", s.handleHidden)
	mux.HandleFunc("/api/unhide", s.handleUnhide)
	mux.HandleFunc("/api/delete", s.handleDelete)
}

// handleIndex serves the embedded dashboard HTML page.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Only match exact "/" path, not "/anything-else".
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

// handleCSS serves the embedded dashboard stylesheet.
func (s *Server) handleCSS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	w.Write(styleCSS)
}

// handleJS serves the embedded dashboard JavaScript.
func (s *Server) handleJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Write(appJS)
}

// handleSnapshot returns a single JSON snapshot of the current state.
// This is useful for debugging and one-shot queries.
func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snap := s.dashboard.Snapshot()

	w.Header().Set("Content-Type", "application/json")
	data, err := json.Marshal(snap)
	if err != nil {
		http.Error(w, "marshal error", http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

// handleSSE pushes JSON snapshots to the client via Server-Sent Events.
// The connection stays open and pushes a snapshot every 500ms until the
// client disconnects.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			snap := s.dashboard.Snapshot()
			data, err := json.Marshal(snap)
			if err != nil {
				slog.Error("dashboard marshal snapshot", "err", err)
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// handleHidden returns a list of downloads that have hidden files.
func (s *Server) handleHidden(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := s.dashboard.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	data, err := json.Marshal(snap.HiddenDownloads)
	if err != nil {
		http.Error(w, "marshal error", http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

// handleUnhide restores hidden files for a download.
// Expects JSON: {"download_kind": "torrent", "download_id": "12345"}
func (s *Server) handleUnhide(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		DownloadKind string `json:"download_kind"`
		DownloadID   string `json:"download_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.DownloadKind == "" || req.DownloadID == "" {
		http.Error(w, "download_kind and download_id are required", http.StatusBadRequest)
		return
	}
	if err := s.dashboard.UnhideDownload(r.Context(), req.DownloadKind, req.DownloadID); err != nil {
		slog.Error("dashboard: unhide download", "kind", req.DownloadKind, "id", req.DownloadID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleDelete force-deletes a download from TorBox.
// Expects JSON: {"download_kind": "torrent", "download_id": "12345"}
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		DownloadKind string `json:"download_kind"`
		DownloadID   string `json:"download_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.DownloadKind == "" || req.DownloadID == "" {
		http.Error(w, "download_kind and download_id are required", http.StatusBadRequest)
		return
	}
	if err := s.dashboard.DeleteDownload(r.Context(), req.DownloadKind, req.DownloadID); err != nil {
		slog.Error("dashboard: delete download", "kind", req.DownloadKind, "id", req.DownloadID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
