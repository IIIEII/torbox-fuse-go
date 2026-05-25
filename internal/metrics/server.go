package metrics

import (
	"context"
	"log/slog"
	"net"
	"net/http"
)

// Server exposes operational metrics and control endpoints over HTTP.
// Use NewServer to create one for production, or NewHandler to get an
// http.Handler for testing with httptest.NewServer.
type Server struct {
	metrics    *Metrics
	mux        *http.ServeMux
	refreshFn  func(ctx context.Context) error
	httpServer *http.Server
	addr       string // actual listen addr after Start
	listenAddr string // configured listen addr
}

// NewServer creates a metrics HTTP server that will listen on the given address.
// The refreshFn is called by the POST /refresh endpoint; it may be nil.
func NewServer(m *Metrics, listenAddr string, refreshFn func(ctx context.Context) error) *Server {
	s := &Server{
		metrics:    m,
		refreshFn:  refreshFn,
		listenAddr: listenAddr,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/refresh", s.handleRefresh)
	s.mux = mux

	return s
}

// NewHandler returns an http.Handler with the metrics, healthz, and refresh
// endpoints. This is useful for testing with httptest.NewServer.
func NewHandler(m *Metrics, refreshFn func(ctx context.Context) error) http.Handler {
	s := &Server{
		metrics:   m,
		refreshFn: refreshFn,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/refresh", s.handleRefresh)
	s.mux = mux

	return mux
}

// Start starts the HTTP server on the configured listen address in a
// background goroutine. Call Addr() to get the actual listen address.
func (s *Server) Start() error {
	if s.httpServer != nil {
		return nil
	}

	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	s.addr = ln.Addr().String()

	s.httpServer = &http.Server{
		Addr:    s.addr,
		Handler: s.mux,
	}

	go func() {
		slog.Info("metrics server listening", "addr", s.addr)
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()

	return nil
}

// Addr returns the actual address the server is listening on.
// Only valid after Start has been called.
func (s *Server) Addr() string {
	return s.addr
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

// handleMetrics returns all metric counters in Prometheus exposition format.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	s.metrics.WritePrometheus(w)
}

// handleHealthz returns a simple health check.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleRefresh triggers a catalog refresh via POST.
func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.refreshFn == nil {
		http.Error(w, "refresh not configured", http.StatusServiceUnavailable)
		return
	}

	if err := s.refreshFn(r.Context()); err != nil {
		if err.Error() == "refresh already in progress" {
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte("already in progress"))
			return
		}
		slog.Error("refresh failed", "err", err)
		http.Error(w, "refresh failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}