// Package metrics provides atomic counters for monitoring the TorBox FUSE
// filesystem and an HTTP server for exposing them.
package metrics

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
)

// Metrics holds atomic counters for all spec-defined monitoring points.
// All fields are safe for concurrent use.
type Metrics struct {
	CatalogItems          atomic.Int64
	CacheBytesTotal       atomic.Int64
	CacheBytesActive      atomic.Int64
	CacheBytesStale       atomic.Int64
	CacheEntries          atomic.Int64
	InflightWindows       atomic.Int64
	ReadCount            atomic.Int64
	CacheHitCount        atomic.Int64
	StreamMissCount      atomic.Int64
	StreamJoinCount      atomic.Int64
	CancelledStreamCount atomic.Int64
	APICallCount         atomic.Int64
	RefreshCount         atomic.Int64
	CDNStatusCodes       sync.Map // map[int]*atomic.Int64
}

// New creates a new Metrics instance with all counters initialised to zero.
func New() *Metrics {
	return &Metrics{}
}

// IncCDNStatusCode increments the counter for the given HTTP status code.
func (m *Metrics) IncCDNStatusCode(code int) {
	for {
		v, loaded := m.CDNStatusCodes.LoadOrStore(code, new(atomic.Int64))
		counter := v.(*atomic.Int64)
		if loaded {
			counter.Add(1)
			return
		}
		// We just stored a new counter; increment from 0 to 1.
		counter.Add(1)
		return
	}
}

// WritePrometheus writes all metric values in Prometheus exposition text format
// to w. Labels are not used — each metric is a simple untyped gauge or counter.
func (m *Metrics) WritePrometheus(w io.Writer) {
	writeCounter(w, "torbox_catalog_items", m.CatalogItems.Load())
	writeCounter(w, "torbox_cache_bytes_total", m.CacheBytesTotal.Load())
	writeGauge(w, "torbox_cache_bytes_active", m.CacheBytesActive.Load())
	writeGauge(w, "torbox_cache_bytes_stale", m.CacheBytesStale.Load())
	writeCounter(w, "torbox_cache_entries", m.CacheEntries.Load())
	writeGauge(w, "torbox_inflight_windows", m.InflightWindows.Load())
	writeCounter(w, "torbox_read_count_total", m.ReadCount.Load())
	writeCounter(w, "torbox_cache_hit_count_total", m.CacheHitCount.Load())
	writeCounter(w, "torbox_stream_miss_count_total", m.StreamMissCount.Load())
	writeCounter(w, "torbox_stream_join_count_total", m.StreamJoinCount.Load())
	writeCounter(w, "torbox_cancelled_stream_count_total", m.CancelledStreamCount.Load())
	writeCounter(w, "torbox_api_call_count_total", m.APICallCount.Load())
	writeCounter(w, "torbox_refresh_count_total", m.RefreshCount.Load())
	writeGauge(w, "torbox_goroutine_count", int64(runtime.NumGoroutine()))

	m.CDNStatusCodes.Range(func(key, value interface{}) bool {
		code := key.(int)
		counter := value.(*atomic.Int64)
		fmt.Fprintf(w, "torbox_cdn_response_count_total{code=%q} %d\n", statusCodeLabel(code), counter.Load())
		return true
	})
}

func writeCounter(w io.Writer, name string, value int64) {
	fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", name, name, value)
}

func writeGauge(w io.Writer, name string, value int64) {
	fmt.Fprintf(w, "# TYPE %s gauge\n%s %d\n", name, name, value)
}

func statusCodeLabel(code int) string {
	return intToStr(code)
}

func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte // big enough for any 32-bit int
	pos := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}