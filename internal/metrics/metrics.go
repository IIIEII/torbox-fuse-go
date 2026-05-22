// Package metrics provides atomic counters for monitoring the TorBox FUSE
// filesystem and an HTTP server for exposing them.
package metrics

import (
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

// Snapshot returns a JSON-serializable map of all current metric values,
// including GoroutineCount from runtime.NumGoroutine().
func (m *Metrics) Snapshot() map[string]interface{} {
	snapshot := map[string]interface{}{
		"catalog_items":          m.CatalogItems.Load(),
		"cache_bytes_total":      m.CacheBytesTotal.Load(),
		"cache_bytes_active":     m.CacheBytesActive.Load(),
		"cache_bytes_stale":      m.CacheBytesStale.Load(),
		"cache_entries":          m.CacheEntries.Load(),
		"inflight_windows":       m.InflightWindows.Load(),
		"read_count":             m.ReadCount.Load(),
		"cache_hit_count":        m.CacheHitCount.Load(),
		"stream_miss_count":      m.StreamMissCount.Load(),
		"stream_join_count":      m.StreamJoinCount.Load(),
		"cancelled_stream_count": m.CancelledStreamCount.Load(),
		"api_call_count":         m.APICallCount.Load(),
		"refresh_count":          m.RefreshCount.Load(),
		"goroutine_count":       int64(runtime.NumGoroutine()),
	}

	// Collect CDN status code counts.
	cdnCodes := make(map[string]int64)
	m.CDNStatusCodes.Range(func(key, value interface{}) bool {
		code := key.(int)
		counter := value.(*atomic.Int64)
		cdnCodes[statusCodeFieldName(code)] = counter.Load()
		return true
	})
	if len(cdnCodes) > 0 {
		snapshot["cdn_status_codes"] = cdnCodes
	}

	return snapshot
}

// statusCodeFieldName converts an HTTP status code to a field name like
// "cdn_200", "cdn_403", etc.
func statusCodeFieldName(code int) string {
	return "cdn_" + intToStr(code)
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