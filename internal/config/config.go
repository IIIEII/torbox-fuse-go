package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	APIKey                 string
	APIBaseURL             string
	MountPath              string
	CacheBudgetMB          int
	PrefetchWindowMB       int
	StreamMaxInflight      int
	StreamConcurrency      int
	AttrTimeoutSec        int
	EntryTimeoutSec       int
	CatalogRefreshInterval time.Duration
	MetricsListenAddr      string
	StateDBPath            string
	LogLevel               string
	UID                   uint32
	GID                   uint32
}

func env(key string, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// TorboxConfig returns a torbox.Config implementation backed by this Config.
// This is needed because Config has exported struct fields (accessed directly
// by other packages) that would conflict with method names required by the
// torbox.Config interface.
func (c *Config) TorboxConfig() torboxConfig {
	return torboxConfig{c}
}

// torboxConfig adapts *Config to implement the torbox.Config interface
// without introducing field/method name conflicts on the Config struct itself.
type torboxConfig struct {
	*Config
}

func (t torboxConfig) APIKey() string     { return t.Config.APIKey }
func (t torboxConfig) APIBaseURL() string  { return t.Config.APIBaseURL }
func (t torboxConfig) APITimeout() time.Duration { return 60 * time.Second }

func Load() (*Config, error) {
	apiKey := os.Getenv("TORBOX_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("TORBOX_API_KEY is required")
	}

	return &Config{
		APIKey:                 apiKey,
		APIBaseURL:             env("TORBOX_API_BASE_URL", "https://api.torbox.app/v1/api"),
		MountPath:              env("FUSE_MOUNT_PATH", "/mnt/torbox"),
		CacheBudgetMB:          envInt("FUSE_CACHE_BUDGET_MB", 256),
		PrefetchWindowMB:       envInt("FUSE_PREFETCH_WINDOW_MB", 16),
		StreamMaxInflight:      envInt("FUSE_STREAM_MAX_INFLIGHT", 2),
		StreamConcurrency:      envInt("FUSE_STREAM_CONCURRENCY", 8),
		AttrTimeoutSec:        envInt("FUSE_ATTR_TIMEOUT_SEC", 1),
		EntryTimeoutSec:       envInt("FUSE_ENTRY_TIMEOUT_SEC", 1),
		CatalogRefreshInterval: envDuration("CATALOG_REFRESH_INTERVAL", 3*time.Hour),
		MetricsListenAddr:      env("METRICS_LISTEN_ADDR", "127.0.0.1:9080"),
		StateDBPath:            env("STATE_DB_PATH", "/config/state.db"),
		LogLevel:               env("LOG_LEVEL", "info"),
		UID:                   uint32(os.Getuid()),
		GID:                   uint32(os.Getgid()),
	}, nil
}