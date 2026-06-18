package config

import (
	"os"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	os.Clearenv()
	os.Setenv("TORBOX_API_KEY", "test-key-123")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.APIKey != "test-key-123" {
		t.Errorf("APIKey = %q, want test-key-123", cfg.APIKey)
	}
	if cfg.APIBaseURL != "https://api.torbox.app/v1/api" {
		t.Errorf("APIBaseURL = %q, want https://api.torbox.app/v1/api", cfg.APIBaseURL)
	}
	if cfg.MountPath != "/mnt/torbox" {
		t.Errorf("MountPath = %q, want /mnt/torbox", cfg.MountPath)
	}
	if cfg.CacheBudgetMB != 256 {
		t.Errorf("CacheBudgetMB = %d, want 256", cfg.CacheBudgetMB)
	}
	if cfg.PrefetchWindowMB != 16 {
		t.Errorf("PrefetchWindowMB = %d, want 16", cfg.PrefetchWindowMB)
	}
	if cfg.StreamMaxInflight != 2 {
		t.Errorf("StreamMaxInflight = %d, want 2", cfg.StreamMaxInflight)
	}
	if cfg.StreamConcurrency != 8 {
		t.Errorf("StreamConcurrency = %d, want 8", cfg.StreamConcurrency)
	}
	if cfg.AttrTimeoutSec != 1 {
		t.Errorf("AttrTimeoutSec = %d, want 1", cfg.AttrTimeoutSec)
	}
	if cfg.EntryTimeoutSec != 1 {
		t.Errorf("EntryTimeoutSec = %d, want 1", cfg.EntryTimeoutSec)
	}
	if cfg.CDNURLCacheTTLSec != 300 {
		t.Errorf("CDNURLCacheTTLSec = %d, want 300", cfg.CDNURLCacheTTLSec)
	}
	if cfg.StreamMaxGlobalWindows != 16 {
		t.Errorf("StreamMaxGlobalWindows = %d, want 16", cfg.StreamMaxGlobalWindows)
	}
	if cfg.CatalogRefreshInterval != 3*time.Hour {
		t.Errorf("CatalogRefreshInterval = %v, want 3h", cfg.CatalogRefreshInterval)
	}
	if cfg.MetricsListenAddr != "127.0.0.1:9080" {
		t.Errorf("MetricsListenAddr = %q, want 127.0.0.1:9080", cfg.MetricsListenAddr)
	}
	if cfg.StateDBPath != "/config/state.db" {
		t.Errorf("StateDBPath = %q, want /config/state.db", cfg.StateDBPath)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want info", cfg.LogLevel)
	}
}

func TestConfigMissingAPIKey(t *testing.T) {
	os.Clearenv()
	_, err := Load()
	if err == nil {
		t.Fatal("Load() should error when TORBOX_API_KEY is missing")
	}
}

func TestConfigCustomValues(t *testing.T) {
	os.Clearenv()
	os.Setenv("TORBOX_API_KEY", "mykey")
	os.Setenv("FUSE_MOUNT_PATH", "/media/torbox")
	os.Setenv("FUSE_CACHE_BUDGET_MB", "512")
	os.Setenv("CATALOG_REFRESH_INTERVAL", "1h")
	os.Setenv("LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.MountPath != "/media/torbox" {
		t.Errorf("MountPath = %q, want /media/torbox", cfg.MountPath)
	}
	if cfg.CacheBudgetMB != 512 {
		t.Errorf("CacheBudgetMB = %d, want 512", cfg.CacheBudgetMB)
	}
	if cfg.CatalogRefreshInterval != 1*time.Hour {
		t.Errorf("CatalogRefreshInterval = %v, want 1h", cfg.CatalogRefreshInterval)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug", cfg.LogLevel)
	}
}

func TestConfigDashboardAuth(t *testing.T) {
	os.Clearenv()
	os.Setenv("TORBOX_API_KEY", "key")

	t.Run("no auth by default", func(t *testing.T) {
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.DashboardUsername != "" {
			t.Errorf("DashboardUsername = %q, want empty", cfg.DashboardUsername)
		}
		if cfg.DashboardPassword != "" {
			t.Errorf("DashboardPassword = %q, want empty", cfg.DashboardPassword)
		}
	})

	t.Run("auth configured via env", func(t *testing.T) {
		os.Setenv("DASHBOARD_USERNAME", "admin")
		os.Setenv("DASHBOARD_PASSWORD", "secret")
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.DashboardUsername != "admin" {
			t.Errorf("DashboardUsername = %q, want admin", cfg.DashboardUsername)
		}
		if cfg.DashboardPassword != "secret" {
			t.Errorf("DashboardPassword = %q, want secret", cfg.DashboardPassword)
		}
	})
}

func TestConfigWritable(t *testing.T) {
	os.Clearenv()
	os.Setenv("TORBOX_API_KEY", "key")

	t.Run("writable off by default", func(t *testing.T) {
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.Writable {
			t.Error("Writable = true, want false by default")
		}
	})

	t.Run("writable enabled via env", func(t *testing.T) {
		os.Setenv("FUSE_WRITABLE", "true")
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if !cfg.Writable {
			t.Error("Writable = false, want true")
		}
	})
}
