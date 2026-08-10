package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr                string
	ReadTimeout               time.Duration
	WriteTimeout              time.Duration
	IdleTimeout               time.Duration
	HitachiEndpoint           string
	HitachiAccessKeyID        string
	HitachiSecretAccessKey    string
	HitachiRegion             string
	HitachiBucket             string
	HitachiUsePathStyle       bool
	HitachiInsecureSkipVerify bool
	SkipStorageHealthCheck    bool
	InternalRoot              string
	HTTPTimeout               time.Duration
	TempDir                   string
	DownloadCacheDir          string
	DownloadCacheMaxFiles     int
	MaxUploadBytes            int64
	PDFFallbackDPI            int
	WorkerID                  int
}

func Load() (Config, error) {
	tempDir := getEnv("TEMP_DIR", os.TempDir())

	cfg := Config{
		ListenAddr:                getEnv("LISTEN_ADDR", ":9082"),
		ReadTimeout:               getDurationEnv("READ_TIMEOUT", 30*time.Second),
		WriteTimeout:              getDurationEnv("WRITE_TIMEOUT", 5*time.Minute),
		IdleTimeout:               getDurationEnv("IDLE_TIMEOUT", 60*time.Second),
		HitachiEndpoint:           getEnv("HITACHI_ENDPOINT", ""),
		HitachiAccessKeyID:        getEnv("HITACHI_ACCESS_KEY_ID", ""),
		HitachiSecretAccessKey:    getEnv("HITACHI_SECRET_ACCESS_KEY", ""),
		HitachiRegion:             getEnv("HITACHI_REGION", "us-east-1"),
		HitachiBucket:             getEnv("HITACHI_BUCKET", "dms-connector"),
		HitachiUsePathStyle:       getBoolEnv("HITACHI_USE_PATH_STYLE", true),
		HitachiInsecureSkipVerify: getBoolEnv("HITACHI_INSECURE_SKIP_VERIFY", false),
		SkipStorageHealthCheck:    getBoolEnv("HITACHI_SKIP_STORAGE_HEALTH_CHECK", false),
		InternalRoot:              strings.Trim(strings.TrimRight(getEnv("HITACHI_OBJECT_PREFIX", "/connector/dms"), "/"), "/"),
		HTTPTimeout:               getDurationEnv("HITACHI_HTTP_TIMEOUT", 60*time.Second),
		TempDir:                   tempDir,
		DownloadCacheDir:          getEnv("DOWNLOAD_CACHE_DIR", filepath.Join(tempDir, "dms-download-cache")),
		DownloadCacheMaxFiles:     getIntEnv("DOWNLOAD_CACHE_MAX_FILES", 128),
		MaxUploadBytes:            getInt64Env("MAX_UPLOAD_BYTES", 64<<20),
		PDFFallbackDPI:            getIntEnv("PDF_RENDER_DPI", 144),
		WorkerID:                  getIntEnv("WORKER_ID", 0),
	}

	if cfg.HitachiEndpoint == "" {
		return cfg, fmt.Errorf("HITACHI_ENDPOINT must be set")
	}
	if cfg.HitachiAccessKeyID == "" {
		return cfg, fmt.Errorf("HITACHI_ACCESS_KEY_ID must be set")
	}
	if cfg.HitachiSecretAccessKey == "" {
		return cfg, fmt.Errorf("HITACHI_SECRET_ACCESS_KEY must be set")
	}
	if cfg.HitachiBucket == "" {
		return cfg, fmt.Errorf("HITACHI_BUCKET must be set")
	}

	return cfg, nil
}

func getBoolEnv(key string, fallback bool) bool {
	if value := os.Getenv(key); value != "" {
		value = strings.ToLower(strings.TrimSpace(value))
		return value == "1" || value == "true" || value == "yes" || value == "on"
	}

	return fallback
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}

	return fallback
}

func getInt64Env(key string, fallback int64) int64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
	}

	return fallback
}

func getIntEnv(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}

	return fallback
}
