package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultAddress = "127.0.0.1:8088"
	DefaultDataDir = "./data"
)

// Config contains process-level settings that are resolved before any
// repository or HTTP server is created.
type Config struct {
	Address            string
	DataDir            string
	ReadHeaderTimeout  time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	IdleTimeout        time.Duration
	ShutdownTimeout    time.Duration
	SchedulerInterval  time.Duration
	SchedulerBatchSize int
}

func FromEnv() (Config, error) {
	interval, err := durationOrDefault("CELLAR_CONTROL_SCHEDULER_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	batch, err := intOrDefault("CELLAR_CONTROL_SCHEDULER_BATCH", 32)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Address:            valueOrDefault("CELLAR_CONTROL_ADDR", DefaultAddress),
		DataDir:            valueOrDefault("CELLAR_CONTROL_DATA_DIR", DefaultDataDir),
		ReadHeaderTimeout:  5 * time.Second,
		ReadTimeout:        15 * time.Second,
		WriteTimeout:       20 * time.Second,
		IdleTimeout:        60 * time.Second,
		ShutdownTimeout:    10 * time.Second,
		SchedulerInterval:  interval,
		SchedulerBatchSize: batch,
	}
	return cfg.Validate()
}

func (c Config) Validate() (Config, error) {
	c.Address = strings.TrimSpace(c.Address)
	c.DataDir = strings.TrimSpace(c.DataDir)
	if c.Address == "" {
		return Config{}, fmt.Errorf("address cannot be empty")
	}
	if c.DataDir == "" {
		return Config{}, fmt.Errorf("data directory cannot be empty")
	}
	absolute, err := filepath.Abs(c.DataDir)
	if err != nil {
		return Config{}, fmt.Errorf("resolve data directory: %w", err)
	}
	c.DataDir = filepath.Clean(absolute)
	if c.ReadHeaderTimeout <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0 {
		return Config{}, fmt.Errorf("HTTP timeouts must be positive")
	}
	if c.IdleTimeout <= 0 || c.ShutdownTimeout <= 0 {
		return Config{}, fmt.Errorf("idle and shutdown timeouts must be positive")
	}
	if c.SchedulerInterval < 0 {
		return Config{}, fmt.Errorf("scheduler interval cannot be negative")
	}
	if c.SchedulerBatchSize < 1 {
		return Config{}, fmt.Errorf("scheduler batch size must be positive")
	}
	return c, nil
}

func (c Config) EnsureDataDir() error {
	if err := os.MkdirAll(c.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	return nil
}

func valueOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func durationOrDefault(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", name, err)
	}
	return parsed, nil
}

func intOrDefault(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return parsed, nil
}
