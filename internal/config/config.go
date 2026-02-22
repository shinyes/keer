package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type StorageBackend string

const (
	StorageBackendLocal StorageBackend = "local"
	StorageBackendS3    StorageBackend = "s3"
)

type S3Config struct {
	Endpoint     string
	Region       string
	Bucket       string
	AccessKeyID  string
	AccessSecret string
	UsePathStyle bool
}

type Config struct {
	Addr              string
	BaseURL           string
	DBPath            string
	UploadsDir        string
	Version           string
	Storage           StorageBackend
	S3                S3Config
	AllowRegistration bool
	BootstrapUser     string
	BootstrapToken    string
}

func Load() (Config, error) {
	cfg := Config{
		Addr:              env("APP_ADDR", ":8080"),
		BaseURL:           strings.TrimRight(env("BASE_URL", "http://localhost:8080"), "/"),
		DBPath:            env("DB_PATH", "./data/keer.db"),
		UploadsDir:        env("UPLOADS_DIR", "./data/uploads"),
		Version:           env("MEMOS_VERSION", "0.26.1"),
		Storage:           StorageBackend(strings.ToLower(env("STORAGE_BACKEND", "local"))),
		AllowRegistration: envBool("ALLOW_REGISTRATION", true),
		BootstrapUser:     env("BOOTSTRAP_USER", "demo"),
		BootstrapToken:    env("BOOTSTRAP_TOKEN", ""),
		S3: S3Config{
			Endpoint:     env("S3_ENDPOINT", ""),
			Region:       env("S3_REGION", ""),
			Bucket:       env("S3_BUCKET", ""),
			AccessKeyID:  env("S3_ACCESS_KEY_ID", ""),
			AccessSecret: env("S3_ACCESS_KEY_SECRET", ""),
			UsePathStyle: envBool("S3_USE_PATH_STYLE", true),
		},
	}

	switch cfg.Storage {
	case StorageBackendLocal:
		return cfg, nil
	case StorageBackendS3:
		if err := cfg.S3.Validate(); err != nil {
			return Config{}, err
		}
		return cfg, nil
	default:
		return Config{}, fmt.Errorf("unsupported storage backend %q", cfg.Storage)
	}
}

func (c S3Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("S3_ENDPOINT is required when STORAGE_BACKEND=s3")
	}
	if c.Region == "" {
		return fmt.Errorf("S3_REGION is required when STORAGE_BACKEND=s3")
	}
	if c.Bucket == "" {
		return fmt.Errorf("S3_BUCKET is required when STORAGE_BACKEND=s3")
	}
	if c.AccessKeyID == "" {
		return fmt.Errorf("S3_ACCESS_KEY_ID is required when STORAGE_BACKEND=s3")
	}
	if c.AccessSecret == "" {
		return fmt.Errorf("S3_ACCESS_KEY_SECRET is required when STORAGE_BACKEND=s3")
	}
	return nil
}

func env(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}
