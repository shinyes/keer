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
	BodyLimitMB       int
	KeerAPIVersion    string
	Storage           StorageBackend
	S3                S3Config
	AllowRegistration bool
	BootstrapUser     string
	BootstrapToken    string
}

func Load() (Config, error) {
	cfg := Config{
		Addr:              env("APP_ADDR", ":12843"),
		BaseURL:           strings.TrimRight(env("BASE_URL", "http://localhost:12843"), "/"),
		DBPath:            env("DB_PATH", "./data/keer.db"),
		UploadsDir:        env("UPLOADS_DIR", "./data/uploads"),
		BodyLimitMB:       envInt("HTTP_BODY_LIMIT_MB", 64),
		KeerAPIVersion:    env("KEER_API_VERSION", "0.1"),
		Storage:           StorageBackendLocal,
		AllowRegistration: envBool("ALLOW_REGISTRATION", true),
		BootstrapUser:     env("BOOTSTRAP_USER", "demo"),
		BootstrapToken:    env("BOOTSTRAP_TOKEN", ""),
	}
	return cfg, nil
}

func (c S3Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("s3 endpoint is required when storage backend is s3")
	}
	if c.Region == "" {
		return fmt.Errorf("s3 region is required when storage backend is s3")
	}
	if c.Bucket == "" {
		return fmt.Errorf("s3 bucket is required when storage backend is s3")
	}
	if c.AccessKeyID == "" {
		return fmt.Errorf("s3 access key id is required when storage backend is s3")
	}
	if c.AccessSecret == "" {
		return fmt.Errorf("s3 access key secret is required when storage backend is s3")
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

func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
