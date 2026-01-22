package config

import "os"

type Config struct {
	HTTPAddr  string
	RedisAddr string
	DataDir   string
}

func Load() Config {
	cfg := Config{
		HTTPAddr:  getEnv("HTTP_ADDR", ":8090"),
		RedisAddr: getEnv("REDIS_ADDR", "localhost:6379"),
		DataDir:   getEnv("DATA_DIR", "data"),
	}
	return cfg
}

func getEnv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}
