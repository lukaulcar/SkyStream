package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                   int
	ADSBEndpoint           string
	EnableWebSocket        bool
	WsRefreshInterval      time.Duration
	EnableClickHouse       bool
	ClickHouseAddr         string
	ClickHouseDB           string
	ClickHouseUser         string
	ClickHousePass         string
	ClickHouseSaveInterval time.Duration
}

func LoadConfig(envFiles ...string) *Config {
	envFile := ".env"
	if len(envFiles) > 0 && envFiles[0] != "" {
		envFile = envFiles[0]
	}

	loadEnvFile(envFile)

	port := 8080
	if p := getEnv("PORT", "HTTP_PORT"); p != "" {
		if val, err := strconv.Atoi(p); err == nil && val > 0 {
			port = val
		}
	}

	endpoint := getEnv("ADSB_ENDPOINT")
	if endpoint == "" {
		endpoint = "https://globe.adsbexchange.com/re-api/?binCraft&zstd&box=-90,90,-180,180"
	}

	enableWS := parseBool(getEnv("ENABLE_WEBSOCKET"), true)
	wsInterval := parseDuration(getEnv("WEBSOCKET_REFRESH_INTERVAL", "WS_REFRESH_INTERVAL"), 1*time.Second)

	enableCH := parseBool(getEnv("ENABLE_CLICKHOUSE"), true)
	chAddr := getEnv("CLICKHOUSE_ADDR")
	if chAddr == "" {
		chAddr = "localhost:9000"
	}

	chDB := getEnv("CLICKHOUSE_DB")
	if chDB == "" {
		chDB = "skystream"
	}

	chUser := getEnv("CLICKHOUSE_USER")
	if chUser == "" {
		chUser = "default"
	}

	chPass := getEnv("CLICKHOUSE_PASSWORD")

	chSaveInterval := parseDuration(getEnv("CLICKHOUSE_SAVE_INTERVAL", "DB_SAVE_INTERVAL"), 1*time.Minute)

	return &Config{
		Port:                   port,
		ADSBEndpoint:           endpoint,
		EnableWebSocket:        enableWS,
		WsRefreshInterval:      wsInterval,
		EnableClickHouse:       enableCH,
		ClickHouseAddr:         chAddr,
		ClickHouseDB:           chDB,
		ClickHouseUser:         chUser,
		ClickHousePass:         chPass,
		ClickHouseSaveInterval: chSaveInterval,
	}
}

func loadEnvFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		if (strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"")) ||
			(strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) {
			if len(val) >= 2 {
				val = val[1 : len(val)-1]
			}
		}

		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
}

func getEnv(keys ...string) string {
	for _, key := range keys {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return ""
}

func parseBool(val string, defaultVal bool) bool {
	if val == "" {
		return defaultVal
	}
	switch strings.ToLower(val) {
	case "1", "t", "true", "yes", "y", "on", "enable", "enabled":
		return true
	case "0", "f", "false", "no", "n", "off", "disable", "disabled":
		return false
	default:
		return defaultVal
	}
}

func parseDuration(val string, defaultVal time.Duration) time.Duration {
	if val == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(val)
	if err == nil && d > 0 {
		return d
	}
	if secs, err := strconv.ParseFloat(val, 64); err == nil && secs > 0 {
		return time.Duration(secs * float64(time.Second))
	}
	return defaultVal
}
