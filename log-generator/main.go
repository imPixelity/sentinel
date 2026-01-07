package main

import (
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
)

const logCount = 1000

type LogEntry struct {
	Level   string
	Message string
	Attrs   map[string]any
}

func attrsToSlice(m map[string]any) []any {
	s := make([]any, 0, len(m)*2)
	for k, v := range m {
		s = append(s, k, v)
	}
	return s
}

func main() {
	file, err := os.OpenFile("log.txt", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	logger := slog.New(slog.NewJSONHandler(file, &slog.HandlerOptions{Level: slog.LevelDebug}))

	logEntry := []LogEntry{
		{"debug", "cache miss", map[string]any{
			"key": "product:456",
		}},
		{"debug", "jwt token validated", map[string]any{
			"user_id": "u_123",
		}},
		{"info", "user login", map[string]any{
			"user_id": "u_123",
			"email":   "john@example.com",
		}},
		{"info", "http request", map[string]any{
			"method": "GET",
			"path":   "/api/users",
			"status": 200,
		}},
		{"info", "cache hit", map[string]any{
			"key": "user:123",
		}},
		{"info", "user logout", map[string]any{
			"user_id": "u_456",
		}},
		{"warn", "slow query", map[string]any{
			"duration_ms": 1500,
		}},
		{"warn", "high memory usage", map[string]any{
			"current_mb": 850,
		}},
		{"warn", "rate limit approaching", map[string]any{
			"requests": 95,
		}},
		{"error", "database query failed", map[string]any{
			"error": "connection timeout",
		}},
		{"error", "redis connection lost", map[string]any{
			"error": "connection refused",
		}},
		{"error", "payment processing failed", map[string]any{
			"error": "card declined",
		}},
		{"error", "api timeout", map[string]any{
			"service": "payment-gateway",
		}},
		{"error", "webhook delivery failed", map[string]any{
			"error": "502 Bad Gateway",
		}},
	}

	for range logCount {
		num := rand.IntN(len(logEntry))
		entry := logEntry[num]
		attrs := attrsToSlice(entry.Attrs)

		switch entry.Level {
		case "debug":
			logger.Debug(entry.Message, attrs...)
		case "info":
			logger.Info(entry.Message, attrs...)
		case "warn":
			logger.Warn(entry.Message, attrs...)
		case "error":
			logger.Error(entry.Message, attrs...)
		}
	}
}
