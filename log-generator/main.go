package main

import (
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
)

type Logger struct {
	status  string
	message string
	details []any
}

func main() {
	file, err := os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	logger := slog.New(slog.NewTextHandler(file, &slog.HandlerOptions{Level: slog.LevelDebug}))

	logData := []Logger{
		{"debug", "cache miss", []any{"key", "product:456"}},
		{"debug", "jwt token validated", []any{"user_id", "u_123"}},
		{"info", "user login", []any{"user_id", "u_123", "email", "john@example.com"}},
		{"info", "http request", []any{"method", "GET", "path", "/api/users", "status", 200}},
		{"info", "cache hit", []any{"key", "user:123"}},
		{"info", "user logout", []any{"user_id", "u_456"}},
		{"warn", "slow query", []any{"duration_ms", 1500}},
		{"warn", "high memory usage", []any{"current_mb", 850}},
		{"warn", "rate limit approaching", []any{"requests", 95}},
		{"error", "database query failed", []any{"error", "connection timeout"}},
		{"error", "redis connection lost", []any{"error", "connection refused"}},
		{"error", "payment processing failed", []any{"error", "card declined"}},
		{"error", "api timeout", []any{"service", "payment-gateway"}},
		{"error", "webhook delivery failed", []any{"error", "502 Bad Gateway"}},
	}

	for range 1000 {
		num := rand.IntN(len(logData))
		switch logData[num].status {
		case "debug":
			logger.Debug(logData[num].message, logData[num].details...)
		case "info":
			logger.Info(logData[num].message, logData[num].details...)
		case "warn":
			logger.Warn(logData[num].message, logData[num].details...)
		case "error":
			logger.Error(logData[num].message, logData[num].details...)
		}
	}
}
