package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type LogEntry struct {
	Timestamp time.Time      `json:"time"`
	Level     string         `json:"level"`
	Message   string         `json:"msg"`
	Attrs     map[string]any `json:"details"`
}

type HealthSummary struct {
	TotalRequests  int
	FailedRequests int
	Warns          int
	Errors         int
}

func main() {
	errCh := make(chan error, 1)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	ingestor := ingestLog(errCh, wg)

	wg.Add(3)
	parserX := parseLog(errCh, wg, ingestor)
	parserY := parseLog(errCh, wg, ingestor)
	parserZ := parseLog(errCh, wg, ingestor)

	wg.Add(3)
	filtererX := filterLog(wg, parserX)
	filtererY := filterLog(wg, parserY)
	filtererZ := filterLog(wg, parserZ)

	wg.Add(1)
	mergedPipeline := mergePipeline(wg, filtererX, filtererY, filtererZ)

	wg.Add(1)
	aggregator := aggregateLog(errCh, wg, mergedPipeline)

	wg.Go(func() {
		for data := range aggregator {
			fmt.Printf("HTTP failed request rate: %d/%d\n", data.FailedRequests, data.TotalRequests)
			fmt.Printf("Warns: %d\n", data.Warns)
			fmt.Printf("Errors: %d\n", data.Errors)
		}
	})

	go func() {
		for err := range errCh {
			log.Fatalf("error %v:", err)
		}
	}()

	wg.Wait()
	close(errCh)
}

func ingestLog(errCh chan<- error, wg *sync.WaitGroup) <-chan string {
	out := make(chan string)

	go func() {
		defer close(out)
		defer wg.Done()

		file, err := os.Open("log.txt")
		if err != nil {
			errCh <- fmt.Errorf("failed to open file %s: %w", file.Name(), err)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			out <- scanner.Text()
		}

		if err := scanner.Err(); err != nil {
			errCh <- fmt.Errorf("failed to scan: %w", err)
		}
	}()

	return out
}

func parseLog(errCh chan<- error, wg *sync.WaitGroup, in <-chan string) <-chan LogEntry {
	out := make(chan LogEntry)

	go func() {
		defer close(out)
		defer wg.Done()

		for log := range in {
			logEntry := LogEntry{}

			if err := json.Unmarshal([]byte(log), &logEntry); err != nil {
				errCh <- fmt.Errorf("failed to unmarshal %s: %w", log, err)
				continue
			}

			out <- logEntry
		}
	}()

	return out
}

func keepLog(entry LogEntry) bool {
	if entry.Level == "WARN" || entry.Level == "ERROR" {
		return true
	}
	if entry.Level == "INFO" && entry.Message == "http request" {
		return true
	}
	return false
}

func filterLog(wg *sync.WaitGroup, in <-chan LogEntry) <-chan LogEntry {
	out := make(chan LogEntry)

	go func() {
		defer close(out)
		defer wg.Done()
		for logEntry := range in {
			if keepLog(logEntry) {
				out <- logEntry
			}
		}
	}()

	return out
}

func mergePipeline(wg *sync.WaitGroup, ins ...<-chan LogEntry) <-chan LogEntry {
	out := make(chan LogEntry)
	wgM := &sync.WaitGroup{}

	wgM.Add(len(ins))
	for _, ch := range ins {
		go func(ch <-chan LogEntry) {
			defer wgM.Done()
			for logEntry := range ch {
				out <- logEntry
			}
		}(ch)
	}

	go func() {
		defer close(out)
		wgM.Wait()
		wg.Done()
	}()

	return out
}

func aggregateLog(errCh chan<- error, wg *sync.WaitGroup, in <-chan LogEntry) <-chan HealthSummary {
	out := make(chan HealthSummary)
	healthSummary := HealthSummary{}

	go func() {
		defer close(out)
		defer wg.Done()
		for logEntry := range in {
			switch logEntry.Level {
			case "WARN":
				healthSummary.Warns++
			case "ERROR":
				healthSummary.Errors++
			case "INFO":
				healthSummary.TotalRequests++
				raw, ok := logEntry.Attrs["status"]
				if !ok {
					errCh <- fmt.Errorf("missing required status: %v", logEntry)
					continue
				}

				status, ok := raw.(float64)
				if !ok {
					errCh <- fmt.Errorf("failed to type assert status: %v", status)
					continue
				}

				if int(status) >= 400 {
					healthSummary.FailedRequests++
				}
			}
		}
		out <- healthSummary
	}()

	return out
}
