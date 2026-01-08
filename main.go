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
	start := time.Now()
	errCh := make(chan error, 1)
	wg := &sync.WaitGroup{}

	go func() {
		for err := range errCh {
			log.Fatalf("error %v:", err)
		}
	}()

	wg.Add(1)
	ingestor := ingestLog(errCh, wg)

	wg.Add(10)
	parser1 := parseLog(errCh, wg, ingestor)
	parser2 := parseLog(errCh, wg, ingestor)
	parser3 := parseLog(errCh, wg, ingestor)
	parser4 := parseLog(errCh, wg, ingestor)
	parser5 := parseLog(errCh, wg, ingestor)
	parser6 := parseLog(errCh, wg, ingestor)
	parser7 := parseLog(errCh, wg, ingestor)
	parser8 := parseLog(errCh, wg, ingestor)
	parser9 := parseLog(errCh, wg, ingestor)
	parser10 := parseLog(errCh, wg, ingestor)

	wg.Add(10)
	filterer1 := filterLog(wg, parser1)
	filterer2 := filterLog(wg, parser2)
	filterer3 := filterLog(wg, parser3)
	filterer4 := filterLog(wg, parser4)
	filterer5 := filterLog(wg, parser5)
	filterer6 := filterLog(wg, parser6)
	filterer7 := filterLog(wg, parser7)
	filterer8 := filterLog(wg, parser8)
	filterer9 := filterLog(wg, parser9)
	filterer10 := filterLog(wg, parser10)

	filterers := []<-chan LogEntry{filterer1, filterer2, filterer3, filterer4, filterer5, filterer6, filterer7, filterer8, filterer9, filterer10}

	wg.Add(1)
	mergedPipeline := mergePipeline(wg, filterers...)

	wg.Add(1)
	aggregator := aggregateLog(errCh, wg, mergedPipeline)

	reportLog(aggregator)
	fmt.Printf("\nlogging done in %v\n", time.Since(start))

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

func reportLog(in <-chan HealthSummary) {
	for summary := range in {
		rate := float64(summary.FailedRequests) / float64(summary.TotalRequests) * 100
		fmt.Printf("HTTP failed request rate: %.2f%%\n", rate)
		fmt.Printf("Total warn level: %d\n", summary.Warns)
		fmt.Printf("Total error level: %d\n", summary.Errors)
	}
}
