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

	go func() {
		for data := range mergedPipeline {
			fmt.Println(data)
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		log.Fatalf("error %v:", err)
	}
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
