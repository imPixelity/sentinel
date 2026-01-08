package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
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
	var ctr int32

	wg.Add(4)
	ingestor := ingestLog(errCh, wg)
	_ = parseLog(errCh, ingestor, wg, &ctr)
	_ = parseLog(errCh, ingestor, wg, &ctr)
	_ = parseLog(errCh, ingestor, wg, &ctr)

	go func() {
		wg.Wait()
		fmt.Printf("success reading %d/%d log\n", ctr, 1000)
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

func parseLog(errCh chan<- error, in <-chan string, wg *sync.WaitGroup, ctr *int32) <-chan LogEntry {
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
			atomic.AddInt32(ctr, 1)
			fmt.Println(logEntry)
		}
	}()

	return out
}
