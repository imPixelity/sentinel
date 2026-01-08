### About Sentinel

Sentinel is a hobby project that I made out of my curiosity of concurrency. :)

### Diagram

<div align="center">
  <img src="diagram.svg" alt="diagram">
</div>

### Result

```bash
HTTP failed request rate: 80.02%
Total warn level: 166464
Total error level: 277719

logging done in 2.724436083s
```

This was done with 1 ingestor, 10 parsers, 10 filterers, and 1 aggregator with 1 million log entries.

### Pipeline Stages

1. Ingest  
   Reads log file line by line
3. Parse  
   Unmarshals JSON log entries in parallel
3. Filter  
   Keeps only relevant entries (WARN, ERROR, and HTTP requests)
4. Merge  
   Combines output from all filter workers into a single channel
5. Aggregate  
   Calculates health metrics
6. Report  
   Display final statistics

### Concurrency Model

- Each stage runs in its own goroutine
- Channels connect stages, enabling concurrent processing
- WaitGroups ensure proper shutdown and resource cleanup
- Error channel handles failures without blocking the pipeline

### License

This is a hobby project for learning purposes. Feel free to use and modify as you like! :)
