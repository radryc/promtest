package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var defaultQueries = []string{
	`sum(rate(node_cpu_seconds_total[5m])) by (instance)`,
	`avg_over_time(process_resident_memory_bytes{job="prometheus"}[1h])`,
	`count(count by (job) (node_exporter_build_info))`,
	`sum(increase(node_network_receive_bytes_total[1m])) by (device)`,
	`topk(10, sum by (namespace) (kube_pod_container_resource_requests_cpu_cores))`,
	`histogram_quantile(0.99, sum(rate(prometheus_http_request_duration_seconds_bucket[5m])) by (le, handler))`,
}

type StressStats struct {
	TotalQueriesSent  uint64
	SuccessfulQueries uint64
	FailedQueries     uint64
	TotalDuration     time.Duration
}

func main() {
	// Command-line flags
	prometheusURL := flag.String("url", "http://localhost:9090", "Prometheus server URL")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	totalRequests := flag.Int("requests", 1000, "Total number of requests to send")
	queryInterval := flag.Duration("interval", 1*time.Second, "Interval between queries per worker")
	queriesStr := flag.String("queries", "", "Comma-separated list of PromQL queries to use (overrides default)")
	rangeStart := flag.Duration("range-start", 5*time.Minute, "How far back from now to start range queries")
	rangeStep := flag.Duration("range-step", 30*time.Second, "Step duration for range queries")
	rangeMode := flag.Bool("range", false, "Use range queries instead of instant queries")

	flag.Parse()

	// Determine which queries to use
	queries := defaultQueries
	if *queriesStr != "" {
		queries = parseQueries(*queriesStr)
		if len(queries) == 0 {
			log.Fatal("No queries provided or failed to parse custom queries.")
		}
	}
	log.Printf("Using %d queries for the test.", len(queries))
	for i, q := range queries {
		log.Printf("Query %d: %s", i+1, q)
	}

	if *concurrency <= 0 {
		log.Fatal("Concurrency must be greater than 0")
	}
	if *totalRequests <= 0 {
		log.Fatal("Total requests must be greater than 0")
	}

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutdown signal received, stopping workers...")
		cancel()
	}()

	var wg sync.WaitGroup
	stats := &StressStats{}
	requestQueue := make(chan struct{}, *totalRequests) // Buffered channel to limit total requests

	// Populate the request queue
	for i := 0; i < *totalRequests; i++ {
		requestQueue <- struct{}{}
	}
	close(requestQueue) // Close the queue as no more items will be added

	log.Printf("Starting Prometheus stress test with %d workers, %d total requests.", *concurrency, *totalRequests)
	log.Printf("Target Prometheus: %s", *prometheusURL)
	log.Printf("Interval per worker: %s", *queryInterval)

	startTime := time.Now()

	// Launch workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(
			ctx, i+1, *prometheusURL, queries, requestQueue, &wg, stats, *queryInterval,
			*rangeMode, *rangeStart, *rangeStep,
		)
	}

	// Wait for all workers to finish or context to be cancelled
	wg.Wait()

	stats.TotalDuration = time.Since(startTime)

	log.Println("Stress test finished.")
	log.Printf("--- Stats ---")
	log.Printf("Total Queries Sent: %d", atomic.LoadUint64(&stats.TotalQueriesSent))
	log.Printf("Successful Queries: %d", atomic.LoadUint64(&stats.SuccessfulQueries))
	log.Printf("Failed Queries: %d", atomic.LoadUint64(&stats.FailedQueries))
	log.Printf("Total Duration: %s", stats.TotalDuration)

	if atomic.LoadUint64(&stats.TotalQueriesSent) > 0 {
		avgLatency := stats.TotalDuration.Seconds() / float64(atomic.LoadUint64(&stats.TotalQueriesSent))
		log.Printf("Approx. Avg QPS: %.2f", float64(atomic.LoadUint64(&stats.TotalQueriesSent))/stats.TotalDuration.Seconds())
		log.Printf("Avg. Latency (overall, not per query): %.4f s", avgLatency) // Note: This is a very rough estimate
	}
}

// worker function executes queries against Prometheus
func worker(
	ctx context.Context,
	id int,
	promURL string,
	queries []string,
	requestQueue <-chan struct{},
	wg *sync.WaitGroup,
	stats *StressStats,
	interval time.Duration,
	rangeMode bool,
	rangeStart time.Duration,
	rangeStep time.Duration,
) {
	defer wg.Done()
	client := &http.Client{
		Timeout: 30 * time.Second, // Timeout for each HTTP request
	}
	randSrc := rand.NewSource(time.Now().UnixNano() + int64(id)) // Create a new source for each goroutine
	rng := rand.New(randSrc)                                     // Create a new random number generator from the source

	log.Printf("Worker %d started", id)

	for {
		select {
		case _, ok := <-requestQueue:
			if !ok { // Channel closed, no more requests to process
				log.Printf("Worker %d finished: request queue empty.", id)
				return
			}

			// Pick a random query from the list
			query := queries[rng.Intn(len(queries))]

			var err error
			if rangeMode {
				end := time.Now()
				start := end.Add(-rangeStart)
				err = executeRangeQuery(client, promURL, query, start, end, rangeStep)
			} else {
				err = executeQuery(client, promURL, query)
			}
			atomic.AddUint64(&stats.TotalQueriesSent, 1)
			if err != nil {
				atomic.AddUint64(&stats.FailedQueries, 1)
				log.Printf("Worker %d: Query failed: %s (Query: %s)", id, err, query)
			} else {
				atomic.AddUint64(&stats.SuccessfulQueries, 1)
			}

			// Wait for the specified interval or until context is cancelled
			select {
			case <-time.After(interval):
				// Continue to next query
			case <-ctx.Done():
				log.Printf("Worker %d stopping due to context cancellation.", id)
				return
			}

		case <-ctx.Done():
			log.Printf("Worker %d stopping due to context cancellation (outer select).", id)
			return
		}
	}
}

// executeQuery sends a single query to Prometheus
func executeQuery(client *http.Client, promURL string, query string) error {
	// Construct the query URL
	fullURL, err := url.Parse(promURL + "/api/v1/query")
	if err != nil {
		return fmt.Errorf("error parsing base URL: %w", err)
	}

	params := url.Values{}
	params.Add("query", query)
	fullURL.RawQuery = params.Encode()

	// Create the request
	req, err := http.NewRequest("GET", fullURL.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bad status code: %d. Response: %s", resp.StatusCode, string(bodyBytes))
	}

	// Optionally, you could read and parse the response body here
	// For a stress test, just ensuring a 200 OK might be sufficient
	_, err = io.Copy(io.Discard, resp.Body) // Discard the body to free up resources
	if err != nil {
		return fmt.Errorf("error discarding response body: %w", err)
	}

	return nil
}

// executeRangeQuery sends a range query to Prometheus
func executeRangeQuery(client *http.Client, promURL string, query string, start, end time.Time, step time.Duration) error {
	fullURL, err := url.Parse(promURL + "/api/v1/query_range")
	if err != nil {
		return fmt.Errorf("error parsing base URL: %w", err)
	}

	params := url.Values{}
	params.Add("query", query)
	params.Add("start", fmt.Sprintf("%d", start.Unix()))
	params.Add("end", fmt.Sprintf("%d", end.Unix()))
	params.Add("step", step.String())
	fullURL.RawQuery = params.Encode()

	req, err := http.NewRequest("GET", fullURL.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bad status code: %d. Response: %s", resp.StatusCode, string(bodyBytes))
	}

	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return fmt.Errorf("error discarding response body: %w", err)
	}

	return nil
}

// parseQueries splits a comma-separated string of queries into a slice.
func parseQueries(queryStr string) []string {
	if queryStr == "" {
		return []string{}
	}
	// This is a simple split. For more complex query strings, more robust parsing might be needed.
	// For example, handling queries that themselves contain commas (though unlikely in PromQL for the main query part).
	var queries []string
	parts := splitRespectingQuotes(queryStr, ',')
	for _, p := range parts {
		trimmed := trimQuotes(p)
		if trimmed != "" {
			queries = append(queries, trimmed)
		}
	}
	return queries
}

// Helper to split string by delimiter, respecting parts within quotes.
// This is a basic implementation.
func splitRespectingQuotes(s string, delimiter rune) []string {
	var result []string
	var currentToken []rune
	inQuotes := false
	for _, char := range s {
		switch char {
		case '"':
			inQuotes = !inQuotes
			currentToken = append(currentToken, char) // Keep quotes for now, will be trimmed later
		case delimiter:
			if inQuotes {
				currentToken = append(currentToken, char)
			} else {
				result = append(result, string(currentToken))
				currentToken = []rune{}
			}
		default:
			currentToken = append(currentToken, char)
		}
	}
	result = append(result, string(currentToken)) // Add the last token
	return result
}

// Helper to trim leading/trailing spaces and quotes from a string.
func trimQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
