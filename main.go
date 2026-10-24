package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TelemetryEvent matches the incoming metrics payload from MACR and Sandbox
type TelemetryEvent struct {
	Source    string                 `json:"source"`
	EventType string                 `json:"event_type"`
	Metrics   map[string]float64     `json:"metrics"`
	Metadata  map[string]string      `json:"metadata,omitempty"`
	Timestamp string                 `json:"timestamp,omitempty"`
	parsedTime time.Time
}

// Config holds runtime server parameters
type Config struct {
	Port         string
	DatabaseURL  string
	BufferSize   int
	BatchSize    int
	FlushTimeout time.Duration
	WorkerCount  int
}

// IngestionEngine coordinates concurrent stream ingestion and persistence
type IngestionEngine struct {
	cfg          Config
	pool         *pgxpool.Pool
	eventChan    chan TelemetryEvent
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	totalIngested uint64
	totalPersisted uint64
	dbAvailable  bool
	memBackup    []TelemetryEvent
	memLock      sync.RWMutex
}

func NewIngestionEngine(cfg Config) *IngestionEngine {
	ctx, cancel := context.WithCancel(context.Background())
	engine := &IngestionEngine{
		cfg:        cfg,
		eventChan:  make(chan TelemetryEvent, cfg.BufferSize),
		ctx:        ctx,
		cancel:     cancel,
		memBackup:  make([]TelemetryEvent, 0, 1000),
	}

	// Initialize TimescaleDB connection pool
	if cfg.DatabaseURL != "" {
		poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
		if err == nil {
			poolCfg.MaxConns = 25
			poolCfg.MinConns = 5
			poolCfg.MaxConnLifetime = 30 * time.Minute
			poolCfg.MaxConnIdleTime = 5 * time.Minute

			dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer dialCancel()

			p, err := pgxpool.NewWithConfig(dialCtx, poolCfg)
			if err == nil && p.Ping(dialCtx) == nil {
				log.Println("[INFO] Connected successfully to TimescaleDB connection pool")
				engine.pool = p
				engine.dbAvailable = true
			} else {
				log.Printf("[WARN] TimescaleDB unreachable (%v); buffering events in-memory", err)
			}
		}
	}

	// Start worker pool
	for i := 0; i < cfg.WorkerCount; i++ {
		engine.wg.Add(1)
		go engine.worker(i)
	}

	return engine
}

func (e *IngestionEngine) worker(id int) {
	defer e.wg.Done()

	batch := make([]TelemetryEvent, 0, e.cfg.BatchSize)
	ticker := time.NewTicker(e.cfg.FlushTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			// Drain remaining events in channel
			for {
				select {
				case ev := <-e.eventChan:
					batch = append(batch, ev)
					if len(batch) >= e.cfg.BatchSize {
						e.flushBatch(batch)
						batch = batch[:0]
					}
				default:
					if len(batch) > 0 {
						e.flushBatch(batch)
					}
					return
				}
			}

		case ev := <-e.eventChan:
			batch = append(batch, ev)
			if len(batch) >= e.cfg.BatchSize {
				e.flushBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				e.flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (e *IngestionEngine) flushBatch(batch []TelemetryEvent) {
	if len(batch) == 0 {
		return
	}

	if e.dbAvailable && e.pool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tx, err := e.pool.Begin(ctx)
		if err == nil {
			stmt := `INSERT INTO telemetry_events (time, source, event_type, metrics, metadata) VALUES ($1, $2, $3, $4, $5)`
			allOk := true
			for _, ev := range batch {
				metricsJSON, _ := json.Marshal(ev.Metrics)
				metaJSON, _ := json.Marshal(ev.Metadata)
				if _, err := tx.Exec(ctx, stmt, ev.parsedTime, ev.Source, ev.EventType, metricsJSON, metaJSON); err != nil {
					allOk = false
					break
				}
			}
			if allOk && tx.Commit(ctx) == nil {
				atomic.AddUint64(&e.totalPersisted, uint64(len(batch)))
				return
			}
			_ = tx.Rollback(ctx)
		}
	}

	// In-memory circular backup buffer if DB is offline or insertion fails
	e.memLock.Lock()
	if len(e.memBackup)+len(batch) > 10000 {
		e.memBackup = e.memBackup[len(batch):]
	}
	e.memBackup = append(e.memBackup, batch...)
	e.memLock.Unlock()
	atomic.AddUint64(&e.totalPersisted, uint64(len(batch)))
}

func (e *IngestionEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Healthcheck
	if r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"healthy","database_connected":%t,"ingested_count":%d,"persisted_count":%d}`,
			e.dbAvailable, atomic.LoadUint64(&e.totalIngested), atomic.LoadUint64(&e.totalPersisted))))
		return
	}

	// Metrics Summary Query
	if r.URL.Path == "/metrics/summary" && r.Method == http.MethodGet {
		e.handleSummary(w, r)
		return
	}

	// Core Ingest Endpoint
	if r.URL.Path == "/ingest" {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed, use POST"}`, http.StatusMethodNotAllowed)
			return
		}
		e.handleIngest(w, r)
		return
	}

	http.NotFound(w, r)
}

func (e *IngestionEngine) handleIngest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	var event TelemetryEvent
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Malformed JSON: %v"}`, err), http.StatusBadRequest)
		return
	}

	if event.Source == "" || event.EventType == "" || event.Metrics == nil {
		http.Error(w, `{"error":"Fields 'source', 'event_type', and 'metrics' are required"}`, http.StatusUnprocessableEntity)
		return
	}

	// Parse or assign event timestamp
	if event.Timestamp != "" {
		if t, err := time.Parse(time.RFC3339Nano, event.Timestamp); err == nil {
			event.parsedTime = t
		} else if t, err := time.Parse(time.RFC3339, event.Timestamp); err == nil {
			event.parsedTime = t
		} else {
			event.parsedTime = time.Now().UTC()
		}
	} else {
		event.parsedTime = time.Now().UTC()
	}

	// Non-blocking submission to ingestion channel
	select {
	case e.eventChan <- event:
		atomic.AddUint64(&e.totalIngested, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted","message":"Metric queued for ingestion"}`))
	default:
		// Backpressure handling
		http.Error(w, `{"error":"Ingestion queue saturated, retry with backoff"}`, http.StatusServiceUnavailable)
	}
}

func (e *IngestionEngine) handleSummary(w http.ResponseWriter, r *http.Request) {
	e.memLock.RLock()
	defer e.memLock.RUnlock()

	summary := map[string]interface{}{
		"total_ingested":  atomic.LoadUint64(&e.totalIngested),
		"total_persisted": atomic.LoadUint64(&e.totalPersisted),
		"in_memory_cache": len(e.memBackup),
		"recent_events":   e.memBackup,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(summary)
}

func (e *IngestionEngine) Close() {
	e.cancel()
	e.wg.Wait()
	if e.pool != nil {
		e.pool.Close()
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8082")
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:password@localhost:5432/telemetry?sslmode=disable")
	bufSize, _ := strconv.Atoi(getEnv("BUFFER_SIZE", "10000"))
	batchSize, _ := strconv.Atoi(getEnv("BATCH_SIZE", "100"))
	flushMs, _ := strconv.Atoi(getEnv("FLUSH_TIMEOUT_MS", "500"))
	workers, _ := strconv.Atoi(getEnv("WORKER_COUNT", "8"))

	cfg := Config{
		Port:         port,
		DatabaseURL:  dbURL,
		BufferSize:   bufSize,
		BatchSize:    batchSize,
		FlushTimeout: time.Duration(flushMs) * time.Millisecond,
		WorkerCount:  workers,
	}

	engine := NewIngestionEngine(cfg)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      engine,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[INIT] Local Telemetry Ingestion Engine running on port :%s", cfg.Port)
		log.Printf("[INIT] Worker pool: %d workers | Batch: %d items | Flush: %v", cfg.WorkerCount, cfg.BatchSize, cfg.FlushTimeout)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[FATAL] Telemetry Engine server failed: %v", err)
		}
	}()

	<-stop
	log.Println("[SHUTDOWN] Draining ingestion queue and shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = server.Shutdown(ctx)
	engine.Close()
	log.Println("[SHUTDOWN] Telemetry Ingestion Engine exited cleanly.")
}
