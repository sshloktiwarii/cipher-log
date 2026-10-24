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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TelemetryEvent struct {
	Source     string             `json:"source"`
	EventType  string             `json:"event_type"`
	Metrics    map[string]float64 `json:"metrics"`
	Metadata   map[string]string  `json:"metadata,omitempty"`
	Timestamp  string             `json:"timestamp,omitempty"`
	parsedTime time.Time
}

type Config struct {
	Port         string
	DatabaseURL  string
	BufferSize   int
	BatchSize    int
	FlushTimeout time.Duration
	WorkerCount  int
	AnomalyCheck time.Duration
}

type IngestionEngine struct {
	cfg            Config
	pool           *pgxpool.Pool
	eventChan      chan TelemetryEvent
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	totalIngested  uint64
	totalPersisted uint64
	dbAvailable    bool
	memBackup      []TelemetryEvent
	memLock        sync.RWMutex
}

func NewIngestionEngine(cfg Config) *IngestionEngine {
	ctx, cancel := context.WithCancel(context.Background())
	engine := &IngestionEngine{
		cfg:       cfg,
		eventChan: make(chan TelemetryEvent, cfg.BufferSize),
		ctx:       ctx,
		cancel:    cancel,
		memBackup: make([]TelemetryEvent, 0, 1000),
	}

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

	// Start worker pool for batch ingestion
	for i := 0; i < cfg.WorkerCount; i++ {
		engine.wg.Add(1)
		go engine.worker(i)
	}

	// Start anomaly detection background monitor (every 60s)
	engine.wg.Add(1)
	go engine.anomalyMonitor()

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

	e.memLock.Lock()
	if len(e.memBackup)+len(batch) > 10000 {
		excess := (len(e.memBackup) + len(batch)) - 10000
		if excess >= len(e.memBackup) {
			e.memBackup = e.memBackup[:0]
		} else {
			e.memBackup = e.memBackup[excess:]
		}
	}
	e.memBackup = append(e.memBackup, batch...)
	e.memLock.Unlock()
	atomic.AddUint64(&e.totalPersisted, uint64(len(batch)))
}

// anomalyMonitor checks TimescaleDB every 60s for error rate > 5% or latency spikes
func (e *IngestionEngine) anomalyMonitor() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.AnomalyCheck)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.checkAnomalies()
		}
	}
}

func (e *IngestionEngine) checkAnomalies() {
	if e.pool == nil || !e.dbAvailable {
		// Evaluate in-memory sliding window
		e.memLock.RLock()
		defer e.memLock.RUnlock()
		if len(e.memBackup) == 0 {
			return
		}

		errorsCount := 0
		for _, ev := range e.memBackup {
			if st, exists := ev.Metadata["status"]; exists && (st == "error" || st == "timeout" || strings.HasPrefix(st, "5")) {
				errorsCount++
			}
		}

		errorRate := (float64(errorsCount) / float64(len(e.memBackup))) * 100.0
		if errorRate > 5.0 {
			log.Printf("🚨 [ANOMALY ALERT] Error rate spike detected: %.2f%% (threshold: 5.0%%) across %d events",
				errorRate, len(e.memBackup))
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query last 5 minutes from TimescaleDB
	query := `
		SELECT 
			count(*) AS total,
			count(*) FILTER (WHERE metadata->>'status' IN ('error', 'timeout') OR (metrics->>'http_status')::int >= 500) AS errors,
			coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY (metrics->>'latency_ms')::numeric), 0) AS p99_latency
		FROM telemetry_events 
		WHERE time > NOW() - INTERVAL '5 minutes';
	`

	var total, errorsCount int64
	var p99Latency float64

	err := e.pool.QueryRow(ctx, query).Scan(&total, &errorsCount, &p99Latency)
	if err == nil && total > 0 {
		errorRate := (float64(errorsCount) / float64(total)) * 100.0
		if errorRate > 5.0 {
			log.Printf("🚨 [ANOMALY ALERT] High error rate: %.2f%% (%d/%d failed) in last 5m!", errorRate, errorsCount, total)
		}
		if p99Latency > 2000.0 {
			log.Printf("🚨 [ANOMALY ALERT] Latency spike! P99 latency is %.2f ms (threshold: 2000ms)", p99Latency)
		}
	}
}

func (e *IngestionEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"healthy","database_connected":%t,"ingested_count":%d,"persisted_count":%d,"anomaly_monitor":true}`,
			e.dbAvailable, atomic.LoadUint64(&e.totalIngested), atomic.LoadUint64(&e.totalPersisted))))
		return
	}

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
	// Guard against unbounded body DoS
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"Failed to read body"}`, http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	var event TelemetryEvent
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Malformed JSON: %v"}`, err), http.StatusBadRequest)
		return
	}

	if event.Source == "" || event.EventType == "" || event.Metrics == nil {
		http.Error(w, `{"error":"Missing required fields"}`, http.StatusUnprocessableEntity)
		return
	}

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

	select {
	case e.eventChan <- event:
		atomic.AddUint64(&e.totalIngested, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	default:
		http.Error(w, `{"error":"Queue saturated"}`, http.StatusServiceUnavailable)
	}
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
		AnomalyCheck: 60 * time.Second,
	}

	engine := NewIngestionEngine(cfg)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      engine,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[INIT] Local Telemetry & Anomaly Engine running on :%s", cfg.Port)
		log.Printf("[INIT] Anomaly Detection: Continuous aggregate check active every 60s")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[FATAL] Server error: %v", err)
		}
	}()

	<-stop
	log.Println("[SHUTDOWN] Exiting Telemetry Engine...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	engine.Close()
}
