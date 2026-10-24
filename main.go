package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
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
	_ "modernc.org/sqlite"
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
	LowMem       bool
	SQLitePath   string
}

type IngestionEngine struct {
	cfg            Config
	pool           *pgxpool.Pool
	sqliteDB       *sql.DB
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

	if cfg.LowMem {
		log.Printf("[INIT] Low-Memory Mode Enabled: Initializing embedded SQLite at %s", cfg.SQLitePath)
		db, err := sql.Open("sqlite", cfg.SQLitePath)
		if err == nil {
			if _, pragmaErr := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;`); pragmaErr != nil {
				log.Printf("[WARN] Failed to set SQLite WAL PRAGMAs: %v", pragmaErr)
			}
			schema := `
			CREATE TABLE IF NOT EXISTS telemetry_events (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
				source TEXT,
				event_type TEXT,
				metrics TEXT,
				metadata TEXT
			);
			CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry_events(timestamp);
			CREATE INDEX IF NOT EXISTS idx_telemetry_source ON telemetry_events(source);
			`
			if _, schemaErr := db.Exec(schema); schemaErr == nil {
				log.Println("[INFO] Successfully initialized SQLite WAL telemetry database")
				engine.sqliteDB = db
				engine.dbAvailable = true
			} else {
				log.Printf("[ERROR] Failed to create SQLite schema: %v", schemaErr)
			}
		} else {
			log.Printf("[ERROR] Failed to open SQLite database: %v", err)
		}
	} else if cfg.DatabaseURL != "" {
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

	// Start anomaly detection background monitor
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

	// 1. Low-Memory SQLite Ingestion Route
	if e.cfg.LowMem && e.sqliteDB != nil {
		tx, err := e.sqliteDB.Begin()
		if err == nil {
			stmt, prepErr := tx.Prepare(`INSERT INTO telemetry_events (timestamp, source, event_type, metrics, metadata) VALUES (?, ?, ?, ?, ?)`)
			if prepErr == nil {
				allOk := true
				for _, ev := range batch {
					metricsJSON, _ := json.Marshal(ev.Metrics)
					metaJSON, _ := json.Marshal(ev.Metadata)
					tsStr := ev.parsedTime.Format("2006-01-02 15:04:05.000")
					if _, execErr := stmt.Exec(tsStr, ev.Source, ev.EventType, string(metricsJSON), string(metaJSON)); execErr != nil {
						allOk = false
						break
					}
				}
				_ = stmt.Close()
				if allOk && tx.Commit() == nil {
					atomic.AddUint64(&e.totalPersisted, uint64(len(batch)))
					return
				}
			}
			_ = tx.Rollback()
		}
	}

	// 2. High-Throughput TimescaleDB Route
	if !e.cfg.LowMem && e.dbAvailable && e.pool != nil {
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

	// 3. Fallback: In-Memory Ring Buffer
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
}

func (e *IngestionEngine) anomalyMonitor() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.AnomalyCheck)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			// Inspect recent error rates
			e.memLock.RLock()
			errCount := 0
			sampleWindow := len(e.memBackup)
			for _, ev := range e.memBackup {
				if strings.Contains(strings.ToLower(ev.EventType), "error") ||
					strings.Contains(strings.ToLower(ev.EventType), "timeout") {
					errCount++
				}
			}
			e.memLock.RUnlock()

			if sampleWindow > 50 && float64(errCount)/float64(sampleWindow) > 0.3 {
				log.Printf("[ALERT] High anomaly error rate detected in recent window: %d errors / %d events", errCount, sampleWindow)
			}
		}
	}
}

func (e *IngestionEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/healthz" && r.Method == http.MethodGet:
		e.handleHealth(w, r)
	case r.URL.Path == "/health" && r.Method == http.MethodGet:
		e.handleHealth(w, r)
	case r.URL.Path == "/ingest" && r.Method == http.MethodPost:
		e.handleIngest(w, r)
	case r.URL.Path == "/metrics/summary" && r.Method == http.MethodGet:
		e.handleSummary(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (e *IngestionEngine) handleHealth(w http.ResponseWriter, r *http.Request) {
	storageType := "timescaledb"
	if e.cfg.LowMem {
		storageType = "sqlite-embedded-wal"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"service":         "cipher-log",
		"status":          "healthy",
		"low_mem":         e.cfg.LowMem,
		"storage":         storageType,
		"db_available":    e.dbAvailable,
		"queue_depth":     len(e.eventChan),
		"total_ingested":  atomic.LoadUint64(&e.totalIngested),
		"total_persisted": atomic.LoadUint64(&e.totalPersisted),
	})
}

func (e *IngestionEngine) handleSummary(w http.ResponseWriter, r *http.Request) {
	e.memLock.RLock()
	backupCount := len(e.memBackup)
	e.memLock.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_ingested":  atomic.LoadUint64(&e.totalIngested),
		"total_persisted": atomic.LoadUint64(&e.totalPersisted),
		"buffered_backup": backupCount,
		"low_mem_mode":    e.cfg.LowMem,
		"storage_backend": func() string {
			if e.cfg.LowMem {
				return "sqlite3-wal"
			}
			return "timescaledb"
		}(),
	})
}

func (e *IngestionEngine) handleIngest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		http.Error(w, `{"error":"Request body too large"}`, http.StatusRequestEntityTooLarge)
		return
	}
	_ = r.Body.Close()

	var event TelemetryEvent
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Malformed JSON: %v"}`, err), http.StatusBadRequest)
		return
	}

	if event.Source == "" || event.EventType == "" || event.Metrics == nil {
		http.Error(w, `{"error":"Missing required fields: source, event_type, metrics"}`, http.StatusUnprocessableEntity)
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
	if e.sqliteDB != nil {
		_ = e.sqliteDB.Close()
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	lowMemFlag := flag.Bool("low-mem", false, "Enable low-memory embedded SQLite WAL ingestion mode")
	sqlitePathFlag := flag.String("sqlite-path", "./telemetry.db", "File path for local SQLite database in low-mem mode")
	portFlag := flag.String("port", "", "HTTP port to bind (overrides PORT env)")
	flag.Parse()

	port := getEnv("PORT", "8082")
	if *portFlag != "" {
		port = *portFlag
	}

	isLowMem := *lowMemFlag || strings.ToLower(os.Getenv("LOW_MEM")) == "true"
	sqlitePath := getEnv("SQLITE_PATH", *sqlitePathFlag)
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
		LowMem:       isLowMem,
		SQLitePath:   sqlitePath,
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
		modeStr := "TimescaleDB Cluster"
		if cfg.LowMem {
			modeStr = fmt.Sprintf("Embedded SQLite WAL (%s)", cfg.SQLitePath)
		}
		log.Printf("[INIT] Cipher-Log Telemetry Engine running on :%s [Storage: %s]", cfg.Port, modeStr)
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
