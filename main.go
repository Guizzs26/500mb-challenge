package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	QueueBufferSize = 50000
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "application failed %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	setup("development", "debug")
	slog.Info("logger initialized", "env", "development", "log_level", "debug")

	deps, err := initDependencies()
	if err != nil {
		return fmt.Errorf("initializing dependencies: %v", err)
	}
	defer deps.cleanup()

	pgRepo := NewPgRepository(deps.pool)
	for i := 0; i < 5; i++ {
		go startWorker(deps.telemetryCh, pgRepo)
	}

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      setupRoutes(deps),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("server starting", "port", "8080", "env", "development")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			srvErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-srvErr:
		slog.Error("server crashed", "error", err)
		return fmt.Errorf("server error: %v", err)
	case sig := <-quit:
		slog.Info("shutdown signal received", "signal", sig.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	slog.Info("shutting down server gracefully")
	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("graceful shutdown failed: %v", err)
	}

	slog.Info("server stopped")
	return nil
}

type dependencies struct {
	pool        *pgxpool.Pool
	telemetryCh chan TelemetryMessage
	cleanup     func()
}

func initDependencies() (*dependencies, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	pool, err := newPostgresPool(ctx)
	if err != nil {
		return nil, fmt.Errorf("database connection: %v", err)
	}

	return &dependencies{
		pool:        pool,
		telemetryCh: make(chan TelemetryMessage, QueueBufferSize),
		cleanup: func() {
			pool.Close()
		},
	}, nil
}

func setupRoutes(deps *dependencies) http.Handler {
	mux := http.NewServeMux()

	pgRepo := NewPgRepository(deps.pool)
	handler := NewHandler(deps, pgRepo)

	mux.HandleFunc("POST /devices/{id}/telemetry", handler.createTelemetryPoint)
	mux.HandleFunc("POST /devices/{id}/telemetry/batch", handler.createTelemetryPointBatch)

	return InjectInstanceID(mux)
}

func newPostgresPool(ctx context.Context) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig("postgres://postgres500user:postgres500pass@postgres:5432/postgres500db?sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %v", err)
	}

	poolCfg.MaxConns = 10
	poolCfg.MinConns = 2
	poolCfg.MaxConnIdleTime = time.Second * 60

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %v", err)
	}

	return pool, nil
}

func setup(env, level string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: l}

	var handler slog.Handler
	if env == "development" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	slog.SetDefault(slog.New(handler))
}

func startWorker(ch <-chan TelemetryMessage, repo *pgRepository) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	maxBatchSize := 200
	buff := make([]TelemetryMessage, 0, maxBatchSize)

	flush := func() {
		if len(buff) == 0 {
			return
		}

		grouped := make(map[string][]TelemetryPoint)
		for _, msg := range buff {
			grouped[msg.DeviceID] = append(grouped[msg.DeviceID], msg.Point)
		}

		for deviceID, points := range grouped {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if _, err := repo.SaveBatch(ctx, deviceID, points); err != nil {
				slog.Error("failed to flush micro-bash to postgres", "error", err)
			}
			cancel()
		}

		buff = buff[:0]
	}

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				flush()
				return
			}
			buff = append(buff, msg)

			if len(buff) >= maxBatchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

func InjectInstanceID(next http.Handler) http.Handler {
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "local-dev"
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Instance-Id", instanceID)
		next.ServeHTTP(w, r)
	})
}

var idRegex = regexp.MustCompile("^[a-zA-Z0-9_-]{1,64}$")

type pgRepository struct {
	pool *pgxpool.Pool
}

func NewPgRepository(p *pgxpool.Pool) *pgRepository {
	return &pgRepository{pool: p}
}

type TelemetryMessage struct {
	DeviceID string
	Point    TelemetryPoint
}

type TelemetryBatchMessage struct {
	DeviceID string
	Points   []TelemetryPoint
}

type TelemetryPoint struct {
	Ts      int64
	Lat     float64
	Lon     float64
	Battery *float64
	Ax      float64
	Ay      float64
	Az      float64
}

type TelemetryRepository interface {
	SaveBatch(ctx context.Context, deviceID string, points []TelemetryPoint) (int, error)
}

func (pr *pgRepository) SaveBatch(
	ctx context.Context,
	deviceID string,
	points []TelemetryPoint,
) (int, error) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO device_telemetries (device_id, ts, lat, lon, battery, ax, ay, az) VALUES ")

	args := make([]any, 0, len(points)*8)
	for i, tp := range points {
		if i > 0 {
			sb.WriteString(", ")
		}

		base := i * 8
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8)

		args = append(args, deviceID, tp.Ts, tp.Lat, tp.Lon, tp.Battery, tp.Ax, tp.Ay, tp.Az)
	}

	tag, err := pr.pool.Exec(ctx, sb.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("exec batch insert: %v", err)
	}

	return int(tag.RowsAffected()), nil
}

type handler struct {
	deps *dependencies
	repo TelemetryRepository
}

func NewHandler(deps *dependencies, repo TelemetryRepository) *handler {
	return &handler{
		deps: deps,
		repo: repo,
	}
}

type TelemetryRequest struct {
	Ts      *int64   `json:"ts"`
	Lat     *float64 `json:"lat"`
	Lon     *float64 `json:"lon"`
	Battery *float64 `json:"battery,omitempty"`
	Ax      *float64 `json:"ax"`
	Ay      *float64 `json:"ay"`
	Az      *float64 `json:"az"`
}

func (tr TelemetryRequest) Validate() error {
	if tr.Ts == nil {
		return errors.New("timestamp field is required")
	} else if *tr.Ts < 0 {
		return errors.New("timestamp field must be positive")
	}

	if tr.Lat == nil {
		return errors.New("latitude field is required")
	} else if *tr.Lat < -90 || *tr.Lat > 90 {
		return errors.New("longitute must be whitin -90 and 90 interval")
	}

	if tr.Lon == nil {
		return errors.New("longitude field is required")
	} else if *tr.Lon < -180 || *tr.Lon > 180 {
		return errors.New("longitute must be whitin -180 and 180 interval")
	}

	if tr.Battery != nil {
		if *tr.Battery < 0 || *tr.Battery > 1 {
			return errors.New("battery must be whitin 0 and 1 interval")
		}
	}

	if tr.Ax == nil {
		return errors.New("ax field is required")
	} else if math.IsNaN(*tr.Ax) || math.IsInf(*tr.Ax, 0) {
		return errors.New("ax field must be a finite number")
	}

	if tr.Ay == nil {
		return errors.New("ay field is required")
	} else if math.IsNaN(*tr.Ay) || math.IsInf(*tr.Ay, 0) {
		return errors.New("ay field must be a finite number")
	}

	if tr.Az == nil {
		return errors.New("az field is required")
	} else if math.IsNaN(*tr.Az) || math.IsInf(*tr.Az, 0) {
		return errors.New("az field must be a finite number")
	}

	return nil
}

type TelemetryBatchRequest struct {
	Points []TelemetryRequest `json:"points"`
}

func (tbr TelemetryBatchRequest) Validate() error {
	totalPoints := len(tbr.Points)
	if totalPoints == 0 {
		return errors.New("points array cannot be empty")
	}
	if totalPoints > 100 {
		return errors.New("batch max capacity exceeded (limit is 100 points)")
	}

	for i, req := range tbr.Points {
		if err := req.Validate(); err != nil {
			return fmt.Errorf("invalid point at index %d: %v", i, err)
		}
	}

	return nil
}

func (h *handler) createTelemetryPoint(w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("id")
	if !idRegex.MatchString(deviceID) {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	var req TelemetryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msg := TelemetryMessage{
		DeviceID: deviceID,
		Point: TelemetryPoint{
			Ts:      *req.Ts,
			Lat:     *req.Lat,
			Lon:     *req.Lon,
			Battery: req.Battery,
			Ax:      *req.Ax,
			Ay:      *req.Ay,
			Az:      *req.Az,
		},
	}

	select {
	case h.deps.telemetryCh <- msg:
		w.WriteHeader(http.StatusAccepted)
	default:
		slog.Warn("backpressure activated, queue is full", "queue_size", QueueBufferSize)
		http.Error(w, "service unavailable, queue full", http.StatusServiceUnavailable)
	}
}

func (h *handler) createTelemetryPointBatch(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	deviceID := r.PathValue("id")
	if !idRegex.MatchString(deviceID) {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	var req TelemetryBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	points := make([]TelemetryPoint, len(req.Points))
	for i, p := range req.Points {
		points[i] = TelemetryPoint{
			Ts:      *p.Ts,
			Lat:     *p.Lat,
			Lon:     *p.Lon,
			Battery: p.Battery,
			Ax:      *p.Ax,
			Ay:      *p.Ay,
			Az:      *p.Az,
		}
	}

	acceptedCount, err := h.repo.SaveBatch(ctx, deviceID, points)
	if err != nil {
		slog.Error("database batch error", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]int{"accepted": acceptedCount})
}
