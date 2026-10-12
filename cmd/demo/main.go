// Command demo runs the product-catalog demo application: an HTTP API whose
// reads go through the distributed cache (cache-aside) and fall back to
// PostgreSQL.
//
//	# with the full stack (coordinator + 3 nodes + Postgres):
//	docker compose -f deploy/docker-compose.yml up --build
//	curl -i localhost:9090/products/42
//
//	# no Docker, no database: in-memory products, optional cache
//	go run ./cmd/demo -store memory                          # no cache (baseline)
//	go run ./cmd/demo -store memory -coordinator http://localhost:8080
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cachekit/api"
	"cachekit/catalog"
	"cachekit/client"
)

func main() {
	if err := run(); err != nil {
		slog.Error("demo failed", "err", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run() error {
	listen := flag.String("listen", ":9090", "HTTP listen address")
	coordURL := flag.String("coordinator", envOr("COORDINATOR_URL", ""), "cache coordinator URL (env COORDINATOR_URL); empty = run without a cache")
	storeKind := flag.String("store", "postgres", "product store: postgres | memory")
	dbURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (env DATABASE_URL)")
	seed := flag.Int("seed", 1000, "products to create in the memory store")
	maxConns := flag.Int("max-db-conns", 20, "maximum open PostgreSQL connections")
	cacheTTL := flag.Duration("cache-ttl", 60*time.Second, "lifetime of a cached product (±10% jitter)")
	negTTL := flag.Duration("negative-ttl", 15*time.Second, "lifetime of a cached 'product does not exist'")
	opTimeout := flag.Duration("op-timeout", 250*time.Millisecond, "timeout for one cache operation")
	dialTimeout := flag.Duration("dial-timeout", 250*time.Millisecond, "timeout for connecting to a cache node")
	simLatency := flag.Duration("simulate-db-latency", 0, "add this delay to every database call (demo/benchmark only)")
	flag.Parse()

	log := slog.Default()

	// ---- product store ----
	var (
		repo catalog.Repository
		ping func(context.Context) error
	)
	switch *storeKind {
	case "memory":
		m := catalog.NewMemoryRepo()
		m.Seed(*seed)
		repo = m
		log.Info("using the in-memory product store", "products", *seed)
	case "postgres":
		if *dbURL == "" {
			return errors.New("-store postgres needs a connection URL: set DATABASE_URL or -database-url (or use -store memory)")
		}
		db, err := openPostgres(*dbURL, *maxConns)
		if err != nil {
			return err
		}
		defer db.Close()
		repo = catalog.NewPostgres(db)
		ping = db.PingContext
		log.Info("using PostgreSQL", "max_conns", *maxConns)
	default:
		return fmt.Errorf("unknown -store %q (want postgres or memory)", *storeKind)
	}
	if *simLatency > 0 {
		repo = catalog.WithLatency(repo, *simLatency)
		log.Warn("simulating database latency (demo only)", "latency", *simLatency)
	}

	// ---- cache ----
	var cache catalog.Cache = catalog.NopCache{}
	if *coordURL != "" {
		c, err := connectCache(*coordURL, client.Options{DialTimeout: *dialTimeout, OpTimeout: *opTimeout}, 30*time.Second, log)
		if err != nil {
			return err
		}
		defer c.Close()
		cache = c
		log.Info("distributed cache connected", "coordinator", *coordURL, "nodes", c.Nodes())
	} else {
		log.Warn("no -coordinator given: running WITHOUT a cache, every read goes to the database")
	}

	svc := catalog.NewService(repo, cache, catalog.Config{TTL: *cacheTTL, NegativeTTL: *negTTL}, log)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           api.NewHandler(svc, api.Options{Ping: ping, Logger: log}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("demo API listening", "addr", *listen)

	select {
	case err := <-errc:
		return err // failed to start (e.g. port in use)
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// openPostgres opens a connection pool and waits (up to 30s) for the database
// to accept connections, because in docker compose it may still be starting.
func openPostgres(url string, maxConns int) (*sql.DB, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(30 * time.Minute)

	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := db.PingContext(ctx)
		cancel()
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			db.Close()
			return nil, fmt.Errorf("database not reachable after 30s: %w", err)
		}
		slog.Info("waiting for the database", "err", err)
		time.Sleep(time.Second)
	}
}

// connectCache creates the cache client, retrying while the coordinator is
// still starting.
func connectCache(coordURL string, opts client.Options, wait time.Duration, log *slog.Logger) (*client.Client, error) {
	deadline := time.Now().Add(wait)
	for {
		c, err := client.New(client.Options{
			CoordinatorURL: coordURL, DialTimeout: opts.DialTimeout, OpTimeout: opts.OpTimeout,
		})
		if err == nil {
			return c, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("cache coordinator not reachable after %s: %w", wait, err)
		}
		log.Info("waiting for the cache coordinator", "err", err)
		time.Sleep(time.Second)
	}
}
