// Command coordinator runs the cluster control plane: it tracks node
// heartbeats and serves the ring to clients.
//
//	go run ./cmd/coordinator -listen :8080
//
// Try:  curl http://localhost:8080/status
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cachekit/coordinator"
	"cachekit/ring"
)

func main() {
	listen := flag.String("listen", ":8080", "HTTP listen address")
	vnodes := flag.Int("vnodes", ring.DefaultVNodes, "virtual nodes per node (clients adopt this value)")
	timeout := flag.Duration("timeout", 3*time.Second, "declare a node dead after this long without a heartbeat (use >= 3x the node heartbeat interval)")
	check := flag.Duration("check", 500*time.Millisecond, "how often to look for dead nodes")
	flag.Parse()

	c := coordinator.New(*vnodes, *timeout)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go c.Run(ctx, *check)

	// No WriteTimeout: /ring long-polls for up to 30s and must not be cut off.
	srv := &http.Server{Addr: *listen, Handler: c.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	slog.Info("coordinator listening", "addr", *listen, "vnodes", *vnodes, "dead_after", *timeout)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
