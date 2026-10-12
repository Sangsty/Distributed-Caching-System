// Command node runs one cache node.
//
//	go run ./cmd/node -id node-1 -listen :7001 -coordinator http://localhost:8080
//
// Without -coordinator the node runs standalone (still usable with redis-cli).
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cachekit/node"
)

func main() {
	hostname, _ := os.Hostname()
	id := flag.String("id", hostname, "stable node name hashed onto the ring (must be unique and survive restarts)")
	listen := flag.String("listen", ":7001", "address to listen on")
	advertise := flag.String("advertise", "", "host:port clients should dial (default: derived from -listen, wildcard host becomes 127.0.0.1)")
	coord := flag.String("coordinator", "", "coordinator base URL, e.g. http://localhost:8080 (empty = standalone)")
	maxItems := flag.Int("max-items", 100000, "maximum number of keys before LRU eviction (0 = unlimited)")
	hb := flag.Duration("heartbeat", time.Second, "heartbeat interval")
	flag.Parse()

	n, err := node.Start(node.Config{
		ID: *id, Listen: *listen, Advertise: *advertise, CoordinatorURL: *coord,
		MaxItems: *maxItems, HeartbeatEvery: *hb, Logger: slog.Default(),
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	n.Stop(true) // tell the coordinator we are leaving, so clients drop us immediately
}
