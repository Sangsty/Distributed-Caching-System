// Package node wraps one cache node's whole lifecycle: an LRU/TTL store, the
// RESP server in front of it, and (optionally) the heartbeat loop that keeps it
// registered with the coordinator. cmd/node is a thin main() around this, and
// the end-to-end tests start several nodes in-process through it.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"cachekit/cluster"
	"cachekit/server"
	"cachekit/store"
)

// Config describes one node.
type Config struct {
	ID             string        // stable name hashed onto the ring (required)
	Listen         string        // address to bind, e.g. ":7001" or "127.0.0.1:0"
	Advertise      string        // host:port clients should dial; derived from Listen if empty
	CoordinatorURL string        // e.g. "http://coordinator:8080"; empty = standalone, no registration
	MaxItems       int           // LRU capacity (0 = unlimited)
	HeartbeatEvery time.Duration // default 1s
	Logger         *slog.Logger
}

// Node is a running cache node.
type Node struct {
	cfg  Config
	log  *slog.Logger
	st   *store.Store
	srv  *server.Server
	addr string // advertised address

	stopJanitor func()
	cancelHB    context.CancelFunc
	hbDone      chan struct{}
	serveDone   chan struct{}
	httpc       *http.Client
	stopOnce    sync.Once
}

// Start binds the listener, begins serving, and (if CoordinatorURL is set)
// starts heartbeating. It returns once the node is accepting connections.
func Start(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("node: ID is required")
	}
	if cfg.Listen == "" {
		cfg.Listen = ":7001"
	}
	if cfg.HeartbeatEvery <= 0 {
		cfg.HeartbeatEvery = time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("node", cfg.ID)

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("node: listen on %q: %w", cfg.Listen, err)
	}
	advertise := cfg.Advertise
	if advertise == "" {
		advertise = defaultAdvertise(ln.Addr())
	}

	n := &Node{
		cfg:       cfg,
		log:       log,
		st:        store.New(cfg.MaxItems),
		addr:      advertise,
		hbDone:    make(chan struct{}),
		serveDone: make(chan struct{}),
		httpc:     &http.Client{Timeout: 2 * time.Second},
	}
	n.srv = server.New(n.st, server.Options{Logger: log})
	n.stopJanitor = n.st.StartJanitor(time.Second, 200)

	go func() {
		defer close(n.serveDone)
		if err := n.srv.Serve(ln); err != nil {
			log.Error("serve failed", "err", err)
		}
	}()
	log.Info("cache node listening", "listen", ln.Addr().String(), "advertise", advertise, "max_items", cfg.MaxItems)

	if cfg.CoordinatorURL != "" {
		ctx, cancel := context.WithCancel(context.Background())
		n.cancelHB = cancel
		go n.heartbeatLoop(ctx)
	} else {
		close(n.hbDone)
	}
	return n, nil
}

// defaultAdvertise turns the bound address into something a client on the same
// machine can dial: wildcard hosts (":7001", "0.0.0.0") become 127.0.0.1.
func defaultAdvertise(a net.Addr) string {
	host, port, err := net.SplitHostPort(a.String())
	if err != nil {
		return a.String()
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// Addr returns the address clients should use to reach this node.
func (n *Node) Addr() string { return n.addr }

// ID returns the node's ring identity.
func (n *Node) ID() string { return n.cfg.ID }

// Store exposes the underlying store (handy in tests).
func (n *Node) Store() *store.Store { return n.st }

func (n *Node) heartbeatLoop(ctx context.Context) {
	defer close(n.hbDone)
	t := time.NewTicker(n.cfg.HeartbeatEvery)
	defer t.Stop()
	healthy := true // log only on state changes, not on every tick
	for {
		err := n.post(ctx, "/heartbeat", cluster.NodeInfo{ID: n.cfg.ID, Addr: n.addr})
		switch {
		case ctx.Err() != nil:
			return
		case err != nil && healthy:
			healthy = false
			n.log.Warn("coordinator unreachable; will keep retrying", "err", err)
		case err == nil && !healthy:
			healthy = true
			n.log.Info("registered with coordinator again")
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

func (n *Node) post(ctx context.Context, path string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(n.cfg.CoordinatorURL, "/")+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("coordinator returned %s", resp.Status)
	}
	return nil
}

// Stop shuts the node down. With graceful=true it tells the coordinator it is
// leaving, so clients drop it immediately; with graceful=false it just goes
// quiet, like a crash, and the coordinator notices after its heartbeat timeout.
func (n *Node) Stop(graceful bool) {
	n.stopOnce.Do(func() {
		if n.cancelHB != nil {
			n.cancelHB()
		}
		<-n.hbDone // no heartbeat can race with the deregister below
		if graceful && n.cfg.CoordinatorURL != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := n.post(ctx, "/deregister", map[string]string{"id": n.cfg.ID}); err != nil {
				n.log.Warn("deregister failed", "err", err)
			}
			cancel()
		}
		n.srv.Close()
		<-n.serveDone
		n.stopJanitor()
		n.log.Info("cache node stopped", "graceful", graceful)
	})
}
