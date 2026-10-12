package client

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"cachekit/cluster"
	"cachekit/server"
	"cachekit/store"
)

// testNode is a bare server (no coordinator) the static-mode tests can stop and restart.
type testNode struct {
	id   string
	addr string
	st   *store.Store
	srv  *server.Server
	done chan struct{}
}

func startNode(t *testing.T, id, addr string) *testNode {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	n := &testNode{id: id, addr: ln.Addr().String(), st: store.New(0), done: make(chan struct{})}
	n.srv = server.New(n.st, server.Options{})
	go func() { n.srv.Serve(ln); close(n.done) }()
	t.Cleanup(n.stop)
	return n
}

func (n *testNode) stop() { n.srv.Close(); <-n.done }

func infos(nodes ...*testNode) []cluster.NodeInfo {
	out := make([]cluster.NodeInfo, len(nodes))
	for i, n := range nodes {
		out[i] = cluster.NodeInfo{ID: n.id, Addr: n.addr}
	}
	return out
}

func TestStaticRoutingAndOperations(t *testing.T) {
	nodes := []*testNode{startNode(t, "n1", "127.0.0.1:0"), startNode(t, "n2", "127.0.0.1:0"), startNode(t, "n3", "127.0.0.1:0")}
	c := NewStatic(infos(nodes...), Options{VNodes: 50})
	defer c.Close()

	if got := c.Nodes(); len(got) != 3 || got[0] != "n1" {
		t.Fatalf("Nodes() = %v", got)
	}
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("key:%d", i)
		if err := c.Set(k, []byte("v-"+k), 0); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}
	// Each key must live on exactly the node the ring names, and nowhere else.
	byID := map[string]*testNode{"n1": nodes[0], "n2": nodes[1], "n3": nodes[2]}
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("key:%d", i)
		owner, _ := c.Owner(k)
		for id, n := range byID {
			_, has := n.st.Get(k)
			if has != (id == owner) {
				t.Fatalf("key %s: owner is %s but node %s has=%v", k, owner, id, has)
			}
		}
		v, found, err := c.Get(k)
		if err != nil || !found || string(v) != "v-"+k {
			t.Fatalf("Get %s = %q,%v,%v", k, v, found, err)
		}
	}
	for _, n := range nodes {
		if n.st.Len() == 0 {
			t.Fatalf("node %s received no keys", n.id)
		}
	}
	if existed, err := c.Del("key:7"); err != nil || !existed {
		t.Fatalf("Del = %v,%v", existed, err)
	}
	if existed, _ := c.Del("key:7"); existed {
		t.Fatal("second Del should report false")
	}
	if _, found, _ := c.Get("key:7"); found {
		t.Fatal("deleted key still found")
	}
	if _, found, err := c.Get("never-set"); err != nil || found {
		t.Fatalf("miss = found %v err %v", found, err)
	}
}

func TestSetWithTTL(t *testing.T) {
	n := startNode(t, "n1", "127.0.0.1:0")
	c := NewStatic(infos(n), Options{})
	defer c.Close()
	if err := c.Set("k", []byte("v"), 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := c.Get("k"); !found {
		t.Fatal("key should exist right after Set")
	}
	time.Sleep(120 * time.Millisecond)
	if _, found, _ := c.Get("k"); found {
		t.Fatal("key should have expired")
	}
	// A TTL below 1ms must still be a valid (tiny) TTL, not "no expiry".
	if err := c.Set("tiny", []byte("v"), time.Microsecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, found, _ := c.Get("tiny"); found {
		t.Fatal("sub-millisecond TTL was treated as no expiry")
	}
}

func TestBinaryAndLargeValues(t *testing.T) {
	n := startNode(t, "n1", "127.0.0.1:0")
	c := NewStatic(infos(n), Options{})
	defer c.Close()
	big := make([]byte, 1<<20)
	for i := range big {
		big[i] = byte(i)
	}
	if err := c.Set("big", big, 0); err != nil {
		t.Fatal(err)
	}
	got, found, err := c.Get("big")
	if err != nil || !found || len(got) != len(big) || got[12345] != big[12345] {
		t.Fatalf("1MiB round trip failed: found=%v err=%v len=%d", found, err, len(got))
	}
	odd := []byte("a\r\nb\x00\xff")
	c.Set("odd", odd, 0)
	if got, _, _ := c.Get("odd"); string(got) != string(odd) {
		t.Fatalf("binary value corrupted: %q", got)
	}
}

func TestEmptyRing(t *testing.T) {
	c := NewStatic(nil, Options{})
	defer c.Close()
	if _, _, err := c.Get("k"); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("Get on empty ring = %v, want ErrNoNodes", err)
	}
	if err := c.Set("k", []byte("v"), 0); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("Set on empty ring = %v", err)
	}
	if _, ok := c.Owner("k"); ok {
		t.Fatal("Owner on empty ring")
	}
}

func TestNodeDownReturnsNodeErrorFast(t *testing.T) {
	n := startNode(t, "n1", "127.0.0.1:0")
	c := NewStatic(infos(n), Options{OpTimeout: 500 * time.Millisecond, DialTimeout: 500 * time.Millisecond})
	defer c.Close()
	if err := c.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	n.stop()

	start := time.Now()
	_, _, err := c.Get("k")
	var ne *NodeError
	if !errors.As(err, &ne) || ne.NodeID != "n1" {
		t.Fatalf("want *NodeError for n1, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("failure took %v; a dead node should fail fast", time.Since(start))
	}
}

func TestStaleConnectionIsRetriedTransparently(t *testing.T) {
	n := startNode(t, "n1", "127.0.0.1:0")
	addr := n.addr
	c := NewStatic(infos(n), Options{})
	defer c.Close()
	if err := c.Set("a", []byte("1"), 0); err != nil { // leaves a pooled connection behind
		t.Fatal(err)
	}
	if err := c.Set("b", []byte("2"), 0); err != nil {
		t.Fatal(err)
	}
	n.stop() // pooled connections are now dead

	// Node comes back on the same address (a restart).
	n2 := startNode(t, "n1", addr)
	n2.st.Set("a", []byte("fresh"), 0)
	v, found, err := c.Get("a")
	if err != nil || !found || string(v) != "fresh" {
		t.Fatalf("client should have reconnected transparently: %q %v %v", v, found, err)
	}
}

func TestServerErrorsSurface(t *testing.T) {
	n := startNode(t, "n1", "127.0.0.1:0")
	c := NewStatic(infos(n), Options{})
	defer c.Close()
	// Use the internal helper to send a bad command.
	_, err := c.do("k", []byte("BOGUS"))
	var se *ServerError
	if !errors.As(err, &se) {
		t.Fatalf("want *ServerError, got %v", err)
	}
	// The connection must remain usable after an error reply.
	if err := c.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
}

func TestApplyReplacesRingAndPools(t *testing.T) {
	n1 := startNode(t, "n1", "127.0.0.1:0")
	n2 := startNode(t, "n2", "127.0.0.1:0")
	c := NewStatic(infos(n1, n2), Options{VNodes: 50})
	defer c.Close()

	c.mu.RLock()
	p1 := c.pools["n1"]
	c.mu.RUnlock()

	// n2 leaves; n1 keeps the same address => its pool object is reused.
	c.apply(cluster.Membership{Epoch: "static", Version: 2, VNodes: 50, Nodes: infos(n1)})
	if got := c.Nodes(); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("Nodes after apply = %v", got)
	}
	c.mu.RLock()
	same := c.pools["n1"] == p1
	_, n2Left := c.pools["n2"]
	c.mu.RUnlock()
	if !same || n2Left {
		t.Fatalf("pool reuse wrong: same=%v n2StillPooled=%v", same, n2Left)
	}
	for i := 0; i < 50; i++ {
		if owner, _ := c.Owner(fmt.Sprint("k", i)); owner != "n1" {
			t.Fatalf("key routed to %s after n2 left", owner)
		}
	}

	// n1 changes address => a new pool is created.
	c.apply(cluster.Membership{Epoch: "static", Version: 3, VNodes: 50, Nodes: []cluster.NodeInfo{{ID: "n1", Addr: n2.addr}}})
	c.mu.RLock()
	changed := c.pools["n1"] != p1 && c.pools["n1"].addr == n2.addr
	c.mu.RUnlock()
	if !changed {
		t.Fatal("address change should replace the pool")
	}
}

func TestNewRequiresCoordinatorURL(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("New without CoordinatorURL must fail")
	}
	if _, err := New(Options{CoordinatorURL: "http://127.0.0.1:1"}); err == nil {
		t.Fatal("New with an unreachable coordinator must fail")
	}
}

// Run with -race: many goroutines share one client while membership changes.
func TestConcurrentUseDuringMembershipChanges(t *testing.T) {
	n1 := startNode(t, "n1", "127.0.0.1:0")
	n2 := startNode(t, "n2", "127.0.0.1:0")
	c := NewStatic(infos(n1, n2), Options{VNodes: 20})
	defer c.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("g%d-%d", g, i%50)
				c.Set(k, []byte("v"), 0)
				c.Get(k)
			}
		}(g)
	}
	for v := uint64(2); v < 40; v++ {
		nodes := infos(n1, n2)
		if v%2 == 0 {
			nodes = infos(n1)
		}
		c.apply(cluster.Membership{Epoch: "static", Version: v, VNodes: 20, Nodes: nodes})
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
