package client_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cachekit/client"
	"cachekit/cluster"
	"cachekit/coordinator"
	"cachekit/node"
)

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// TestClusterEndToEnd runs the whole system in one process: a coordinator, three
// real TCP cache nodes, and a client following membership changes. It exercises
// the story the project is about: keys spread over nodes, a node crashes, the
// coordinator notices, the client re-routes, only the dead node's keys are lost,
// the node returns, and a graceful leave is picked up immediately.
func TestClusterEndToEnd(t *testing.T) {
	const vnodes = 50
	coord := coordinator.New(vnodes, 400*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go coord.Run(ctx, 50*time.Millisecond)
	httpSrv := httptest.NewServer(coord.Handler())
	defer httpSrv.Close()

	start := func(id string) *node.Node {
		n, err := node.Start(node.Config{
			ID: id, Listen: "127.0.0.1:0", CoordinatorURL: httpSrv.URL,
			HeartbeatEvery: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	n1, n2, n3 := start("node-1"), start("node-2"), start("node-3")
	defer n1.Stop(false)
	defer n3.Stop(false)

	c, err := client.New(client.Options{CoordinatorURL: httpSrv.URL, PollWait: 2 * time.Second, OpTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// 1. The client discovers all three nodes.
	waitFor(t, "client sees 3 nodes", func() bool { return len(c.Nodes()) == 3 })

	// 2. Fill the cache; every key must be readable.
	const keys = 600
	owner := map[string]string{}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("user:%d", i)
		if err := c.Set(k, []byte(k+"-value"), 0); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		owner[k], _ = c.Owner(k)
	}
	for k := range owner {
		if v, found, err := c.Get(k); err != nil || !found || string(v) != k+"-value" {
			t.Fatalf("Get %s = %q,%v,%v", k, v, found, err)
		}
	}
	perNode := map[string]int{}
	for _, o := range owner {
		perNode[o]++
	}
	if len(perNode) != 3 {
		t.Fatalf("keys landed on %d nodes, want 3: %v", len(perNode), perNode)
	}

	// 3. node-2 crashes (no goodbye). Until the coordinator notices, operations
	//    on its keys fail with a NodeError rather than hanging or lying.
	n2.Stop(false)
	waitFor(t, "client drops crashed node-2", func() bool { return len(c.Nodes()) == 2 })

	// 4. After re-routing: no errors anywhere; only node-2's keys are gone.
	hits, misses := 0, 0
	for k, o := range owner {
		v, found, err := c.Get(k)
		if err != nil {
			t.Fatalf("Get %s after failover: %v", k, err)
		}
		if o == "node-2" {
			if found {
				t.Fatalf("key %s of the crashed node unexpectedly found (%q)", k, v)
			}
			misses++
		} else {
			if !found {
				t.Fatalf("key %s lives on surviving %s but was lost: consistent hashing must not move it", k, o)
			}
			if now, _ := c.Owner(k); now != o {
				t.Fatalf("key %s moved from %s to %s although %s survived", k, o, now, o)
			}
			hits++
		}
	}
	if misses != perNode["node-2"] || hits != keys-perNode["node-2"] {
		t.Fatalf("hits=%d misses=%d, want %d/%d", hits, misses, keys-perNode["node-2"], perNode["node-2"])
	}
	// The cache is usable again: refill the lost keys on their new owners.
	for k, o := range owner {
		if o == "node-2" {
			if err := c.Set(k, []byte("refilled"), 0); err != nil {
				t.Fatalf("refill %s: %v", k, err)
			}
		}
	}

	// 5. node-2 restarts (new port, same ID): it gets its old ring positions
	//    back, so its old keys route to it again — to an empty cache (misses).
	n2b := start("node-2")
	defer n2b.Stop(false)
	waitFor(t, "client sees node-2 again", func() bool { return len(c.Nodes()) == 3 })
	for k, o := range owner {
		now, _ := c.Owner(k)
		if now != o {
			t.Fatalf("key %s: owner is %s after rejoin, was %s before the crash", k, now, o)
		}
		if _, _, err := c.Get(k); err != nil {
			t.Fatalf("Get %s after rejoin: %v", k, err)
		}
	}

	// 6. node-3 leaves gracefully: removed from the ring right away, without
	//    waiting for the heartbeat timeout.
	n3.Stop(true)
	waitFor(t, "client drops gracefully-left node-3", func() bool { return len(c.Nodes()) == 2 })

	// 7. The coordinator's log tells the whole story.
	var types []string
	for _, e := range coord.Events(0) {
		types = append(types, e.Type+":"+e.NodeID)
	}
	got := strings.Join(types, " ")
	for _, want := range []string{"join:node-1", "join:node-2", "join:node-3", "dead:node-2", "leave:node-3"} {
		if !strings.Contains(got, want) {
			t.Errorf("event log %q is missing %q", got, want)
		}
	}
	// node-2 must have joined twice: at start and after the restart.
	if strings.Count(got, "join:node-2") != 2 {
		t.Errorf("expected node-2 to join twice, log: %s", got)
	}
}

// TestClientSurvivesCoordinatorRestartEpoch checks the epoch logic: when the
// coordinator restarts (new epoch, version counter back to zero) the client must
// pick up the new membership instead of assuming "same version, no change".
func TestClientSurvivesCoordinatorRestartEpoch(t *testing.T) {
	coordA := coordinator.New(50, time.Hour)
	// A swappable handler lets the test "restart" the coordinator behind one URL.
	var current atomic.Pointer[http.Handler]
	hA := coordA.Handler()
	current.Store(&hA)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		(*current.Load()).ServeHTTP(w, r)
	}))
	defer srv.Close()
	coordA.Heartbeat(cluster.NodeInfo{ID: "a", Addr: "127.0.0.1:1"})

	c, err := client.New(client.Options{CoordinatorURL: srv.URL, PollWait: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitFor(t, "client sees node a", func() bool { return len(c.Nodes()) == 1 && c.Nodes()[0] == "a" })

	// "Restart" the coordinator: swap in a fresh one behind the same URL. Its
	// first membership change also yields version 1, the same number the old
	// coordinator had, so only the epoch tells them apart.
	coordB := coordinator.New(50, time.Hour)
	coordB.Heartbeat(cluster.NodeInfo{ID: "b", Addr: "127.0.0.1:2"})
	hB := coordB.Handler()
	current.Store(&hB)

	waitFor(t, "client switches to the new coordinator's membership", func() bool {
		n := c.Nodes()
		return len(n) == 1 && n[0] == "b"
	})
}
