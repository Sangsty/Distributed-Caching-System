package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cachekit/cluster"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTest() (*Coordinator, *fakeClock) {
	c := New(50, 3*time.Second)
	clk := &fakeClock{t: time.Unix(2_000_000, 0)}
	c.now = clk.Now
	return c, clk
}

func node(id, addr string) cluster.NodeInfo { return cluster.NodeInfo{ID: id, Addr: addr} }

func ids(m cluster.Membership) []string {
	out := []string{}
	for _, n := range m.Nodes {
		out = append(out, n.ID)
	}
	return out
}

func TestJoinAndMembership(t *testing.T) {
	c, _ := newTest()
	if m := c.Membership(); len(m.Nodes) != 0 || m.Nodes == nil || m.VNodes != 50 || m.Epoch == "" {
		t.Fatalf("fresh membership = %+v", m)
	}
	if err := c.Heartbeat(node("b", "127.0.0.1:2")); err != nil {
		t.Fatal(err)
	}
	c.Heartbeat(node("a", "127.0.0.1:1"))
	m := c.Membership()
	if got := ids(m); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("nodes = %v (want sorted a,b)", got)
	}
	if m.Version != 2 {
		t.Fatalf("version = %d, want 2", m.Version)
	}
}

func TestHeartbeatRefreshDoesNotBumpVersion(t *testing.T) {
	c, clk := newTest()
	c.Heartbeat(node("a", "127.0.0.1:1"))
	v := c.Membership().Version
	for i := 0; i < 5; i++ {
		clk.Advance(time.Second)
		c.Heartbeat(node("a", "127.0.0.1:1"))
		c.CheckFailures()
	}
	if c.Membership().Version != v {
		t.Fatal("routine heartbeats must not change the version")
	}
	if len(c.Membership().Nodes) != 1 {
		t.Fatal("node with regular heartbeats must stay alive")
	}
}

func TestFailureDetectionAndRejoin(t *testing.T) {
	c, clk := newTest()
	c.Heartbeat(node("a", "127.0.0.1:1"))
	c.Heartbeat(node("b", "127.0.0.1:2"))
	v := c.Membership().Version

	clk.Advance(2 * time.Second)
	c.Heartbeat(node("a", "127.0.0.1:1")) // only a keeps beating
	if dead := c.CheckFailures(); len(dead) != 0 {
		t.Fatalf("nothing should be dead at 2s, got %v", dead)
	}
	clk.Advance(2 * time.Second) // b silent for 4s > 3s timeout
	dead := c.CheckFailures()
	if len(dead) != 1 || dead[0] != "b" {
		t.Fatalf("dead = %v, want [b]", dead)
	}
	m := c.Membership()
	if len(m.Nodes) != 1 || m.Nodes[0].ID != "a" || m.Version != v+1 {
		t.Fatalf("after death: %+v", m)
	}
	if again := c.CheckFailures(); len(again) != 0 {
		t.Fatal("a dead node must be reported only once")
	}

	// b comes back, possibly on a new port.
	c.Heartbeat(node("b", "127.0.0.1:9999"))
	m = c.Membership()
	if len(m.Nodes) != 2 || m.Nodes[1].Addr != "127.0.0.1:9999" || m.Version != v+2 {
		t.Fatalf("after rejoin: %+v", m)
	}
	evs := c.Events(0)
	var types []string
	for _, e := range evs {
		types = append(types, e.Type+":"+e.NodeID)
	}
	want := "join:a join:b dead:b join:b"
	if strings.Join(types, " ") != want {
		t.Fatalf("events = %v, want %s", types, want)
	}
}

func TestAddressChangeBumpsVersion(t *testing.T) {
	c, _ := newTest()
	c.Heartbeat(node("a", "127.0.0.1:1"))
	v := c.Membership().Version
	c.Heartbeat(node("a", "127.0.0.1:2"))
	m := c.Membership()
	if m.Version != v+1 || m.Nodes[0].Addr != "127.0.0.1:2" {
		t.Fatalf("address change not published: %+v", m)
	}
	if evs := c.Events(0); evs[len(evs)-1].Type != cluster.EventUpdate {
		t.Fatalf("last event = %+v", evs[len(evs)-1])
	}
}

func TestDeregister(t *testing.T) {
	c, _ := newTest()
	c.Heartbeat(node("a", "127.0.0.1:1"))
	v := c.Membership().Version
	if !c.Deregister("a") {
		t.Fatal("known node should deregister")
	}
	if c.Deregister("a") {
		t.Fatal("second deregister should report false")
	}
	if m := c.Membership(); len(m.Nodes) != 0 || m.Version != v+1 {
		t.Fatalf("membership after leave = %+v", m)
	}
}

func TestDeregisterDeadNodeDoesNotBump(t *testing.T) {
	c, clk := newTest()
	c.Heartbeat(node("a", "127.0.0.1:1"))
	clk.Advance(10 * time.Second)
	c.CheckFailures()
	v := c.Membership().Version
	if !c.Deregister("a") {
		t.Fatal("dead node is still known")
	}
	if c.Membership().Version != v {
		t.Fatal("removing an already-dead node must not bump the version")
	}
}

func TestPruneLongDeadNodes(t *testing.T) {
	c, clk := newTest()
	c.Heartbeat(node("a", "127.0.0.1:1"))
	clk.Advance(10 * time.Second)
	c.CheckFailures()
	if len(c.Status().Nodes) != 1 {
		t.Fatal("dead node should still be visible in Status")
	}
	clk.Advance(2 * time.Hour)
	c.CheckFailures()
	if len(c.Status().Nodes) != 0 {
		t.Fatal("long-dead node should be forgotten")
	}
}

func TestValidation(t *testing.T) {
	c, _ := newTest()
	for name, n := range map[string]cluster.NodeInfo{
		"empty id":      node("", "127.0.0.1:1"),
		"space in id":   node("a b", "127.0.0.1:1"),
		"no port":       node("a", "127.0.0.1"),
		"no host":       node("a", ":7001"),
		"empty addr":    node("a", ""),
		"id too long":   node(strings.Repeat("x", 200), "127.0.0.1:1"),
		"garbage addr":  node("a", "not an address"),
		"missing port2": node("a", "host:"),
	} {
		if err := c.Heartbeat(n); !errors.Is(err, ErrInvalidNode) {
			t.Errorf("%s: want ErrInvalidNode, got %v", name, err)
		}
	}
	if len(c.Membership().Nodes) != 0 {
		t.Fatal("invalid heartbeats must not register nodes")
	}
}

func TestWaitForChange(t *testing.T) {
	c, _ := newTest()
	m0 := c.Membership()

	// 1. Unblocks when membership changes.
	res := make(chan cluster.Membership, 1)
	go func() { res <- c.WaitForChange(context.Background(), m0.Epoch, m0.Version) }()
	select {
	case <-res:
		t.Fatal("returned before any change")
	case <-time.After(50 * time.Millisecond):
	}
	c.Heartbeat(node("a", "127.0.0.1:1"))
	select {
	case m := <-res:
		if len(m.Nodes) != 1 {
			t.Fatalf("got %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForChange did not wake up")
	}

	// 2. Returns on context timeout when nothing changes.
	m1 := c.Membership()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	m := c.WaitForChange(ctx, m1.Epoch, m1.Version)
	if m.Version != m1.Version || time.Since(start) < 40*time.Millisecond {
		t.Fatalf("timeout path wrong: %+v after %v", m, time.Since(start))
	}

	// 3. Already-stale snapshot returns immediately; so does a different epoch.
	start = time.Now()
	c.WaitForChange(context.Background(), m1.Epoch, m1.Version-1)
	c.WaitForChange(context.Background(), "some-other-epoch", m1.Version)
	if time.Since(start) > time.Second {
		t.Fatal("stale snapshot should be answered immediately")
	}
}

func TestStatusOwnership(t *testing.T) {
	c, clk := newTest()
	c.Heartbeat(node("a", "127.0.0.1:1"))
	c.Heartbeat(node("b", "127.0.0.1:2"))
	c.Heartbeat(node("c", "127.0.0.1:3"))
	clk.Advance(5 * time.Second)
	c.Heartbeat(node("a", "127.0.0.1:1"))
	c.Heartbeat(node("b", "127.0.0.1:2"))
	c.CheckFailures() // c dies

	st := c.Status()
	if len(st.Nodes) != 3 {
		t.Fatalf("status nodes = %+v", st.Nodes)
	}
	sum := 0.0
	for _, n := range st.Nodes {
		if n.ID == "c" {
			if n.Alive || n.Ownership != 0 {
				t.Fatalf("dead node reported as %+v", n)
			}
			continue
		}
		if !n.Alive || n.Ownership <= 0.2 {
			t.Fatalf("live node %+v", n)
		}
		sum += n.Ownership
	}
	if sum < 0.999 || sum > 1.001 {
		t.Fatalf("live ownership sums to %v", sum)
	}
}

func TestEventLogIsBounded(t *testing.T) {
	c, _ := newTest()
	for i := 0; i < maxEvents+50; i++ {
		c.Heartbeat(node("a", "127.0.0.1:1"))
		c.Deregister("a")
	}
	evs := c.Events(0)
	if len(evs) != maxEvents {
		t.Fatalf("event log holds %d entries, want %d", len(evs), maxEvents)
	}
	if evs[len(evs)-1].Seq != uint64(2*(maxEvents+50)) {
		t.Fatalf("last seq = %d", evs[len(evs)-1].Seq)
	}
	if got := c.Events(evs[len(evs)-1].Seq); len(got) != 0 {
		t.Fatal("Events(last) should be empty")
	}
}

func TestRunMarksNodesDead(t *testing.T) {
	c := New(10, 100*time.Millisecond) // real clock
	c.Heartbeat(node("a", "127.0.0.1:1"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx, 20*time.Millisecond); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.Membership().Nodes) == 0 {
			cancel()
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Run never marked the silent node dead")
}

// ------------------------------------------------------------------ HTTP

func post(t *testing.T, h http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	return rec
}

func TestHTTPHeartbeatRingStatusEvents(t *testing.T) {
	c, _ := newTest()
	h := c.Handler()

	if rec := post(t, h, "/heartbeat", `{"id":"a","addr":"127.0.0.1:7001"}`); rec.Code != 200 {
		t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body)
	}
	rec := get(t, h, "/ring")
	var m cluster.Membership
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil || rec.Code != 200 {
		t.Fatalf("ring: %d %v %s", rec.Code, err, rec.Body)
	}
	if len(m.Nodes) != 1 || m.Nodes[0].Addr != "127.0.0.1:7001" || m.VNodes != 50 {
		t.Fatalf("ring = %+v", m)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("ring responses must not be cached")
	}

	var st cluster.Status
	json.Unmarshal(get(t, h, "/status").Body.Bytes(), &st)
	if len(st.Nodes) != 1 || st.Nodes[0].Ownership != 1 {
		t.Fatalf("status = %+v", st)
	}
	var evs []cluster.Event
	json.Unmarshal(get(t, h, "/events?since=0").Body.Bytes(), &evs)
	if len(evs) != 1 || evs[0].Type != cluster.EventJoin {
		t.Fatalf("events = %+v", evs)
	}
	if rec := get(t, h, "/healthz"); rec.Code != 200 {
		t.Fatal("healthz")
	}

	if rec := post(t, h, "/deregister", `{"id":"a"}`); rec.Code != 200 || !strings.Contains(rec.Body.String(), "true") {
		t.Fatalf("deregister: %d %s", rec.Code, rec.Body)
	}
	if n := len(c.Membership().Nodes); n != 0 {
		t.Fatalf("node still present after deregister: %d", n)
	}
}

func TestHTTPBadInput(t *testing.T) {
	c, _ := newTest()
	h := c.Handler()
	cases := []struct {
		name string
		rec  *httptest.ResponseRecorder
		want int
	}{
		{"bad json", post(t, h, "/heartbeat", `{nope`), 400},
		{"unknown field", post(t, h, "/heartbeat", `{"id":"a","addr":"h:1","x":1}`), 400},
		{"bad addr", post(t, h, "/heartbeat", `{"id":"a","addr":"nope"}`), 400},
		{"empty body", post(t, h, "/heartbeat", ``), 400},
		{"oversized body", post(t, h, "/heartbeat", `{"id":"`+strings.Repeat("a", 10000)+`"}`), 400},
		{"bad since", get(t, h, "/ring?since=abc"), 400},
		{"bad wait", get(t, h, "/ring?since=1&wait=forever"), 400},
		{"bad events since", get(t, h, "/events?since=-1"), 400},
		{"wrong method", get(t, h, "/heartbeat"), 405},
		{"unknown path", get(t, h, "/nope"), 404},
	}
	for _, tc := range cases {
		if tc.rec.Code != tc.want {
			t.Errorf("%s: status %d, want %d (%s)", tc.name, tc.rec.Code, tc.want, tc.rec.Body)
		}
	}
	if len(c.Membership().Nodes) != 0 {
		t.Fatal("bad requests must not register nodes")
	}
}

func TestHTTPLongPoll(t *testing.T) {
	c := New(50, time.Hour)
	srv := httptest.NewServer(c.Handler())
	defer srv.Close()
	m0 := c.Membership()

	type result struct {
		m   cluster.Membership
		err error
	}
	res := make(chan result, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/ring?since=0&epoch=" + m0.Epoch + "&wait=5s")
		if err != nil {
			res <- result{err: err}
			return
		}
		defer resp.Body.Close()
		var m cluster.Membership
		err = json.NewDecoder(resp.Body).Decode(&m)
		res <- result{m, err}
	}()
	time.Sleep(100 * time.Millisecond)
	c.Heartbeat(node("a", "127.0.0.1:1"))
	select {
	case r := <-res:
		if r.err != nil || len(r.m.Nodes) != 1 || r.m.Version != 1 {
			t.Fatalf("long poll result: %+v %v", r.m, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not return after a membership change")
	}

	// With nothing changing, it returns after `wait` with the same version.
	start := time.Now()
	resp, err := http.Get(srv.URL + "/ring?since=1&epoch=" + m0.Epoch + "&wait=200ms")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if d := time.Since(start); d < 150*time.Millisecond || d > 2*time.Second {
		t.Fatalf("long poll returned after %v, want ~200ms", d)
	}
}
