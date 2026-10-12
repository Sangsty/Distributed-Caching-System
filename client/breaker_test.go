package client

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cachekit/cluster"
)

// ---- state machine, with a fake clock ---------------------------------------

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}
func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}

func testBreaker(threshold int, cooldown time.Duration) (*breaker, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	b := newBreaker(threshold, cooldown)
	b.now = clk.now
	return b, clk
}

func TestBreakerOpensAfterThresholdConsecutiveFailures(t *testing.T) {
	b, _ := testBreaker(3, time.Second)
	for i := 0; i < 2; i++ {
		if !b.allow() {
			t.Fatalf("closed breaker must allow (failure %d)", i)
		}
		b.failure()
	}
	if b.isOpen() {
		t.Fatal("two failures are below the threshold of three")
	}
	b.success() // a success resets the run of failures
	b.failure()
	b.failure()
	if b.isOpen() {
		t.Fatal("failures must be consecutive: the success reset the count")
	}
	b.failure()
	if !b.isOpen() || b.allow() {
		t.Fatal("three consecutive failures must open the breaker and refuse calls")
	}
}

func TestBreakerHalfOpenAllowsExactlyOneProbe(t *testing.T) {
	b, clk := testBreaker(1, time.Second)
	b.failure()
	if b.allow() {
		t.Fatal("open breaker must refuse")
	}
	clk.advance(999 * time.Millisecond)
	if b.allow() {
		t.Fatal("still inside the cooldown")
	}
	clk.advance(2 * time.Millisecond)
	if !b.allow() {
		t.Fatal("after the cooldown one probe must be allowed")
	}
	if b.allow() || b.allow() {
		t.Fatal("while the probe is in flight no other call may pass")
	}
	b.success() // probe succeeded: node is back
	if b.isOpen() || !b.allow() {
		t.Fatal("a successful probe must close the breaker")
	}
}

func TestBreakerFailedProbeReopensForAnotherCooldown(t *testing.T) {
	b, clk := testBreaker(1, time.Second)
	b.failure()
	clk.advance(time.Second)
	if !b.allow() {
		t.Fatal("probe expected")
	}
	b.failure() // probe failed
	if b.allow() {
		t.Fatal("must be open again right after a failed probe")
	}
	clk.advance(time.Second)
	if !b.allow() {
		t.Fatal("a new probe is allowed after another cooldown")
	}
}

func TestBreakerLateFailureDoesNotExtendCooldown(t *testing.T) {
	b, clk := testBreaker(1, time.Second)
	b.failure() // opens at t0, until t0+1s
	clk.advance(900 * time.Millisecond)
	b.failure() // a call that was already in flight fails late
	clk.advance(150 * time.Millisecond)
	if !b.allow() {
		t.Fatal("a late failure must not push the reopening time back")
	}
}

func TestBreakerNilAndDisabled(t *testing.T) {
	var b *breaker
	if !b.allow() || b.isOpen() {
		t.Fatal("nil breaker is always closed")
	}
	b.failure()
	b.success()
	if newBreaker(-1, time.Second) != nil {
		t.Fatal("negative threshold must disable the breaker")
	}
	d := newBreaker(0, 0)
	if d.threshold != defaultBreakerThreshold || d.cooldown != defaultBreakerCooldown {
		t.Fatalf("defaults not applied: %+v", d)
	}
}

func TestBreakerConcurrentUse(t *testing.T) { // run with -race
	b := newBreaker(3, time.Millisecond)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				if b.allow() {
					if (i+g)%3 == 0 {
						b.success()
					} else {
						b.failure()
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// ---- end to end against a node that can be made unresponsive ----------------

// flakyNode accepts connections and answers every request with "+OK" while
// healthy; while "sick" it reads requests and never replies (like a hung or
// firewalled node), so each client call waits out its full timeout.
type flakyNode struct {
	ln    net.Listener
	sick  atomic.Bool
	calls atomic.Int64 // requests that reached the node
}

func newFlakyNode(t *testing.T) *flakyNode {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &flakyNode{ln: ln}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
					f.calls.Add(1)
					if !f.sick.Load() {
						c.Write([]byte("+OK\r\n"))
					}
				}
			}(c)
		}
	}()
	return f
}

func (f *flakyNode) info() []cluster.NodeInfo {
	return []cluster.NodeInfo{{ID: "flaky", Addr: f.ln.Addr().String()}}
}

func timed(f func() error) (time.Duration, error) {
	start := time.Now()
	err := f()
	return time.Since(start), err
}

func TestBreakerEndToEndFastFailAndRecovery(t *testing.T) {
	const opTimeout = 150 * time.Millisecond
	f := newFlakyNode(t)
	c := NewStatic(f.info(), Options{OpTimeout: opTimeout, BreakerThreshold: 2, BreakerCooldown: 400 * time.Millisecond})
	defer c.Close()
	set := func() error { return c.Set("k", []byte("v"), 0) }

	if _, err := timed(set); err != nil {
		t.Fatalf("healthy node: %v", err)
	}

	f.sick.Store(true)
	for i := 0; i < 2; i++ { // the first two calls pay the full timeout and trip the breaker
		d, err := timed(set)
		var ne *NodeError
		if !errors.As(err, &ne) || errors.Is(err, ErrCircuitOpen) || d < opTimeout/2 {
			t.Fatalf("call %d: want a real timeout (>= %v), got %v after %v", i, opTimeout/2, err, d)
		}
	}
	before := f.calls.Load()
	d, err := timed(set)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("breaker should be open, got %v", err)
	}
	var ne *NodeError
	if !errors.As(err, &ne) || ne.NodeID != "flaky" {
		t.Fatalf("an open breaker must still be a *NodeError naming the node: %v", err)
	}
	if d > 20*time.Millisecond {
		t.Fatalf("open breaker took %v; it must fail instantly", d)
	}
	if f.calls.Load() != before {
		t.Fatal("an open breaker must not send anything to the node")
	}

	// Cooldown over, node still sick: the single probe pays one timeout and re-opens.
	time.Sleep(450 * time.Millisecond)
	if d, err := timed(set); errors.Is(err, ErrCircuitOpen) || err == nil || d < opTimeout/2 {
		t.Fatalf("probe should really try the node and time out: %v after %v", err, d)
	}
	if _, err := timed(set); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("a failed probe must re-open the breaker, got %v", err)
	}

	// The node recovers; after the next cooldown the probe succeeds and normal service resumes.
	f.sick.Store(false)
	time.Sleep(450 * time.Millisecond)
	if err := set(); err != nil {
		t.Fatalf("probe against the recovered node: %v", err)
	}
	for i := 0; i < 5; i++ {
		if d, err := timed(set); err != nil || d > 100*time.Millisecond {
			t.Fatalf("after recovery call %d: %v in %v", i, err, d)
		}
	}
}

func TestBreakerDisabledKeepsPayingTimeouts(t *testing.T) {
	const opTimeout = 100 * time.Millisecond
	f := newFlakyNode(t)
	f.sick.Store(true)
	c := NewStatic(f.info(), Options{OpTimeout: opTimeout, BreakerThreshold: -1})
	defer c.Close()
	for i := 0; i < 5; i++ {
		d, err := timed(func() error { return c.Set("k", []byte("v"), 0) })
		if err == nil || errors.Is(err, ErrCircuitOpen) || d < opTimeout/2 {
			t.Fatalf("call %d: with the breaker disabled every call must wait out the timeout, got %v after %v", i, err, d)
		}
	}
}

func TestBreakerIgnoresServerErrors(t *testing.T) {
	// A node that replies "-ERR" is alive: error replies must never trip the breaker.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
					c.Write([]byte("-ERR nope\r\n"))
				}
			}(c)
		}
	}()
	c := NewStatic([]cluster.NodeInfo{{ID: "n", Addr: ln.Addr().String()}}, Options{BreakerThreshold: 1})
	defer c.Close()
	for i := 0; i < 5; i++ {
		err := c.Set("k", []byte("v"), 0)
		var se *ServerError
		if !errors.As(err, &se) {
			t.Fatalf("call %d: want ServerError, got %v", i, err)
		}
	}
}
