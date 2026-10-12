package flight

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waiters returns how many callers have joined the in-flight call for key.
func (g *Group) waiters(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.m[key]; ok {
		return c.dups
	}
	return 0
}

func TestConcurrentCallsShareOneExecution(t *testing.T) {
	var g Group
	var executions atomic.Int32
	release := make(chan struct{})
	const callers = 50

	type result struct {
		val       any
		err       error
		coalesced bool
	}
	results := make(chan result, callers)
	for i := 0; i < callers; i++ {
		go func() {
			v, err, co := g.Do("k", func() (any, error) {
				executions.Add(1)
				<-release
				return "value", nil
			})
			results <- result{v, err, co}
		}()
	}
	// Deterministic: wait until all other callers have joined before releasing.
	deadline := time.Now().Add(5 * time.Second)
	for g.waiters("k") < callers-1 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d callers joined", g.waiters("k"))
		}
		time.Sleep(time.Millisecond)
	}
	close(release)

	leaders := 0
	for i := 0; i < callers; i++ {
		r := <-results
		if r.err != nil || r.val != "value" {
			t.Fatalf("got %v, %v", r.val, r.err)
		}
		if !r.coalesced {
			leaders++
		}
	}
	if executions.Load() != 1 || leaders != 1 {
		t.Fatalf("executions=%d leaders=%d, want 1 and 1", executions.Load(), leaders)
	}
}

func TestSequentialCallsRunAgain(t *testing.T) {
	var g Group
	n := 0
	for i := 1; i <= 3; i++ {
		v, _, co := g.Do("k", func() (any, error) { n++; return n, nil })
		if v != i || co {
			t.Fatalf("call %d: v=%v coalesced=%v (results must not be cached)", i, v, co)
		}
	}
}

func TestDifferentKeysRunIndependently(t *testing.T) {
	var g Group
	var wg sync.WaitGroup
	var executions atomic.Int32
	gate := make(chan struct{})
	for _, k := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			g.Do(k, func() (any, error) { executions.Add(1); <-gate; return nil, nil })
		}(k)
	}
	deadline := time.Now().Add(5 * time.Second)
	for executions.Load() < 3 { // all three run at the same time
		if time.Now().After(deadline) {
			t.Fatal("different keys were serialised")
		}
		time.Sleep(time.Millisecond)
	}
	close(gate)
	wg.Wait()
}

func TestErrorIsSharedAndKeyIsReleased(t *testing.T) {
	var g Group
	boom := errors.New("boom")
	release := make(chan struct{})
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() {
			_, err, _ := g.Do("k", func() (any, error) { <-release; return nil, boom })
			errs <- err
		}()
	}
	for g.waiters("k") < 2 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	for i := 0; i < 3; i++ {
		if err := <-errs; !errors.Is(err, boom) {
			t.Fatalf("got %v", err)
		}
	}
	if v, err, _ := g.Do("k", func() (any, error) { return "ok", nil }); err != nil || v != "ok" {
		t.Fatalf("key not released after an error: %v %v", v, err)
	}
}

func TestPanicBecomesErrorAndKeyIsReleased(t *testing.T) {
	var g Group
	_, err, _ := g.Do("k", func() (any, error) { panic("kaboom") })
	if err == nil {
		t.Fatal("a panicking loader must produce an error")
	}
	if v, err, _ := g.Do("k", func() (any, error) { return "fine", nil }); err != nil || v != "fine" {
		t.Fatalf("group unusable after a panic: %v %v", v, err)
	}
}
