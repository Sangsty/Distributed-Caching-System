// Package flight collapses concurrent calls for the same key into one
// execution (the "single-flight" pattern, same idea as
// golang.org/x/sync/singleflight, written here to keep the project free of
// external dependencies).
//
// Why a cache needs it: when a popular key is cold or just expired, hundreds of
// requests miss at once. Without coalescing every one of them queries the
// database for the same row (a cache stampede). With it, one request does the
// query and the rest wait for its result.
package flight

import (
	"fmt"
	"sync"
)

type call struct {
	wg   sync.WaitGroup
	val  any
	err  error
	dups int // callers that joined while the call was running; guarded by Group.mu
}

// Group coalesces calls by key. The zero value is ready to use.
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

// Do runs fn once per key at a time. If a call for key is already running, Do
// waits for it and returns its result with coalesced=true; otherwise it runs fn
// itself (coalesced=false). Results are not cached: once the call finishes, the
// next Do for the key runs fn again.
//
// If fn panics, every caller gets an error instead of the panic, and the key is
// released so later calls can retry.
func (g *Group) Do(key string, fn func() (any, error)) (val any, err error, coalesced bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	g.run(key, c, fn)
	return c.val, c.err, false
}

func (g *Group) run(key string, c *call, fn func() (any, error)) {
	defer func() {
		if r := recover(); r != nil {
			c.val, c.err = nil, fmt.Errorf("flight: loader panicked: %v", r)
		}
		g.mu.Lock()
		delete(g.m, key) // before Done: callers arriving after this start a fresh call
		g.mu.Unlock()
		c.wg.Done()
	}()
	c.val, c.err = fn()
}
