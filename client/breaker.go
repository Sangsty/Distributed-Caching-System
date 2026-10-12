package client

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is the cause inside the *NodeError returned while a node's
// circuit breaker is open: the client is deliberately not calling the node.
var ErrCircuitOpen = errors.New("circuit open: node recently failed, not trying it")

// Default circuit-breaker settings (see Options).
const (
	defaultBreakerThreshold = 3
	defaultBreakerCooldown  = time.Second
)

type breakerState int

const (
	stateClosed   breakerState = iota // normal: calls go through
	stateOpen                         // node failed: calls are refused immediately
	stateHalfOpen                     // cooldown over: exactly one probe call is allowed
)

// breaker is a per-node circuit breaker.
//
// Why it exists: when a node dies without saying goodbye, the coordinator needs
// its heartbeat timeout (seconds) to notice. Until then every request for that
// node's keys waits out a full dial/read timeout and then fails, so the
// application's latency jumps by the timeout (twice, if it also tries to write).
// After `threshold` consecutive failures the breaker opens and the client
// refuses calls to that node instantly, so those keys fall back to the database
// at once. After `cooldown` one probe call is let through: if it succeeds the
// node is back and the breaker closes; if it fails the breaker re-opens.
//
// Only transport failures (cannot connect, timeout, broken connection) count. An
// error reply from a live node (e.g. "ERR unknown command") proves it is up.
//
// A nil *breaker never trips (the breaker is disabled).
type breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time // replaceable in tests

	mu        sync.Mutex
	state     breakerState
	fails     int // consecutive failures while closed
	openUntil time.Time
}

// newBreaker returns a breaker, or nil (disabled) if threshold < 0. A zero
// threshold or cooldown selects the default.
func newBreaker(threshold int, cooldown time.Duration) *breaker {
	if threshold < 0 {
		return nil
	}
	if threshold == 0 {
		threshold = defaultBreakerThreshold
	}
	if cooldown <= 0 {
		cooldown = defaultBreakerCooldown
	}
	return &breaker{threshold: threshold, cooldown: cooldown, now: time.Now}
}

// allow reports whether a call may go to the node. When it returns true in the
// half-open state, that call is the probe and its outcome must be reported.
func (b *breaker) allow() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateClosed:
		return true
	case stateOpen:
		if b.now().Before(b.openUntil) {
			return false
		}
		b.state = stateHalfOpen // this caller becomes the single probe
		return true
	default: // half-open: the probe is already in flight
		return false
	}
}

// success reports that the node answered.
func (b *breaker) success() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.state, b.fails = stateClosed, 0
	b.mu.Unlock()
}

// failure reports a transport failure.
func (b *breaker) failure() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateClosed:
		if b.fails++; b.fails >= b.threshold {
			b.state, b.openUntil = stateOpen, b.now().Add(b.cooldown)
		}
	case stateHalfOpen: // the probe failed: back to open for another cooldown
		b.state, b.openUntil = stateOpen, b.now().Add(b.cooldown)
	case stateOpen:
		// A late failure from a call admitted before the breaker opened: nothing to do.
	}
}

// isOpen reports whether the breaker is currently refusing calls (for tests and stats).
func (b *breaker) isOpen() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state != stateClosed
}
