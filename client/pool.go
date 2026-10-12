package client

import (
	"errors"
	"net"
	"sync"
	"time"

	"cachekit/resp"
)

// conn is one pooled TCP connection with its RESP reader/writer.
type conn struct {
	nc net.Conn
	rd *resp.Reader
	wr *resp.Writer
}

// exec sends one command and reads its reply under a single deadline.
func (c *conn) exec(timeout time.Duration, args [][]byte) (resp.Value, error) {
	c.nc.SetDeadline(time.Now().Add(timeout))
	c.wr.WriteArgs(args...)
	if err := c.wr.Flush(); err != nil {
		return resp.Value{}, err
	}
	return c.rd.ReadValue()
}

// nodePool keeps idle connections to one node. Connections are checked out
// exclusively (one request in flight per connection), so no RESP framing is
// ever shared between goroutines.
type nodePool struct {
	addr        string
	dialTimeout time.Duration
	maxIdle     int
	brk         *breaker // nil = circuit breaker disabled

	mu     sync.Mutex
	idle   []*conn
	closed bool
}

func newPool(addr string, dialTimeout time.Duration, maxIdle int, brk *breaker) *nodePool {
	return &nodePool{addr: addr, dialTimeout: dialTimeout, maxIdle: maxIdle, brk: brk}
}

// get returns an idle connection (reused=true) or dials a new one. fresh=true
// skips the idle list, which a retry uses after a pooled connection failed.
func (p *nodePool) get(fresh bool) (c *conn, reused bool, err error) {
	if !fresh {
		p.mu.Lock()
		if n := len(p.idle); n > 0 {
			c = p.idle[n-1]
			p.idle = p.idle[:n-1]
			p.mu.Unlock()
			return c, true, nil
		}
		p.mu.Unlock()
	}
	nc, err := net.DialTimeout("tcp", p.addr, p.dialTimeout)
	if err != nil {
		return nil, false, err
	}
	return &conn{nc: nc, rd: resp.NewReader(nc), wr: resp.NewWriter(nc)}, false, nil
}

// put returns a healthy connection to the pool (or closes it if the pool is
// full or closed).
func (p *nodePool) put(c *conn) {
	p.mu.Lock()
	if p.closed || len(p.idle) >= p.maxIdle {
		p.mu.Unlock()
		c.nc.Close()
		return
	}
	p.idle = append(p.idle, c)
	p.mu.Unlock()
}

// drainIdle closes every idle connection. After one pooled connection fails,
// its siblings are probably dead too (the node restarted), so retrying with
// another idle one would just fail again.
func (p *nodePool) drainIdle() {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, c := range idle {
		c.nc.Close()
	}
}

func (p *nodePool) close() {
	p.mu.Lock()
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, c := range idle {
		c.nc.Close()
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// roundTrip performs one request/response. If a *reused* connection fails
// quickly (stale after a node restart or an idle-timeout close), it retries
// once on a fresh connection. A timeout is not retried: the node is slow or
// gone, and retrying would only double the caller's wait. Every command the
// client sends (GET, SET, DEL) is idempotent, so a retry is always safe.
func (p *nodePool) roundTrip(timeout time.Duration, args [][]byte) (resp.Value, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		c, reused, err := p.get(attempt > 0)
		if err != nil {
			return resp.Value{}, err // can't even connect: node is down
		}
		v, err := c.exec(timeout, args)
		if err == nil {
			p.put(c)
			return v, nil
		}
		c.nc.Close() // never reuse a connection whose framing may be out of sync
		lastErr = err
		if !reused || isTimeout(err) {
			break
		}
		p.drainIdle()
	}
	return resp.Value{}, lastErr
}
