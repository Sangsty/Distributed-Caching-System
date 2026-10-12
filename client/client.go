// Package client is the application-facing side of the cache. It keeps a local
// copy of the consistent-hash ring, so routing a key to its node is a local,
// in-memory lookup (no extra network hop), and it keeps that copy current by
// long-polling the coordinator.
//
// Failure model: if the owning node is unreachable, Get/Set/Del return a
// *NodeError. The caller (typically cache-aside code) should treat that like a
// cache miss and fall back to the database. The client never "re-routes" a key
// to another node on its own; once the coordinator declares the node dead the
// ring changes and the key is owned by the next node. Until then a per-node
// circuit breaker (see breaker.go) stops the client from waiting out a full
// timeout on every request to a node that has just failed.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"cachekit/cluster"
	"cachekit/resp"
	"cachekit/ring"
)

// ErrNoNodes is returned when the ring is empty.
var ErrNoNodes = errors.New("client: no cache nodes available")

// NodeError reports a failure talking to one specific node.
type NodeError struct {
	NodeID string
	Err    error
}

func (e *NodeError) Error() string { return fmt.Sprintf("cache node %s: %v", e.NodeID, e.Err) }
func (e *NodeError) Unwrap() error { return e.Err }

// ServerError is an error reply from a node (e.g. "ERR unknown command").
type ServerError struct{ Msg string }

func (e *ServerError) Error() string { return "server error: " + e.Msg }

// Options configures a Client. Zero values select the defaults noted below.
type Options struct {
	CoordinatorURL string        // e.g. "http://localhost:8080" (New only)
	DialTimeout    time.Duration // default 1s
	OpTimeout      time.Duration // per request, default 1s
	MaxIdlePerNode int           // idle connections kept per node, default 8
	PollWait       time.Duration // coordinator long-poll length, default 10s
	VNodes         int           // NewStatic only; default ring.DefaultVNodes

	// Per-node circuit breaker: after BreakerThreshold consecutive transport
	// failures the node is skipped (calls fail instantly with ErrCircuitOpen) for
	// BreakerCooldown, then one probe call decides whether it is back. This keeps
	// a dead-but-not-yet-evicted node from adding a full timeout to every request.
	BreakerThreshold int           // default 3; a negative value disables the breaker
	BreakerCooldown  time.Duration // default 1s
}

func (o *Options) defaults() {
	if o.DialTimeout <= 0 {
		o.DialTimeout = time.Second
	}
	if o.OpTimeout <= 0 {
		o.OpTimeout = time.Second
	}
	if o.MaxIdlePerNode <= 0 {
		o.MaxIdlePerNode = 8
	}
	if o.PollWait <= 0 {
		o.PollWait = 10 * time.Second
	}
}

// Client routes keys to cache nodes. It is safe for concurrent use.
type Client struct {
	opts Options

	mu      sync.RWMutex
	ring    *ring.Ring
	pools   map[string]*nodePool // node ID -> pool
	epoch   string
	version uint64

	httpc  *http.Client
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New connects to the coordinator, loads the current membership, and keeps it
// up to date in the background until Close. It fails if the coordinator cannot
// be reached at start-up.
func New(opts Options) (*Client, error) {
	opts.defaults()
	if opts.CoordinatorURL == "" {
		return nil, errors.New("client: CoordinatorURL is required (use NewStatic for a fixed node list)")
	}
	c := newClient(opts)
	c.httpc = &http.Client{Timeout: opts.PollWait + 5*time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	m, err := c.fetch(ctx, nil)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("client: initial membership fetch: %w", err)
	}
	c.apply(m)

	wctx, wcancel := context.WithCancel(context.Background())
	c.cancel = wcancel
	c.wg.Add(1)
	go c.watch(wctx)
	return c, nil
}

// NewStatic builds a client over a fixed node list, with no coordinator. Useful
// for tests and for running without the control plane.
func NewStatic(nodes []cluster.NodeInfo, opts Options) *Client {
	opts.defaults()
	c := newClient(opts)
	c.apply(cluster.Membership{Epoch: "static", Version: 1, VNodes: opts.VNodes, Nodes: nodes})
	return c
}

func newClient(opts Options) *Client {
	return &Client{opts: opts, ring: ring.New(opts.VNodes), pools: map[string]*nodePool{}}
}

// Close stops the background watcher and closes all connections.
func (c *Client) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.mu.Lock()
	pools := c.pools
	c.pools = map[string]*nodePool{}
	c.mu.Unlock()
	for _, p := range pools {
		p.close()
	}
}

// ------------------------------------------------------------- membership

// apply swaps in a new membership: rebuild the ring, reuse pools for nodes
// whose address is unchanged, and close pools for nodes that left.
func (c *Client) apply(m cluster.Membership) {
	r := m.BuildRing()
	c.mu.Lock()
	next := make(map[string]*nodePool, len(m.Nodes))
	for _, n := range m.Nodes {
		if p, ok := c.pools[n.ID]; ok && p.addr == n.Addr {
			next[n.ID] = p
		} else {
			next[n.ID] = newPool(n.Addr, c.opts.DialTimeout, c.opts.MaxIdlePerNode,
				newBreaker(c.opts.BreakerThreshold, c.opts.BreakerCooldown))
		}
	}
	var stale []*nodePool
	for id, p := range c.pools {
		if next[id] != p {
			stale = append(stale, p)
		}
	}
	c.ring, c.pools, c.epoch, c.version = r, next, m.Epoch, m.Version
	c.mu.Unlock()
	for _, p := range stale { // requests already holding a stale pool finish fine; put() closes their conns
		p.close()
	}
}

// fetch asks the coordinator for the membership. With cur != nil it long-polls
// until the membership differs from cur (or PollWait elapses).
func (c *Client) fetch(ctx context.Context, cur *cluster.Membership) (cluster.Membership, error) {
	u := strings.TrimRight(c.opts.CoordinatorURL, "/") + "/ring"
	if cur != nil {
		q := url.Values{}
		q.Set("since", strconv.FormatUint(cur.Version, 10))
		q.Set("epoch", cur.Epoch)
		q.Set("wait", c.opts.PollWait.String())
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return cluster.Membership{}, err
	}
	res, err := c.httpc.Do(req)
	if err != nil {
		return cluster.Membership{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return cluster.Membership{}, fmt.Errorf("coordinator returned %s", res.Status)
	}
	var m cluster.Membership
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		return cluster.Membership{}, err
	}
	return m, nil
}

// watch long-polls the coordinator and applies every change. When the
// coordinator is unreachable the client keeps using its last ring and retries
// with exponential backoff.
func (c *Client) watch(ctx context.Context) {
	defer c.wg.Done()
	const minBackoff, maxBackoff = 200 * time.Millisecond, 5 * time.Second
	backoff := minBackoff
	for ctx.Err() == nil {
		c.mu.RLock()
		cur := cluster.Membership{Epoch: c.epoch, Version: c.version}
		c.mu.RUnlock()

		m, err := c.fetch(ctx, &cur)
		if err != nil {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		backoff = minBackoff
		if m.Epoch != cur.Epoch || m.Version != cur.Version {
			c.apply(m)
		}
	}
}

// ---------------------------------------------------------------- queries

// Nodes returns the IDs of the nodes currently on the ring, sorted.
func (c *Client) Nodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.GetNodes()
}

// Owner returns the ID of the node responsible for key.
func (c *Client) Owner(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.GetNode(key)
}

// Version returns the membership version the client is currently using.
func (c *Client) Version() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

// ------------------------------------------------------------- operations

func (c *Client) do(key string, args ...[]byte) (resp.Value, error) {
	c.mu.RLock()
	id, ok := c.ring.GetNode(key)
	var p *nodePool
	if ok {
		p = c.pools[id]
	}
	c.mu.RUnlock()
	if !ok || p == nil {
		return resp.Value{}, ErrNoNodes
	}
	if !p.brk.allow() {
		return resp.Value{}, &NodeError{NodeID: id, Err: ErrCircuitOpen}
	}
	v, err := p.roundTrip(c.opts.OpTimeout, args)
	if err != nil {
		p.brk.failure()
		return resp.Value{}, &NodeError{NodeID: id, Err: err}
	}
	p.brk.success() // any reply, even an error reply, proves the node is alive
	if v.Type == resp.Error {
		return resp.Value{}, &ServerError{Msg: string(v.Str)}
	}
	return v, nil
}

// Get returns the value for key. found is false on a cache miss.
func (c *Client) Get(key string) (val []byte, found bool, err error) {
	v, err := c.do(key, []byte("GET"), []byte(key))
	if err != nil {
		return nil, false, err
	}
	if v.Type != resp.Bulk {
		return nil, false, &ServerError{Msg: "unexpected reply to GET"}
	}
	if v.Null {
		return nil, false, nil
	}
	return v.Str, true, nil
}

// Set stores val under key. ttl <= 0 means no expiry; positive TTLs have
// millisecond resolution (rounded up to at least 1ms).
func (c *Client) Set(key string, val []byte, ttl time.Duration) error {
	args := [][]byte{[]byte("SET"), []byte(key), val}
	if ttl > 0 {
		ms := ttl.Milliseconds()
		if ms < 1 {
			ms = 1
		}
		args = append(args, []byte("PX"), []byte(strconv.FormatInt(ms, 10)))
	}
	_, err := c.do(key, args...)
	return err
}

// Del removes key and reports whether it existed.
func (c *Client) Del(key string) (existed bool, err error) {
	v, err := c.do(key, []byte("DEL"), []byte(key))
	if err != nil {
		return false, err
	}
	return v.Type == resp.Integer && v.Int > 0, nil
}
