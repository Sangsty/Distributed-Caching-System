// Package coordinator is the control plane of the cache cluster. Nodes send it
// heartbeats; it declares silent nodes dead and publishes a versioned list of
// live nodes that clients use to build their local ring. It never sits on the
// data path: key lookups happen in the client, so no request pays an extra hop.
//
// Known limitation (by design, for this project): the coordinator is a single
// point of failure. Existing clients keep working from their last ring if it
// goes down, but membership stops updating. A production system would run it on
// Raft/etcd or replace it with gossip.
package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"cachekit/cluster"
)

const (
	maxEvents  = 500           // event log is a bounded ring buffer
	pruneAfter = 1 * time.Hour // forget nodes that have been dead this long
	maxIDLen   = 128
)

type nodeState struct {
	info     cluster.NodeInfo
	lastSeen time.Time
	alive    bool
}

// Coordinator tracks cluster membership. It is safe for concurrent use.
type Coordinator struct {
	vnodes  int
	timeout time.Duration
	epoch   string

	mu      sync.Mutex
	nodes   map[string]*nodeState
	version uint64
	seq     uint64
	events  []cluster.Event
	changed chan struct{} // closed and replaced on every membership change
	now     func() time.Time
}

// New creates a coordinator. vnodes is the per-node virtual node count that
// all clients will use; timeout is how long a node may stay silent before it is
// declared dead (use at least 3x the heartbeat interval).
func New(vnodes int, timeout time.Duration) *Coordinator {
	var b [8]byte
	rand.Read(b[:])
	return &Coordinator{
		vnodes:  vnodes,
		timeout: timeout,
		epoch:   hex.EncodeToString(b[:]),
		nodes:   make(map[string]*nodeState),
		changed: make(chan struct{}),
		now:     time.Now,
	}
}

// ErrInvalidNode is returned for malformed heartbeat input.
var ErrInvalidNode = errors.New("coordinator: invalid node")

func validate(info cluster.NodeInfo) error {
	if info.ID == "" || len(info.ID) > maxIDLen || strings.ContainsAny(info.ID, " \t\r\n") {
		return fmt.Errorf("%w: id must be 1-%d characters without whitespace", ErrInvalidNode, maxIDLen)
	}
	host, port, err := net.SplitHostPort(info.Addr)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("%w: addr %q must look like host:port", ErrInvalidNode, info.Addr)
	}
	return nil
}

// bumpLocked records a membership change: new version, log entry, and a wake-up
// for everyone long-polling. Callers hold c.mu.
func (c *Coordinator) bumpLocked(typ, nodeID string) {
	c.version++
	c.seq++
	c.events = append(c.events, cluster.Event{Seq: c.seq, Time: c.now(), Type: typ, NodeID: nodeID, Version: c.version})
	if len(c.events) > maxEvents {
		c.events = append([]cluster.Event(nil), c.events[len(c.events)-maxEvents:]...)
	}
	close(c.changed)
	c.changed = make(chan struct{})
}

// Heartbeat registers a node or refreshes its liveness. It is idempotent, so
// one call serves as join, keep-alive and rejoin-after-death: a node never
// needs to know whether the coordinator restarted or marked it dead.
func (c *Coordinator) Heartbeat(info cluster.NodeInfo) error {
	if err := validate(info); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	st, ok := c.nodes[info.ID]
	switch {
	case !ok:
		c.nodes[info.ID] = &nodeState{info: info, lastSeen: now, alive: true}
		c.bumpLocked(cluster.EventJoin, info.ID)
	case !st.alive:
		st.info, st.lastSeen, st.alive = info, now, true
		c.bumpLocked(cluster.EventJoin, info.ID)
	case st.info.Addr != info.Addr:
		st.info, st.lastSeen = info, now
		c.bumpLocked(cluster.EventUpdate, info.ID) // same ring position, new address
	default:
		st.lastSeen = now // the common case: no version change
	}
	return nil
}

// Deregister removes a node immediately (graceful shutdown). It reports whether
// the node was known.
func (c *Coordinator) Deregister(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.nodes[id]
	if !ok {
		return false
	}
	delete(c.nodes, id)
	if st.alive { // a dead node was already out of the ring
		c.bumpLocked(cluster.EventLeave, id)
	}
	return true
}

// CheckFailures marks nodes whose last heartbeat is older than the timeout as
// dead, forgets nodes dead for a long time, and returns the IDs newly marked dead.
func (c *Coordinator) CheckFailures() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	var dead []string
	for id, st := range c.nodes {
		age := now.Sub(st.lastSeen)
		switch {
		case st.alive && age > c.timeout:
			st.alive = false
			dead = append(dead, id)
		case !st.alive && age > pruneAfter:
			delete(c.nodes, id)
		}
	}
	sort.Strings(dead) // map order is random; keep events and results deterministic
	for _, id := range dead {
		c.bumpLocked(cluster.EventDead, id)
	}
	return dead
}

// Run calls CheckFailures every interval until ctx is cancelled.
func (c *Coordinator) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.CheckFailures()
		case <-ctx.Done():
			return
		}
	}
}

func (c *Coordinator) membershipLocked() cluster.Membership {
	m := cluster.Membership{Epoch: c.epoch, Version: c.version, VNodes: c.vnodes, Nodes: []cluster.NodeInfo{}}
	for _, st := range c.nodes {
		if st.alive {
			m.Nodes = append(m.Nodes, st.info)
		}
	}
	sort.Slice(m.Nodes, func(i, j int) bool { return m.Nodes[i].ID < m.Nodes[j].ID })
	return m
}

// Membership returns the current list of live nodes.
func (c *Coordinator) Membership() cluster.Membership {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.membershipLocked()
}

// WaitForChange long-polls: it returns the current membership as soon as it
// differs from (epoch, version), or when ctx ends, whichever is first. A
// client holding a snapshot from a previous coordinator run (different epoch)
// is answered immediately.
func (c *Coordinator) WaitForChange(ctx context.Context, epoch string, version uint64) cluster.Membership {
	for {
		c.mu.Lock()
		if c.epoch != epoch || c.version != version {
			m := c.membershipLocked()
			c.mu.Unlock()
			return m
		}
		ch := c.changed
		c.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return c.Membership()
		}
	}
}

// Status returns every known node (alive or dead) with the ring share each
// live node owns.
func (c *Coordinator) Status() cluster.Status {
	c.mu.Lock()
	now := c.now()
	m := c.membershipLocked()
	st := cluster.Status{Epoch: c.epoch, Version: c.version, VNodes: c.vnodes, Nodes: make([]cluster.NodeStatus, 0, len(c.nodes))}
	for _, ns := range c.nodes {
		st.Nodes = append(st.Nodes, cluster.NodeStatus{
			ID: ns.info.ID, Addr: ns.info.Addr, Alive: ns.alive,
			LastSeenMs: now.Sub(ns.lastSeen).Milliseconds(),
		})
	}
	c.mu.Unlock()

	own := m.BuildRing().Ownership() // computed outside the lock
	sort.Slice(st.Nodes, func(i, j int) bool { return st.Nodes[i].ID < st.Nodes[j].ID })
	for i := range st.Nodes {
		if st.Nodes[i].Alive {
			st.Nodes[i].Ownership = own[st.Nodes[i].ID]
		}
	}
	return st
}

// Events returns log entries with Seq > since, oldest first.
func (c *Coordinator) Events(since uint64) []cluster.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := []cluster.Event{}
	for _, e := range c.events {
		if e.Seq > since {
			out = append(out, e)
		}
	}
	return out
}
