// Package cluster holds the types shared by the coordinator (which publishes
// cluster membership) and the client library (which consumes it). Both sides
// build their ring through Membership.BuildRing, so they always agree on which
// node owns which key.
package cluster

import (
	"time"

	"cachekit/ring"
)

// NodeInfo identifies one cache node. ID is the stable name hashed onto the
// ring; Addr ("host:port") is where clients dial it. Keeping them separate
// means a node that restarts on a new port keeps its ring positions.
type NodeInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Membership is a versioned snapshot of the live nodes.
type Membership struct {
	// Epoch identifies one coordinator run. Version restarts from zero when
	// the coordinator restarts, so clients compare (Epoch, Version) pairs.
	Epoch   string     `json:"epoch"`
	Version uint64     `json:"version"`
	VNodes  int        `json:"vnodes"`
	Nodes   []NodeInfo `json:"nodes"` // alive nodes only, sorted by ID
}

// BuildRing constructs the ring this membership describes.
func (m Membership) BuildRing() *ring.Ring {
	r := ring.New(m.VNodes)
	for _, n := range m.Nodes {
		r.AddNode(n.ID)
	}
	return r
}

// Event types recorded by the coordinator.
const (
	EventJoin   = "join"   // a node appeared, or came back after being marked dead
	EventUpdate = "update" // a live node re-registered with a different address
	EventDead   = "dead"   // heartbeats stopped; node removed from the ring
	EventLeave  = "leave"  // node deregistered gracefully
)

// Event is one entry in the coordinator's membership log.
type Event struct {
	Seq     uint64    `json:"seq"`
	Time    time.Time `json:"time"`
	Type    string    `json:"type"`
	NodeID  string    `json:"node_id"`
	Version uint64    `json:"version"` // membership version after this event
}

// NodeStatus describes one known node in Status.
type NodeStatus struct {
	ID         string  `json:"id"`
	Addr       string  `json:"addr"`
	Alive      bool    `json:"alive"`
	LastSeenMs int64   `json:"last_seen_ms_ago"`
	Ownership  float64 `json:"ownership"` // fraction of the ring (0 for dead nodes)
}

// Status is the coordinator's full view, including dead nodes. The dashboard
// renders this.
type Status struct {
	Epoch   string       `json:"epoch"`
	Version uint64       `json:"version"`
	VNodes  int          `json:"vnodes"`
	Nodes   []NodeStatus `json:"nodes"`
}
