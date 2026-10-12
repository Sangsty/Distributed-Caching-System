package node

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cachekit/client"
	"cachekit/cluster"
)

func TestDefaultAdvertise(t *testing.T) {
	cases := map[string]string{
		"0.0.0.0:7001":   "127.0.0.1:7001",
		"[::]:7001":      "127.0.0.1:7001",
		"127.0.0.1:7002": "127.0.0.1:7002",
		"10.1.2.3:7003":  "10.1.2.3:7003",
	}
	for in, want := range cases {
		addr, err := net.ResolveTCPAddr("tcp", in)
		if err != nil {
			t.Fatal(err)
		}
		if got := defaultAdvertise(addr); got != want {
			t.Errorf("defaultAdvertise(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestStartValidation(t *testing.T) {
	if _, err := Start(Config{}); err == nil {
		t.Fatal("Start without an ID must fail")
	}
	n, err := Start(Config{ID: "a", Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop(false)
	if _, err := Start(Config{ID: "b", Listen: n.srvAddr()}); err == nil {
		t.Fatal("binding an address that is already in use must fail")
	}
}

// srvAddr is the real bound address (Advertise can differ from it).
func (n *Node) srvAddr() string { return n.addr }

func TestStandaloneNodeServesAndStopsIdempotently(t *testing.T) {
	n, err := Start(Config{ID: "solo", Listen: "127.0.0.1:0", MaxItems: 10})
	if err != nil {
		t.Fatal(err)
	}
	c := client.NewStatic([]cluster.NodeInfo{{ID: n.ID(), Addr: n.Addr()}}, client.Options{})
	defer c.Close()
	if err := c.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if n.Store().Len() != 1 {
		t.Fatal("value should be in the node's store")
	}
	n.Stop(true)
	n.Stop(false) // second call must be a harmless no-op
	if _, _, err := c.Get("k"); err == nil {
		t.Fatal("stopped node should refuse connections")
	}
}

func TestHeartbeatRetriesWhenCoordinatorIsDown(t *testing.T) {
	var up atomic.Bool
	var beats atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/heartbeat" {
			beats.Add(1)
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	n, err := Start(Config{ID: "hb", Listen: "127.0.0.1:0", CoordinatorURL: srv.URL, HeartbeatEvery: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err) // a node must start even if the coordinator is down
	}
	defer n.Stop(false)
	time.Sleep(100 * time.Millisecond)
	if beats.Load() != 0 {
		t.Fatal("no heartbeat can succeed while the coordinator is down")
	}
	up.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for beats.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatal("node never resumed heartbeating after the coordinator came back")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
