package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
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

func newTestStore(max int) (*Store, *fakeClock) {
	s := New(max)
	c := &fakeClock{t: time.Unix(1_000_000, 0)}
	s.now = c.Now
	return s, c
}

func TestSetGetDel(t *testing.T) {
	s, _ := newTestStore(0)
	if _, ok := s.Get("a"); ok {
		t.Fatal("empty store returned a value")
	}
	s.Set("a", []byte("1"), 0)
	if v, ok := s.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("got %q,%v", v, ok)
	}
	s.Set("a", []byte("2"), 0) // overwrite
	if v, _ := s.Get("a"); string(v) != "2" {
		t.Fatalf("overwrite failed: %q", v)
	}
	if s.Len() != 1 {
		t.Fatalf("len = %d", s.Len())
	}
	if !s.Del("a") || s.Del("a") {
		t.Fatal("Del should succeed once, then report false")
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("deleted key still readable")
	}
}

func TestLRUEvictionOrder(t *testing.T) {
	s, _ := newTestStore(3)
	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)
	s.Set("c", []byte("3"), 0)
	s.Get("a") // a is now most recent; b is the LRU
	s.Set("d", []byte("4"), 0)
	if _, ok := s.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := s.Get(k); !ok {
			t.Fatalf("%s should still be present", k)
		}
	}
	if st := s.Stats(); st.Evictions != 1 || st.Items != 3 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestOverwriteDoesNotEvict(t *testing.T) {
	s, _ := newTestStore(2)
	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)
	s.Set("a", []byte("3"), 0) // update, not insert
	if s.Len() != 2 || s.Stats().Evictions != 0 {
		t.Fatalf("overwrite must not evict: %+v", s.Stats())
	}
}

func TestCapacityOne(t *testing.T) {
	s, _ := newTestStore(1)
	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)
	if _, ok := s.Get("a"); ok {
		t.Fatal("a should be evicted")
	}
	if v, ok := s.Get("b"); !ok || string(v) != "2" {
		t.Fatal("newest key must survive")
	}
}

func TestTTLExpiry(t *testing.T) {
	s, clk := newTestStore(0)
	s.Set("k", []byte("v"), 10*time.Second)
	clk.Advance(9 * time.Second)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key expired too early")
	}
	clk.Advance(2 * time.Second)
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should have expired")
	}
	if s.Len() != 0 {
		t.Fatal("lazy expiry should have removed the entry")
	}
	if st := s.Stats(); st.Expired != 1 {
		t.Fatalf("expired counter = %d", st.Expired)
	}
}

func TestOverwriteResetsTTL(t *testing.T) {
	s, clk := newTestStore(0)
	s.Set("k", []byte("v1"), 5*time.Second)
	s.Set("k", []byte("v2"), 0) // now persistent
	clk.Advance(time.Hour)
	if v, ok := s.Get("k"); !ok || string(v) != "v2" {
		t.Fatal("overwrite without TTL must clear the old expiry")
	}
}

func TestExpireAndTTL(t *testing.T) {
	s, clk := newTestStore(0)
	if s.Expire("missing", time.Second) {
		t.Fatal("Expire on missing key must be false")
	}
	s.Set("k", []byte("v"), 0)
	if _, exists, hasTTL := s.TTL("k"); !exists || hasTTL {
		t.Fatal("persistent key: exists=true hasTTL=false expected")
	}
	if !s.Expire("k", 30*time.Second) {
		t.Fatal("Expire should succeed")
	}
	clk.Advance(10 * time.Second)
	rem, exists, hasTTL := s.TTL("k")
	if !exists || !hasTTL || rem != 20*time.Second {
		t.Fatalf("TTL = %v,%v,%v", rem, exists, hasTTL)
	}
	if !s.Expire("k", 0) { // non-positive deletes
		t.Fatal("Expire(0) should report true and delete")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("Expire(0) must delete the key")
	}
	if _, exists, _ := s.TTL("k"); exists {
		t.Fatal("TTL of a missing key")
	}
}

func TestDelExpiredKeyReportsFalse(t *testing.T) {
	s, clk := newTestStore(0)
	s.Set("k", []byte("v"), time.Second)
	clk.Advance(2 * time.Second)
	if s.Del("k") {
		t.Fatal("Del of an expired key must report false")
	}
}

func TestSweep(t *testing.T) {
	s, clk := newTestStore(0)
	for i := 0; i < 10; i++ {
		s.Set(fmt.Sprintf("short%d", i), []byte("x"), time.Second)
		s.Set(fmt.Sprintf("long%d", i), []byte("x"), time.Hour)
	}
	clk.Advance(2 * time.Second)
	if s.Len() != 20 {
		t.Fatal("nothing should be removed before a sweep")
	}
	if n := s.Sweep(0); n != 10 {
		t.Fatalf("Sweep removed %d, want 10", n)
	}
	if s.Len() != 10 {
		t.Fatalf("len = %d, want 10", s.Len())
	}
}

func TestSweepLimitIsBounded(t *testing.T) {
	s, clk := newTestStore(0)
	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("k%d", i), []byte("x"), time.Second)
	}
	clk.Advance(time.Minute)
	if n := s.Sweep(10); n != 10 {
		t.Fatalf("limited sweep removed %d, want exactly 10", n)
	}
}

func TestFlushAndStats(t *testing.T) {
	s, _ := newTestStore(0)
	s.Set("a", []byte("1"), 0)
	s.Get("a")
	s.Get("nope")
	s.Flush()
	if s.Len() != 0 {
		t.Fatal("Flush left keys behind")
	}
	if st := s.Stats(); st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("stats = %+v", st)
	}
	s.Set("a", []byte("1"), 0) // store must be usable after Flush
	if _, ok := s.Get("a"); !ok {
		t.Fatal("store broken after Flush")
	}
}

func TestJanitorSweepsInBackground(t *testing.T) {
	s := New(0) // real clock
	s.Set("k", []byte("v"), 20*time.Millisecond)
	stop := s.StartJanitor(10*time.Millisecond, 100)
	defer stop()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.Len() == 0 {
			stop()
			stop() // calling twice must be safe
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("janitor never removed the expired key")
}

// Run with -race.
func TestConcurrentAccess(t *testing.T) {
	s := New(100)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := fmt.Sprintf("k%d", (g*7+i)%300)
				switch i % 5 {
				case 0, 1:
					s.Set(k, []byte("v"), time.Duration(i%3)*time.Millisecond)
				case 2:
					s.Get(k)
				case 3:
					s.Del(k)
				default:
					s.Sweep(20)
				}
			}
		}(g)
	}
	wg.Wait()
	if s.Len() > 100 {
		t.Fatalf("store exceeded its capacity: %d", s.Len())
	}
}
