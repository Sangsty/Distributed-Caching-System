package ring

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("key:%d", i)
	}
	return out
}

func TestEmptyRing(t *testing.T) {
	r := New(10)
	if _, ok := r.GetNode("x"); ok {
		t.Fatal("empty ring must report ok=false")
	}
	if r.NodeCount() != 0 || len(r.GetNodes()) != 0 || len(r.Ownership()) != 0 {
		t.Fatal("empty ring should have no nodes")
	}
}

func TestDefaultVNodes(t *testing.T) {
	if New(0).VNodes() != DefaultVNodes || New(-5).VNodes() != DefaultVNodes {
		t.Fatal("vnodes < 1 should select the default")
	}
}

func TestAddRemoveIdempotence(t *testing.T) {
	r := New(10)
	if !r.AddNode("a") {
		t.Fatal("first add should succeed")
	}
	if r.AddNode("a") {
		t.Fatal("duplicate add must return false")
	}
	if len(r.entries) != 10 {
		t.Fatalf("duplicate add changed the ring: %d entries", len(r.entries))
	}
	if r.RemoveNode("missing") {
		t.Fatal("removing an unknown node must return false")
	}
	if !r.RemoveNode("a") || r.NodeCount() != 0 || len(r.entries) != 0 {
		t.Fatal("remove should empty the ring")
	}
	if r.Has("a") {
		t.Fatal("Has after remove")
	}
}

func TestSingleNodeOwnsEverything(t *testing.T) {
	r := New(5)
	r.AddNode("only")
	for _, k := range keys(100) {
		if n, ok := r.GetNode(k); !ok || n != "only" {
			t.Fatalf("key %q -> %q,%v", k, n, ok)
		}
	}
	if o := r.Ownership()["only"]; o != 1 {
		t.Fatalf("ownership = %v, want 1", o)
	}
	// vnodes=1 and a single node: one entry, still must own everything.
	r1 := New(1)
	r1.AddNode("only")
	if o := r1.Ownership()["only"]; o != 1 {
		t.Fatalf("single entry ownership = %v, want 1", o)
	}
}

func TestDeterministicAcrossInsertionOrder(t *testing.T) {
	a, b := New(50), New(50)
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		a.AddNode(id)
	}
	for _, id := range []string{"n4", "n2", "n1", "n3"} {
		b.AddNode(id)
	}
	for _, k := range keys(5000) {
		na, _ := a.GetNode(k)
		nb, _ := b.GetNode(k)
		if na != nb {
			t.Fatalf("key %q: %s vs %s — rings must not depend on insertion order", k, na, nb)
		}
	}
}

func TestWrapAround(t *testing.T) {
	r := New(1)
	r.AddNode("a")
	r.AddNode("b")
	// Hand-place entries so the test does not depend on hash values.
	r.entries = []entry{{100, "a"}, {200, "b"}}
	cases := []struct {
		h    uint64
		want string
	}{
		{0, "a"}, {100, "a"}, {101, "b"}, {200, "b"},
		{201, "a"},            // past the last entry wraps to the first
		{math.MaxUint64, "a"}, // wrap at the very end of the space
	}
	for _, c := range cases {
		if got, _ := r.lookup(c.h); got != c.want {
			t.Errorf("lookup(%d) = %s, want %s", c.h, got, c.want)
		}
	}
}

func TestEqualHashTieBreakIsDeterministic(t *testing.T) {
	r := New(1)
	r.AddNode("a")
	r.AddNode("b")
	// Two virtual nodes at the same position: the smaller node ID wins,
	// regardless of how the entries arrived.
	r.entries = []entry{{50, "a"}, {50, "b"}, {200, "c"}}
	if got, _ := r.lookup(50); got != "a" {
		t.Fatalf("tie-break: got %s, want a", got)
	}
	if got, _ := r.lookup(10); got != "a" {
		t.Fatalf("below tie: got %s, want a", got)
	}
	if got, _ := r.lookup(51); got != "c" {
		t.Fatalf("above tie: got %s, want c", got)
	}
}

func TestAddNodeOnlyStealsKeysForItself(t *testing.T) {
	r := New(DefaultVNodes)
	for i := 0; i < 5; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}
	ks := keys(20000)
	before := make([]string, len(ks))
	for i, k := range ks {
		before[i], _ = r.GetNode(k)
	}
	r.AddNode("node-new")
	moved := 0
	for i, k := range ks {
		after, _ := r.GetNode(k)
		if after != before[i] {
			moved++
			if after != "node-new" {
				t.Fatalf("key %q moved %s -> %s; keys may only move to the new node", k, before[i], after)
			}
		}
	}
	frac := float64(moved) / float64(len(ks))
	if frac < 0.10 || frac > 0.25 { // ideal is 1/6 = 0.167
		t.Fatalf("moved fraction %.3f, expected about 0.167", frac)
	}
}

func TestRemoveNodeOnlyMovesItsOwnKeys(t *testing.T) {
	r := New(DefaultVNodes)
	for i := 0; i < 5; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}
	ks := keys(20000)
	before := make([]string, len(ks))
	for i, k := range ks {
		before[i], _ = r.GetNode(k)
	}
	r.RemoveNode("node-2")
	for i, k := range ks {
		after, _ := r.GetNode(k)
		if before[i] == "node-2" {
			if after == "node-2" {
				t.Fatalf("key %q still on removed node", k)
			}
		} else if after != before[i] {
			t.Fatalf("key %q on a surviving node moved %s -> %s", k, before[i], after)
		}
	}
}

func TestBalance(t *testing.T) {
	r := New(DefaultVNodes)
	const nodes = 10
	for i := 0; i < nodes; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}
	counts := map[string]int{}
	const total = 100000
	for _, k := range keys(total) {
		n, _ := r.GetNode(k)
		counts[n]++
	}
	mean := float64(total) / nodes
	for id, c := range counts {
		if ratio := float64(c) / mean; ratio < 0.7 || ratio > 1.3 {
			t.Errorf("%s holds %d keys (%.2fx the mean) — too uneven for %d vnodes", id, c, ratio, DefaultVNodes)
		}
	}
	if len(counts) != nodes {
		t.Fatalf("only %d of %d nodes received keys", len(counts), nodes)
	}
}

func TestOwnershipSumsToOneAndMatchesKeys(t *testing.T) {
	r := New(DefaultVNodes)
	for i := 0; i < 4; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}
	own := r.Ownership()
	sum := 0.0
	for _, f := range own {
		sum += f
	}
	if math.Abs(sum-1) > 1e-9 {
		t.Fatalf("ownership sums to %v", sum)
	}
	// Ownership (arc length) should predict the share of real keys.
	counts := map[string]int{}
	const total = 100000
	for _, k := range keys(total) {
		n, _ := r.GetNode(k)
		counts[n]++
	}
	for id, f := range own {
		got := float64(counts[id]) / total
		if math.Abs(got-f) > 0.02 {
			t.Errorf("%s: ownership %.3f but holds %.3f of keys", id, f, got)
		}
	}
}

func TestOwnershipWithTiedEntries(t *testing.T) {
	r := New(1)
	r.AddNode("a")
	r.AddNode("b")
	r.entries = []entry{{50, "a"}, {50, "b"}, {200, "b"}}
	own := r.Ownership()
	// b's tied entry has a zero-length arc; the sum must still be 1.
	if math.Abs(own["a"]+own["b"]-1) > 1e-9 {
		t.Fatalf("ownership sums to %v", own["a"]+own["b"])
	}
}

func TestGetNodesSorted(t *testing.T) {
	r := New(3)
	for _, id := range []string{"c", "a", "b"} {
		r.AddNode(id)
	}
	got := r.GetNodes()
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("GetNodes = %v", got)
	}
}

// Run with -race: readers and writers hammer the ring concurrently.
func TestConcurrentUse(t *testing.T) {
	r := New(20)
	r.AddNode("base")
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				r.GetNode(fmt.Sprintf("k%d-%d", g, i))
				r.Ownership()
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			id := fmt.Sprintf("n%d", i)
			r.AddNode(id)
			r.RemoveNode(id)
		}
	}()
	wg.Wait()
	if r.NodeCount() != 1 {
		t.Fatalf("expected only base node left, got %v", r.GetNodes())
	}
}

func BenchmarkGetNode(b *testing.B) {
	r := New(DefaultVNodes)
	for i := 0; i < 10; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}
	ks := keys(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetNode(ks[i&1023])
	}
}
