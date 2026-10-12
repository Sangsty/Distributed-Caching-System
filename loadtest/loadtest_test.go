package loadtest

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
)

func TestKeyGenDeterministicSkewedAndInRange(t *testing.T) {
	a, err := NewKeyGen(42, 1000, 1.1)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewKeyGen(42, 1000, 1.1)
	counts := map[int64]int{}
	const n = 200000
	for i := 0; i < n; i++ {
		x, y := a.Next(), b.Next()
		if x != y {
			t.Fatal("same seed must give the same sequence")
		}
		if x < 1 || x > 1000 {
			t.Fatalf("id %d out of range 1..1000", x)
		}
		counts[x]++
	}
	if counts[1] <= counts[2] || counts[2] <= counts[10] || counts[10] <= counts[500] {
		t.Fatalf("not skewed: c1=%d c2=%d c10=%d c500=%d", counts[1], counts[2], counts[10], counts[500])
	}
	top10 := 0
	for id := int64(1); id <= 10; id++ {
		top10 += counts[id]
	}
	if share := float64(top10) / n; share < 0.25 || share > 0.9 {
		t.Fatalf("top-10 share %.2f looks wrong for a Zipf(1.1) over 1000 keys", share)
	}
}

func TestKeyGenRejectsBadParams(t *testing.T) {
	if _, err := NewKeyGen(1, 1, 1.1); err == nil {
		t.Fatal("n=1 should be rejected")
	}
	if _, err := NewKeyGen(1, 100, 1.0); err == nil {
		t.Fatal("s<=1 should be rejected")
	}
}

func TestPercentile(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	cases := map[float64]float64{50: 5, 90: 9, 99: 10, 100: 10, 10: 1, 0.1: 1}
	for p, want := range cases {
		if got := Percentile(s, p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
	if !math.IsNaN(Percentile(nil, 50)) {
		t.Fatal("empty slice must give NaN")
	}
}

func sample(sec float64, lat time.Duration, status int, cache, node string) Sample {
	return Sample{Offset: time.Duration(sec * float64(time.Second)), Latency: lat, Status: status, Cache: cache, Node: node}
}

func TestSecondsAggregation(t *testing.T) {
	c := NewCollector()
	ms := time.Millisecond
	c.Add(sample(0.1, 2*ms, 200, "HIT", "n1"))
	c.Add(sample(0.2, 4*ms, 200, "HIT", "n2"))
	c.Add(sample(0.9, 10*ms, 200, "MISS", "n1"))
	c.Add(sample(0.95, 5*time.Second, 0, "", "")) // transport error: not in latency
	c.Add(sample(1.5, 3*ms, 500, "MISS", "n1"))   // HTTP error
	c.Add(sample(2.2, 6*ms, 200, "MISS", "n2"))
	c.Drop(2500 * ms)

	secs := c.Seconds(3)
	s0 := secs[0]
	if s0.OK != 3 || s0.Errors != 1 || s0.Hits != 2 || s0.Misses != 1 || s0.Sent() != 4 {
		t.Fatalf("second 0: %+v", s0)
	}
	if got := s0.HitRatio(); math.Abs(got-2.0/3) > 1e-9 {
		t.Fatalf("hit ratio %v", got)
	}
	if s0.P50 != 4 || s0.P99 != 10 {
		t.Fatalf("latency percentiles over OK requests only: p50=%v p99=%v", s0.P50, s0.P99)
	}
	if secs[1].OK != 0 || secs[1].Errors != 1 || !math.IsNaN(secs[1].P50) || !math.IsNaN(secs[1].HitRatio()) {
		t.Fatalf("second 1: %+v", secs[1])
	}
	if secs[2].OK != 1 || secs[2].Dropped != 1 || secs[2].Sent() != 2 {
		t.Fatalf("second 2: %+v", secs[2])
	}
}

func TestNodeCounts(t *testing.T) {
	c := NewCollector()
	c.Add(sample(0.1, time.Millisecond, 200, "HIT", "b"))
	c.Add(sample(0.2, time.Millisecond, 200, "HIT", "a"))
	c.Add(sample(0.3, time.Millisecond, 200, "HIT", "a"))
	c.Add(sample(1.1, time.Millisecond, 200, "MISS", ""))  // no node header: ignored
	c.Add(sample(1.2, time.Millisecond, 500, "MISS", "a")) // failed: ignored
	nodes, per := c.NodeCounts(2)
	if len(nodes) != 2 || nodes[0] != "a" || nodes[1] != "b" {
		t.Fatalf("nodes = %v", nodes)
	}
	if per[0]["a"] != 2 || per[0]["b"] != 1 || len(per[1]) != 0 {
		t.Fatalf("per second = %v", per)
	}
}

func TestBuildPhases(t *testing.T) {
	s := time.Second
	got := BuildPhases(10*s, 90*s, []Event{{60 * s, "restart"}, {30 * s, "crash"}}) // unsorted on purpose
	want := []Phase{
		{"before: crash", 10 * s, 30 * s},
		{"after: crash", 30 * s, 60 * s},
		{"after: restart", 60 * s, 90 * s},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("phase %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if p := BuildPhases(5*s, 60*s, nil); len(p) != 1 || p[0].From != 5*s || p[0].To != 60*s || p[0].Name != "steady state" {
		t.Fatalf("no events: %+v", p)
	}
	// An event inside the warm-up produces no empty "before" phase.
	p := BuildPhases(10*s, 60*s, []Event{{5 * s, "early"}})
	if len(p) != 1 || p[0].Name != "after: early" || p[0].From != 10*s {
		t.Fatalf("early event: %+v", p)
	}
}

func TestPhaseStatsAndMarkdown(t *testing.T) {
	c := NewCollector()
	ms := time.Millisecond
	for i := 0; i < 100; i++ {
		cache := "MISS"
		if i%4 != 0 {
			cache = "HIT" // 75% hits
		}
		c.Add(sample(10+float64(i)/100*10, time.Duration(i+1)*ms, 200, cache, "n1"))
	}
	c.Add(sample(12, time.Second, 0, "", "")) // one error inside the window
	c.Add(sample(25, ms, 200, "HIT", "n1"))   // outside the window
	st := c.Stats(Phase{"w", 10 * time.Second, 20 * time.Second})
	if st.OK != 100 || st.Errors != 1 || math.Abs(st.HitRatio-0.75) > 1e-9 {
		t.Fatalf("%+v", st)
	}
	if st.P50 != 50 || st.P99 != 99 || st.Max != 100 || math.Abs(st.RPS-10) > 1e-9 {
		t.Fatalf("latency/rps: %+v", st)
	}
	md := SummaryMarkdown([]PhaseStats{st})
	if !strings.Contains(md, "| w | 10–20 s | 10 | 75.0% | 50.0 | 95.0 | 99.0 | 100.0 | 1 |") {
		t.Fatalf("markdown row wrong:\n%s", md)
	}
}

func TestCSVWriters(t *testing.T) {
	c := NewCollector()
	c.Add(sample(0.5, 2*time.Millisecond, 200, "HIT", "n1"))
	secs := c.Seconds(2)
	var b bytes.Buffer
	if err := WriteTimeseries(&b, secs); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(b.String()), "\n")
	if len(lines) != 3 || lines[0] != "t,sent,ok,errors,dropped,hits,misses,hit_ratio,p50_ms,p95_ms,p99_ms" {
		t.Fatalf("timeseries:\n%s", b.String())
	}
	if lines[1] != "0,1,1,0,0,1,0,1.0000,2.000,2.000,2.000" {
		t.Fatalf("row 0 = %q", lines[1])
	}
	if lines[2] != "1,0,0,0,0,0,0,,,," { // empty second: blanks, not NaN
		t.Fatalf("row 1 = %q", lines[2])
	}
	b.Reset()
	nodes, per := c.NodeCounts(2)
	WriteNodes(&b, nodes, per)
	if b.String() != "t,n1\n0,1\n1,0\n" {
		t.Fatalf("nodes csv: %q", b.String())
	}
	b.Reset()
	WriteEvents(&b, []Event{{25 * time.Second, "crash node2"}})
	if b.String() != "t_seconds,label\n25.00,crash node2\n" {
		t.Fatalf("events csv: %q", b.String())
	}
}

func secsWithRatios(ratios ...float64) []Second {
	out := make([]Second, len(ratios))
	for i, r := range ratios {
		out[i].T = i
		if math.IsNaN(r) {
			continue
		}
		out[i].OK = 100
		out[i].Hits = int(math.Round(r * 100))
		out[i].Misses = 100 - out[i].Hits
	}
	return out
}

func TestComputeImpact(t *testing.T) {
	s := time.Second
	//            0     1     2     3     4     5     6     7     8     9
	secs := secsWithRatios(.96, .96, .50, .60, .92, .70, .93, .95, .96, .96)
	im := ComputeImpact(secs, Event{2 * s, "crash"}, 10*s, 0.96)
	if !im.Dipped || im.Worst != 0.50 {
		t.Fatalf("%+v", im)
	}
	// threshold 0.91: second 4 (.92) is ok but 5 (.70) is not, so the first sustained run starts at 6.
	if !im.Recovered || im.RecoverySeconds != 4 {
		t.Fatalf("recovery: %+v", im)
	}
	if !strings.Contains(im.String(), "back within 5 points after 4 s") {
		t.Fatalf("string: %s", im.String())
	}

	// No dip at all.
	flat := secsWithRatios(.96, .95, .96, .94, .95, .96)
	im = ComputeImpact(flat, Event{2 * s, "noop"}, 6*s, 0.96)
	if im.Dipped || !im.Recovered || !strings.Contains(im.String(), "no measurable") {
		t.Fatalf("flat: %+v", im)
	}

	// Never recovers.
	bad := secsWithRatios(.96, .96, .40, .40, .40, .40)
	im = ComputeImpact(bad, Event{2 * s, "crash"}, 6*s, 0.96)
	if !im.Dipped || im.Recovered || !strings.Contains(im.String(), "had not recovered") {
		t.Fatalf("bad: %+v", im)
	}

	// The window ends at the next event: a later dip is not this event's.
	im = ComputeImpact(secs, Event{2 * s, "crash"}, 4*s, 0.96)
	if im.Worst != 0.50 {
		t.Fatalf("window: %+v", im)
	}
	// Empty window / all-NaN seconds do not panic.
	nan := secsWithRatios(math.NaN(), math.NaN())
	if im := ComputeImpact(nan, Event{0, "x"}, 2*s, 0.9); !math.IsNaN(im.Worst) || !strings.Contains(im.String(), "no data") {
		t.Fatalf("nan: %+v", im)
	}
	_ = ComputeImpact(secs, Event{50 * s, "late"}, 60*s, 0.9)
}
