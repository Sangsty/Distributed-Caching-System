// Package loadtest holds the pure logic behind cmd/loadgen: a Zipfian key
// generator, a collector that turns raw request samples into per-second time
// series and per-phase statistics, and the CSV / markdown writers. It does no
// network I/O, so it is unit-tested directly.
package loadtest

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---- key generation ---------------------------------------------------------

// KeyGen draws product ids 1..N with a Zipfian skew: id 1 is the hottest, and
// the popularity of id k falls off roughly as 1/k^s. Real cache traffic looks
// like this (a few very hot keys, a long cold tail), which is what makes a cache
// worth having and what makes a node crash interesting.
type KeyGen struct {
	z *rand.Zipf
	n int
}

// NewKeyGen creates a generator over ids 1..n with exponent s (> 1; larger is
// more skewed). The same seed always produces the same sequence.
func NewKeyGen(seed int64, n int, s float64) (*KeyGen, error) {
	if n < 2 {
		return nil, fmt.Errorf("loadtest: need at least 2 keys, got %d", n)
	}
	if s <= 1 {
		return nil, fmt.Errorf("loadtest: zipf exponent must be > 1, got %v", s)
	}
	z := rand.NewZipf(rand.New(rand.NewSource(seed)), s, 1, uint64(n-1))
	return &KeyGen{z: z, n: n}, nil
}

// Next returns the next id, in 1..n. It is not safe for concurrent use.
func (g *KeyGen) Next() int64 { return int64(g.z.Uint64()) + 1 }

// ---- samples and collection -------------------------------------------------

// Sample is one completed request.
type Sample struct {
	Offset  time.Duration // when it was SCHEDULED to be sent, relative to the run start
	Latency time.Duration // completion time minus the scheduled time (includes any queueing)
	Status  int           // HTTP status; 0 = transport error or timeout
	Cache   string        // X-Cache header: "HIT", "MISS" or ""
	Node    string        // X-Cache-Node header: the cache node that owns the key
}

// OK reports whether the request succeeded.
func (s Sample) OK() bool { return s.Status == 200 }

// Collector accumulates samples from many goroutines.
type Collector struct {
	mu      sync.Mutex
	samples []Sample
	dropped []time.Duration
}

// NewCollector returns an empty collector.
func NewCollector() *Collector { return &Collector{} }

// Add records a completed request.
func (c *Collector) Add(s Sample) {
	c.mu.Lock()
	c.samples = append(c.samples, s)
	c.mu.Unlock()
}

// Drop records a request that could not even be started because the load
// generator itself was saturated (so a tool limit is never mistaken for a
// server result).
func (c *Collector) Drop(offset time.Duration) {
	c.mu.Lock()
	c.dropped = append(c.dropped, offset)
	c.mu.Unlock()
}

// Percentile returns the p-th percentile (0 < p <= 100) of an ascending slice
// using the nearest-rank method, or NaN for an empty slice.
func Percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Second is one second of the time series. Latency percentiles are over
// successful requests only (a timeout is counted in Errors, not averaged in).
type Second struct {
	T                                 int
	OK, Errors, Dropped, Hits, Misses int
	P50, P95, P99                     float64 // milliseconds; NaN if no successful request
}

// Sent is the number of requests scheduled in this second.
func (s Second) Sent() int { return s.OK + s.Errors + s.Dropped }

// HitRatio is hits / (hits + misses), or NaN if nothing was served.
func (s Second) HitRatio() float64 {
	if s.Hits+s.Misses == 0 {
		return math.NaN()
	}
	return float64(s.Hits) / float64(s.Hits+s.Misses)
}

// Seconds buckets the samples by scheduled second, for seconds 0..total-1.
func (c *Collector) Seconds(total int) []Second {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Second, total)
	lat := make([][]float64, total)
	for i := range out {
		out[i].T = i
	}
	for _, s := range c.samples {
		i := int(s.Offset / time.Second)
		if i < 0 || i >= total {
			continue
		}
		if !s.OK() {
			out[i].Errors++
			continue
		}
		out[i].OK++
		switch s.Cache {
		case "HIT":
			out[i].Hits++
		case "MISS":
			out[i].Misses++
		}
		lat[i] = append(lat[i], ms(s.Latency))
	}
	for _, d := range c.dropped {
		if i := int(d / time.Second); i >= 0 && i < total {
			out[i].Dropped++
		}
	}
	for i := range out {
		sort.Float64s(lat[i])
		out[i].P50, out[i].P95, out[i].P99 = Percentile(lat[i], 50), Percentile(lat[i], 95), Percentile(lat[i], 99)
	}
	return out
}

// NodeCounts returns, per second, how many successful requests were for keys
// owned by each cache node (from the X-Cache-Node header), plus the sorted node
// names. This shows traffic re-homing when a node dies.
func (c *Collector) NodeCounts(total int) (nodes []string, perSecond []map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	perSecond = make([]map[string]int, total)
	for i := range perSecond {
		perSecond[i] = map[string]int{}
	}
	seen := map[string]bool{}
	for _, s := range c.samples {
		i := int(s.Offset / time.Second)
		if !s.OK() || s.Node == "" || i < 0 || i >= total {
			continue
		}
		perSecond[i][s.Node]++
		seen[s.Node] = true
	}
	for n := range seen {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	return nodes, perSecond
}

// ---- phases -----------------------------------------------------------------

// Event is something that happened during the run (a node was killed, ...).
type Event struct {
	At    time.Duration
	Label string
}

// Phase is a window of the run reported as one row of the summary.
type Phase struct {
	Name     string
	From, To time.Duration
}

// BuildPhases splits the run into reporting windows: everything before the first
// event (after the warm-up), then one window per event up to the next event.
// Windows of zero length are dropped.
func BuildPhases(warmup, total time.Duration, events []Event) []Phase {
	evs := append([]Event(nil), events...)
	sort.Slice(evs, func(i, j int) bool { return evs[i].At < evs[j].At })
	var out []Phase
	add := func(name string, from, to time.Duration) {
		if from < warmup {
			from = warmup
		}
		if to > total {
			to = total
		}
		if to > from {
			out = append(out, Phase{name, from, to})
		}
	}
	if len(evs) == 0 {
		add("steady state", 0, total)
		return out
	}
	add("before: "+evs[0].Label, 0, evs[0].At)
	for i, e := range evs {
		end := total
		if i+1 < len(evs) {
			end = evs[i+1].At
		}
		add("after: "+e.Label, e.At, end)
	}
	return out
}

// PhaseStats are the statistics of one Phase.
type PhaseStats struct {
	Phase
	OK, Errors, Dropped int
	HitRatio            float64
	P50, P95, P99, Max  float64 // ms, successful requests only
	RPS                 float64 // successful requests per second
}

// Stats computes the statistics of the samples scheduled inside p.
func (c *Collector) Stats(p Phase) PhaseStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := PhaseStats{Phase: p}
	var lat []float64
	var hits, misses int
	for _, s := range c.samples {
		if s.Offset < p.From || s.Offset >= p.To {
			continue
		}
		if !s.OK() {
			st.Errors++
			continue
		}
		st.OK++
		switch s.Cache {
		case "HIT":
			hits++
		case "MISS":
			misses++
		}
		lat = append(lat, ms(s.Latency))
	}
	for _, d := range c.dropped {
		if d >= p.From && d < p.To {
			st.Dropped++
		}
	}
	sort.Float64s(lat)
	st.P50, st.P95, st.P99 = Percentile(lat, 50), Percentile(lat, 95), Percentile(lat, 99)
	st.Max = Percentile(lat, 100)
	st.HitRatio = math.NaN()
	if hits+misses > 0 {
		st.HitRatio = float64(hits) / float64(hits+misses)
	}
	if d := (p.To - p.From).Seconds(); d > 0 {
		st.RPS = float64(st.OK) / d
	}
	return st
}

// ---- output -----------------------------------------------------------------

func fnum(v float64, prec int) string {
	if math.IsNaN(v) {
		return ""
	}
	return strconv.FormatFloat(v, 'f', prec, 64)
}

// WriteTimeseries writes the per-second CSV used by docs/plot_failure.py.
func WriteTimeseries(w io.Writer, secs []Second) error {
	cw := csv.NewWriter(w)
	cw.Write([]string{"t", "sent", "ok", "errors", "dropped", "hits", "misses", "hit_ratio", "p50_ms", "p95_ms", "p99_ms"})
	for _, s := range secs {
		cw.Write([]string{
			strconv.Itoa(s.T), strconv.Itoa(s.Sent()), strconv.Itoa(s.OK), strconv.Itoa(s.Errors), strconv.Itoa(s.Dropped),
			strconv.Itoa(s.Hits), strconv.Itoa(s.Misses),
			fnum(s.HitRatio(), 4), fnum(s.P50, 3), fnum(s.P95, 3), fnum(s.P99, 3),
		})
	}
	cw.Flush()
	return cw.Error()
}

// WriteNodes writes requests-per-second by owning cache node (wide format).
func WriteNodes(w io.Writer, nodes []string, perSecond []map[string]int) error {
	cw := csv.NewWriter(w)
	cw.Write(append([]string{"t"}, nodes...))
	for t, m := range perSecond {
		row := []string{strconv.Itoa(t)}
		for _, n := range nodes {
			row = append(row, strconv.Itoa(m[n]))
		}
		cw.Write(row)
	}
	cw.Flush()
	return cw.Error()
}

// WriteEvents writes the event markers (seconds since the run start).
func WriteEvents(w io.Writer, events []Event) error {
	cw := csv.NewWriter(w)
	cw.Write([]string{"t_seconds", "label"})
	for _, e := range events {
		cw.Write([]string{strconv.FormatFloat(e.At.Seconds(), 'f', 2, 64), e.Label})
	}
	cw.Flush()
	return cw.Error()
}

func pct(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return fmt.Sprintf("%.1f%%", v*100)
}

func msStr(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return fmt.Sprintf("%.1f", v)
}

// SummaryMarkdown renders the per-phase statistics as a markdown table that can
// be pasted straight into the README.
func SummaryMarkdown(stats []PhaseStats) string {
	var b strings.Builder
	b.WriteString("| Phase | Window | Requests/s | Hit ratio | p50 ms | p95 ms | p99 ms | Max ms | Errors |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, s := range stats {
		fmt.Fprintf(&b, "| %s | %.0f–%.0f s | %.0f | %s | %s | %s | %s | %s | %d |\n",
			s.Name, s.From.Seconds(), s.To.Seconds(), s.RPS, pct(s.HitRatio),
			msStr(s.P50), msStr(s.P95), msStr(s.P99), msStr(s.Max), s.Errors+s.Dropped)
	}
	return b.String()
}

// ---- event impact -----------------------------------------------------------

const (
	// ImpactTolerance is how close (in absolute hit ratio) to the baseline counts as "recovered".
	ImpactTolerance = 0.05
	// ImpactSustain is how many consecutive seconds must stay within tolerance.
	ImpactSustain = 3
)

// Impact describes how one event affected the hit ratio.
type Impact struct {
	Event
	Baseline        float64 // hit ratio before the first event
	Worst           float64 // lowest per-second hit ratio between this event and the next
	Dipped          bool    // Worst fell more than ImpactTolerance below Baseline
	Recovered       bool    // the hit ratio came back before the window ended
	RecoverySeconds int     // seconds from the event until it was back (valid if Dipped && Recovered)
}

// ComputeImpact measures the hit-ratio dip caused by e. The window runs from the
// event to end (the next event or the end of the run).
func ComputeImpact(secs []Second, e Event, end time.Duration, baseline float64) Impact {
	im := Impact{Event: e, Baseline: baseline, Worst: math.NaN()}
	from := int(e.At / time.Second)
	to := int(math.Ceil(end.Seconds()))
	if to > len(secs) {
		to = len(secs)
	}
	if from < 0 || from >= to {
		return im
	}
	worstAt := -1
	for t := from; t < to; t++ {
		r := secs[t].HitRatio()
		if math.IsNaN(r) {
			continue
		}
		if worstAt < 0 || r < im.Worst {
			im.Worst, worstAt = r, t
		}
	}
	if worstAt < 0 {
		return im
	}
	threshold := baseline - ImpactTolerance
	im.Dipped = im.Worst < threshold
	if !im.Dipped {
		im.Recovered = true
		return im
	}
	for t := worstAt; t+ImpactSustain <= to; t++ {
		ok := true
		for k := 0; k < ImpactSustain; k++ {
			if r := secs[t+k].HitRatio(); math.IsNaN(r) || r < threshold {
				ok = false
				break
			}
		}
		if ok {
			im.Recovered, im.RecoverySeconds = true, t-from
			return im
		}
	}
	return im
}

// String renders the impact as one markdown bullet.
func (im Impact) String() string {
	head := fmt.Sprintf("- **%s** (at %.0f s): ", im.Label, im.At.Seconds())
	switch {
	case math.IsNaN(im.Worst):
		return head + "no data in the window.\n"
	case !im.Dipped:
		return head + fmt.Sprintf("no measurable hit-ratio dip (worst second %s vs %s before).\n", pct(im.Worst), pct(im.Baseline))
	case im.Recovered:
		return head + fmt.Sprintf("hit ratio fell to %s in its worst second (was %s) and was back within %.0f points after %d s.\n",
			pct(im.Worst), pct(im.Baseline), ImpactTolerance*100, im.RecoverySeconds)
	default:
		return head + fmt.Sprintf("hit ratio fell to %s in its worst second (was %s) and had not recovered by the end of the window.\n",
			pct(im.Worst), pct(im.Baseline))
	}
}
