// Command loadgen drives the demo API with a Zipfian (hot-key) read workload at
// a fixed request rate, can run shell commands at chosen moments (for example
// `docker kill` a cache node), and records what the application experienced:
// hit ratio, latency percentiles, errors, and which cache node owned each key,
// per second.
//
//	# steady state, 30 s, against the cached app
//	go run ./cmd/loadgen -url http://127.0.0.1:9090 -duration 30s -out docs/results/cached
//
//	# crash one cache node at 30 s, bring it back at 60 s
//	go run ./cmd/loadgen -duration 90s -out docs/results/failure `
//	    -event "30s|crash node2|docker kill cachekit-node2-1" `
//	    -event "60s|restart node2|docker start cachekit-node2-1"
//
// The load is OPEN-LOOP: requests are issued on a fixed schedule no matter how
// slowly the server answers, and each latency is measured from the moment the
// request was scheduled. (A closed loop, where each worker waits for its
// response before sending the next, quietly lowers the load exactly when the
// server is struggling, which hides the very latency spike a failure graph is
// meant to show.)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"cachekit/loadtest"
)

type eventFlag struct {
	at    time.Duration
	label string
	cmd   []string
}

type eventFlags []eventFlag

func (e *eventFlags) String() string { return fmt.Sprint(len(*e), " events") }

// Set parses "<offset>|<label>|<command>", e.g. "30s|crash node2|docker kill cachekit-node2-1".
func (e *eventFlags) Set(v string) error {
	parts := strings.SplitN(v, "|", 3)
	if len(parts) != 3 {
		return fmt.Errorf("want <offset>|<label>|<command>, got %q", v)
	}
	at, err := time.ParseDuration(strings.TrimSpace(parts[0]))
	if err != nil || at < 0 {
		return fmt.Errorf("bad offset %q", parts[0])
	}
	cmd := strings.Fields(parts[2])
	if len(cmd) == 0 {
		return fmt.Errorf("empty command in %q", v)
	}
	*e = append(*e, eventFlag{at: at, label: strings.TrimSpace(parts[1]), cmd: cmd})
	return nil
}

type serverStats struct {
	CacheHits    uint64 `json:"cache_hits"`
	CacheMisses  uint64 `json:"cache_misses"`
	CacheErrors  uint64 `json:"cache_errors"`
	DBReads      uint64 `json:"db_reads"`
	Coalesced    uint64 `json:"coalesced_loads"`
	Invalidation uint64 `json:"invalidations"`
}

func fetchStats(c *http.Client, base string) (serverStats, bool) {
	var st serverStats
	resp, err := c.Get(base + "/stats")
	if err != nil {
		return st, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || json.NewDecoder(resp.Body).Decode(&st) != nil {
		return st, false
	}
	return st, true
}

type job struct {
	id    int64
	sched time.Duration
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run() error {
	base := flag.String("url", "http://127.0.0.1:9090", "base URL of the demo API")
	duration := flag.Duration("duration", 60*time.Second, "length of the run")
	rate := flag.Float64("rate", 400, "requests per second (open loop)")
	keys := flag.Int("keys", 1000, "number of product ids to draw from (1..keys); the seeded database has 1000")
	zipfS := flag.Float64("zipf", 1.1, "Zipf exponent (> 1); larger = a few keys get even more of the traffic")
	seed := flag.Int64("seed", 1, "random seed (same seed = same request sequence)")
	workers := flag.Int("workers", 256, "concurrent in-flight requests allowed")
	warmup := flag.Duration("warmup", 10*time.Second, "initial period excluded from the summary table (cold cache filling); still plotted")
	out := flag.String("out", "", "directory for timeseries.csv, nodes.csv, events.csv, summary.md (empty = print only)")
	var events eventFlags
	flag.Var(&events, "event", `run a command at an offset: "<offset>|<label>|<command>" (repeatable)`)
	flag.Parse()

	if *rate <= 0 || *duration <= 0 || *workers < 1 {
		return fmt.Errorf("-rate, -duration and -workers must be positive")
	}
	keygen, err := loadtest.NewKeyGen(*seed, *keys, *zipfS)
	if err != nil {
		return err
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns: *workers, MaxIdleConnsPerHost: *workers, IdleConnTimeout: 90 * time.Second,
		},
	}
	// Fail early, with a useful message, if the app is not up.
	resp, err := client.Get(*base + "/healthz")
	if err != nil {
		return fmt.Errorf("cannot reach %s (is the stack up? try `docker compose -f deploy/docker-compose.yml up -d`): %w", *base, err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s/healthz returned %d", *base, resp.StatusCode)
	}
	statsBefore, haveStats := fetchStats(client, *base)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	col := loadtest.NewCollector()
	start := time.Now()

	// Scheduled events (kill / restart a node, ...).
	var (
		evMu     sync.Mutex
		happened []loadtest.Event
		evWG     sync.WaitGroup
	)
	evCtx, evCancel := context.WithCancel(ctx)
	defer evCancel()
	sort.Slice(events, func(i, j int) bool { return events[i].at < events[j].at })
	for _, ev := range events {
		evWG.Add(1)
		go func(ev eventFlag) {
			defer evWG.Done()
			if err := sleepUntil(evCtx, start.Add(ev.at)); err != nil {
				return
			}
			issued := time.Since(start)
			cctx, cancel := context.WithTimeout(evCtx, 60*time.Second)
			defer cancel()
			outp, err := exec.CommandContext(cctx, ev.cmd[0], ev.cmd[1:]...).CombinedOutput()
			if err != nil {
				fmt.Fprintf(os.Stderr, "\n!! event %q FAILED at %.1fs: %v\n%s\n", ev.label, issued.Seconds(), err, outp)
				return
			}
			fmt.Printf("  [%5.1fs] %s (%s)\n", issued.Seconds(), ev.label, strings.Join(ev.cmd, " "))
			evMu.Lock()
			happened = append(happened, loadtest.Event{At: issued, Label: ev.label})
			evMu.Unlock()
		}(ev)
	}

	fmt.Printf("loadgen: %s  %.0f req/s for %s, Zipf s=%.2f over %d keys, seed %d\n", *base, *rate, *duration, *zipfS, *keys, *seed)

	// Workers.
	jobs := make(chan job, 8192)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				col.Add(doRequest(client, *base, j, start))
			}
		}()
	}

	// Dispatcher: fixed schedule, independent of how fast responses come back.
	interval := time.Duration(float64(time.Second) / *rate)
	keyCounts := map[int64]int{}
	issuedTotal := 0
	var lastSched time.Duration
	interrupted := false
	for i := 0; ; i++ {
		sched := time.Duration(i) * interval
		if sched >= *duration {
			break
		}
		if sleepUntil(ctx, start.Add(sched)) != nil {
			interrupted = true // Ctrl+C: report what we have
			break
		}
		id := keygen.Next()
		keyCounts[id]++
		issuedTotal++
		lastSched = sched
		select {
		case jobs <- job{id, sched}:
		default:
			col.Drop(sched) // the load generator itself is saturated
		}
	}
	close(jobs)
	wg.Wait()
	evCancel()
	evWG.Wait()

	runLen := *duration
	if interrupted {
		runLen = lastSched + interval
	}
	total := int(math.Ceil(runLen.Seconds()))
	if total < 1 {
		total = 1
	}

	statsAfter, haveStatsAfter := fetchStats(client, *base)

	evMu.Lock()
	evs := append([]loadtest.Event(nil), happened...)
	evMu.Unlock()
	sort.Slice(evs, func(i, j int) bool { return evs[i].At < evs[j].At })

	// ---- report ----
	phases := loadtest.BuildPhases(*warmup, runLen, evs)
	var stats []loadtest.PhaseStats
	for _, p := range phases {
		stats = append(stats, col.Stats(p))
	}
	secs := col.Seconds(total)
	var sum strings.Builder
	fmt.Fprintf(&sum, "Load: %.0f req/s open-loop for %s, Zipf s=%.2f over %d keys (seed %d); the first %s is excluded from the table.\n\n",
		*rate, runLen.Round(time.Second), *zipfS, *keys, *seed, *warmup)
	sum.WriteString(loadtest.SummaryMarkdown(stats))

	// How each event moved the hit ratio, relative to the steady state before the first event.
	if len(stats) > 0 && strings.HasPrefix(stats[0].Name, "before:") && !math.IsNaN(stats[0].HitRatio) {
		sum.WriteString("\nEvent impact (hit ratio, per second):\n\n")
		for i, e := range evs {
			end := runLen
			if i+1 < len(evs) {
				end = evs[i+1].At
			}
			sum.WriteString(loadtest.ComputeImpact(secs, e, end, stats[0].HitRatio).String())
		}
	}

	// Share of traffic taken by the hottest keys.
	type kc struct {
		id int64
		n  int
	}
	var all []kc
	for id, n := range keyCounts {
		all = append(all, kc{id, n})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].n > all[j].n })
	top := func(k int) float64 {
		s := 0
		for i := 0; i < k && i < len(all); i++ {
			s += all[i].n
		}
		return 100 * float64(s) / float64(max(issuedTotal, 1))
	}
	hotN := max(*keys/10, 1)
	fmt.Fprintf(&sum, "\nHot keys: the 10 hottest keys received %.0f%% of requests and the hottest 10%% (%d keys) received %.0f%%.\n",
		top(10), hotN, top(hotN))

	if haveStats && haveStatsAfter {
		reads := statsAfter.DBReads - statsBefore.DBReads
		fmt.Fprintf(&sum, "\nDatabase reads during the run: %d (%.0f per 1000 requests); cache errors seen by the app: %d.\n",
			reads, 1000*float64(reads)/float64(max(issuedTotal, 1)), statsAfter.CacheErrors-statsBefore.CacheErrors)
	}
	nodes, perSec := col.NodeCounts(total)
	if len(nodes) > 0 {
		// Share per owning node over the steady state before the first event (or the whole
		// run if there are none), so a crash does not blur which node owns what.
		from, to := int(warmup.Seconds()), total
		scope := "after the warm-up"
		if len(evs) > 0 {
			if at := int(evs[0].At.Seconds()); at > from {
				to = at
				scope = "before the first event"
			}
		}
		totals := map[string]int{}
		served := 0
		for t := from; t < to && t < len(perSec); t++ {
			for n, c := range perSec[t] {
				totals[n] += c
				served += c
			}
		}
		if served > 0 {
			fmt.Fprintf(&sum, "\nShare of requests by owning cache node (%s):", scope)
			for _, n := range nodes {
				fmt.Fprintf(&sum, " %s %.0f%%", n, 100*float64(totals[n])/float64(served))
			}
			sum.WriteString(".\n")
		}
	}
	var dropped int
	for _, s := range secs {
		dropped += s.Dropped
	}
	if dropped > 0 {
		fmt.Fprintf(&sum, "\nWARNING: the load generator dropped %d requests because it could not keep up (raise -workers or lower -rate); the numbers above under-report load.\n", dropped)
	}
	fmt.Println()
	fmt.Print(sum.String())

	if *out == "" {
		return nil
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	write := func(name string, fn func(io.Writer) error) error {
		f, err := os.Create(filepath.Join(*out, name))
		if err != nil {
			return err
		}
		defer f.Close()
		return fn(f)
	}
	if err := write("timeseries.csv", func(w io.Writer) error { return loadtest.WriteTimeseries(w, secs) }); err != nil {
		return err
	}
	if err := write("nodes.csv", func(w io.Writer) error { return loadtest.WriteNodes(w, nodes, perSec) }); err != nil {
		return err
	}
	if err := write("events.csv", func(w io.Writer) error { return loadtest.WriteEvents(w, evs) }); err != nil {
		return err
	}
	if err := write("summary.md", func(w io.Writer) error { _, err := io.WriteString(w, sum.String()); return err }); err != nil {
		return err
	}
	fmt.Printf("\nwrote %s (timeseries.csv, nodes.csv, events.csv, summary.md)\n", *out)
	fmt.Printf("plot it:  python docs/plot_failure.py %s\n", *out)
	return nil
}

func doRequest(c *http.Client, base string, j job, start time.Time) loadtest.Sample {
	s := loadtest.Sample{Offset: j.sched}
	req, err := http.NewRequest(http.MethodGet, base+"/products/"+strconv.FormatInt(j.id, 10), nil)
	if err == nil {
		var resp *http.Response
		if resp, err = c.Do(req); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			s.Status = resp.StatusCode
			s.Cache = resp.Header.Get("X-Cache")
			s.Node = resp.Header.Get("X-Cache-Node")
		}
	}
	// Measured from the SCHEDULED time, so queueing behind a slow server counts.
	s.Latency = time.Since(start.Add(j.sched))
	return s
}

func sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
