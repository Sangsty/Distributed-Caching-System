// Command experiment produces the two headline measurements for the report:
//
//  1. remap.csv   — fraction of keys that change owner when one node is added,
//     consistent hashing vs. hash(key) % N.
//  2. balance.csv — load imbalance across nodes as the number of virtual nodes
//     grows.
//
// Pure in-memory; no network. Usage:
//
//	go run ./cmd/experiment -out docs/results
//	python docs/plot_results.py docs/results
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"

	"cachekit/ring"
)

func main() {
	keysN := flag.Int("keys", 100000, "number of keys")
	vnodes := flag.Int("vnodes", ring.DefaultVNodes, "virtual nodes per node for the remap experiment")
	maxNodes := flag.Int("max-nodes", 20, "largest cluster size for the remap experiment")
	trials := flag.Int("trials", 20, "trials per point in the balance experiment")
	out := flag.String("out", "docs/results", "output directory for CSV files")
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	keys := make([]string, *keysN)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", i)
	}

	if err := remap(keys, *vnodes, *maxNodes, filepath.Join(*out, "remap.csv")); err != nil {
		log.Fatal(err)
	}
	if err := balance(keys, *trials, filepath.Join(*out, "balance.csv")); err != nil {
		log.Fatal(err)
	}
	fmt.Println("wrote", filepath.Join(*out, "remap.csv"), "and", filepath.Join(*out, "balance.csv"))
}

// remap: for each cluster size N, add one node and count moved keys.
func remap(keys []string, vnodes, maxNodes int, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"nodes_before", "consistent_moved_pct", "modulo_moved_pct", "ideal_pct"})

	fmt.Printf("%-8s %-22s %-18s %s\n", "N->N+1", "consistent hashing %", "hash % N  %", "ideal 1/(N+1) %")
	hashes := make([]uint64, len(keys))
	for i, k := range keys {
		hashes[i] = ring.Hash(k)
	}
	for n := 2; n <= maxNodes; n++ {
		r := ring.New(vnodes)
		for i := 0; i < n; i++ {
			r.AddNode(fmt.Sprintf("node-%d", i))
		}
		before := make([]string, len(keys))
		for i, k := range keys {
			before[i], _ = r.GetNode(k)
		}
		r.AddNode(fmt.Sprintf("node-%d", n))
		movedC, movedM := 0, 0
		for i, k := range keys {
			if after, _ := r.GetNode(k); after != before[i] {
				movedC++
			}
			if hashes[i]%uint64(n) != hashes[i]%uint64(n+1) {
				movedM++
			}
		}
		pc := 100 * float64(movedC) / float64(len(keys))
		pm := 100 * float64(movedM) / float64(len(keys))
		ideal := 100 / float64(n+1)
		w.Write([]string{fmt.Sprint(n), fmt.Sprintf("%.3f", pc), fmt.Sprintf("%.3f", pm), fmt.Sprintf("%.3f", ideal)})
		fmt.Printf("%-8s %-22.2f %-18.2f %.2f\n", fmt.Sprintf("%d->%d", n, n+1), pc, pm, ideal)
	}
	w.Flush()
	return w.Error()
}

// balance: 10 nodes, vary vnodes, average over several differently-named
// clusters so the numbers are not one lucky or unlucky draw.
func balance(keys []string, trials int, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"vnodes", "cv_pct", "max_over_mean"})

	const nodes = 10
	fmt.Printf("\n%-8s %-22s %s\n", "vnodes", "std dev / mean %", "max / mean")
	for _, vn := range []int{1, 5, 10, 25, 50, 100, 150, 300, 500} {
		var cvSum, maxSum float64
		for t := 0; t < trials; t++ {
			r := ring.New(vn)
			ids := make([]string, nodes)
			for i := range ids {
				ids[i] = fmt.Sprintf("t%d-node-%d", t, i)
				r.AddNode(ids[i])
			}
			counts := map[string]int{}
			for _, k := range keys {
				n, _ := r.GetNode(k)
				counts[n]++
			}
			mean := float64(len(keys)) / nodes
			var sq, max float64
			for _, id := range ids { // include nodes that got zero keys
				c := float64(counts[id])
				sq += (c - mean) * (c - mean)
				if c > max {
					max = c
				}
			}
			cvSum += math.Sqrt(sq/nodes) / mean
			maxSum += max / mean
		}
		cv := 100 * cvSum / float64(trials)
		mx := maxSum / float64(trials)
		w.Write([]string{fmt.Sprint(vn), fmt.Sprintf("%.3f", cv), fmt.Sprintf("%.3f", mx)})
		fmt.Printf("%-8d %-22.2f %.2f\n", vn, cv, mx)
	}
	w.Flush()
	return w.Error()
}
