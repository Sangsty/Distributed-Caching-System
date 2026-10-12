// Command cachectl is a small CLI for poking a running cluster through the real
// client library. It is also the quickest way to see failover happen.
//
//	cachectl [-coordinator URL] <command>
//
//	status                  show nodes, liveness and ring ownership
//	set <key> <value> [ttl] store a value (ttl like 30s)
//	get <key>               read a value
//	del <key>               delete a key
//	owner <key>             which node owns this key
//	fill <n>                set key:0 .. key:n-1 and show how they spread over nodes
//	check <n>               get key:0 .. key:n-1 and count hits, misses, errors
//
// Failover demo:  fill 10000, stop one node, then run check 10000 a few times.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"cachekit/client"
	"cachekit/cluster"
)

func main() {
	coord := flag.String("coordinator", "http://localhost:8080", "coordinator base URL")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: cachectl [-coordinator URL] status | set <k> <v> [ttl] | get <k> | del <k> | owner <k> | fill <n> | check <n>")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(*coord, args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(coord string, args []string) error {
	if args[0] == "status" {
		return status(coord)
	}
	c, err := client.New(client.Options{CoordinatorURL: coord, PollWait: 2 * time.Second})
	if err != nil {
		return err
	}
	defer c.Close()

	need := func(n int) error {
		if len(args) < n+1 {
			return fmt.Errorf("%s needs %d argument(s)", args[0], n)
		}
		return nil
	}
	switch args[0] {
	case "set":
		if err := need(2); err != nil {
			return err
		}
		var ttl time.Duration
		if len(args) > 3 {
			if ttl, err = time.ParseDuration(args[3]); err != nil {
				return fmt.Errorf("bad ttl %q: %w", args[3], err)
			}
		}
		owner, _ := c.Owner(args[1])
		if err := c.Set(args[1], []byte(args[2]), ttl); err != nil {
			return err
		}
		fmt.Printf("OK (stored on %s)\n", owner)
	case "get":
		if err := need(1); err != nil {
			return err
		}
		owner, _ := c.Owner(args[1])
		v, found, err := c.Get(args[1])
		if err != nil {
			return err
		}
		if !found {
			fmt.Printf("(miss) on %s\n", owner)
		} else {
			fmt.Printf("%s   (from %s)\n", v, owner)
		}
	case "del":
		if err := need(1); err != nil {
			return err
		}
		existed, err := c.Del(args[1])
		if err != nil {
			return err
		}
		fmt.Println("deleted:", existed)
	case "owner":
		if err := need(1); err != nil {
			return err
		}
		owner, ok := c.Owner(args[1])
		if !ok {
			return client.ErrNoNodes
		}
		fmt.Println(owner)
	case "fill":
		return fill(c, args)
	case "check":
		return check(c, args)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	return nil
}

func count(args []string) (int, error) {
	if len(args) < 2 {
		return 0, fmt.Errorf("%s needs a key count", args[0])
	}
	n, err := strconv.Atoi(args[1])
	if err != nil || n < 1 {
		return 0, fmt.Errorf("bad count %q", args[1])
	}
	return n, nil
}

func fill(c *client.Client, args []string) error {
	n, err := count(args)
	if err != nil {
		return err
	}
	perNode, errs := map[string]int{}, 0
	start := time.Now()
	for i := 0; i < n; i++ {
		k := "key:" + strconv.Itoa(i)
		owner, _ := c.Owner(k)
		if err := c.Set(k, []byte("value:"+strconv.Itoa(i)), 0); err != nil {
			errs++
			continue
		}
		perNode[owner]++
	}
	fmt.Printf("stored %d keys in %v (%d errors)\n", n-errs, time.Since(start).Round(time.Millisecond), errs)
	printSpread(perNode, n-errs)
	return nil
}

func check(c *client.Client, args []string) error {
	n, err := count(args)
	if err != nil {
		return err
	}
	hits, misses, errs := map[string]int{}, map[string]int{}, map[string]int{}
	for i := 0; i < n; i++ {
		k := "key:" + strconv.Itoa(i)
		owner, _ := c.Owner(k)
		_, found, err := c.Get(k)
		switch {
		case err != nil:
			var ne *client.NodeError
			if errors.As(err, &ne) {
				owner = ne.NodeID
			}
			errs[owner]++
		case found:
			hits[owner]++
		default:
			misses[owner]++
		}
	}
	h, m, e := sum(hits), sum(misses), sum(errs)
	fmt.Printf("hits %d (%.1f%%)   misses %d   errors %d   [ring version %d, nodes %v]\n",
		h, 100*float64(h)/float64(n), m, e, c.Version(), c.Nodes())
	if e > 0 {
		fmt.Println("errors per node (node unreachable, coordinator has not removed it yet):", errs)
	}
	return nil
}

func sum(m map[string]int) (t int) {
	for _, v := range m {
		t += v
	}
	return
}

func printSpread(perNode map[string]int, total int) {
	ids := make([]string, 0, len(perNode))
	for id := range perNode {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		fmt.Printf("  %-12s %6d keys  (%.1f%%)\n", id, perNode[id], 100*float64(perNode[id])/float64(max(total, 1)))
	}
}

func status(coord string) error {
	res, err := (&http.Client{Timeout: 5 * time.Second}).Get(strings.TrimRight(coord, "/") + "/status")
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("coordinator returned %s: %s", res.Status, b)
	}
	var st cluster.Status
	if err := json.NewDecoder(res.Body).Decode(&st); err != nil {
		return err
	}
	fmt.Printf("membership version %d, %d vnodes per node\n", st.Version, st.VNodes)
	for _, n := range st.Nodes {
		state := "alive"
		if !n.Alive {
			state = "DEAD "
		}
		fmt.Printf("  %-12s %-5s %-22s ring share %5.1f%%   last heartbeat %dms ago\n",
			n.ID, state, n.Addr, 100*n.Ownership, n.LastSeenMs)
	}
	return nil
}
