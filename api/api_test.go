package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"cachekit/catalog"
	"cachekit/client"
	"cachekit/cluster"
	"cachekit/node"
)

// env is a real (in-process) two-node cache cluster behind a catalog.Service
// and the API handler, with an in-memory repository as the "database".
type env struct {
	t     *testing.T
	repo  *catalog.MemoryRepo
	svc   *catalog.Service
	nodes map[string]*node.Node
	srv   *httptest.Server
}

func newEnv(t *testing.T, cfg catalog.Config, wrap func(catalog.Repository) catalog.Repository, opts Options) *env {
	t.Helper()
	e := &env{t: t, repo: catalog.NewMemoryRepo(), nodes: map[string]*node.Node{}}
	e.repo.Seed(20)

	var infos []cluster.NodeInfo
	for _, id := range []string{"cache-a", "cache-b"} {
		n, err := node.Start(node.Config{ID: id, Listen: "127.0.0.1:0"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { n.Stop(false) })
		e.nodes[id] = n
		infos = append(infos, cluster.NodeInfo{ID: id, Addr: n.Addr()})
	}
	c := client.NewStatic(infos, client.Options{DialTimeout: 300 * time.Millisecond, OpTimeout: 300 * time.Millisecond})
	t.Cleanup(c.Close)

	var repo catalog.Repository = e.repo
	if wrap != nil {
		repo = wrap(repo)
	}
	e.svc = catalog.NewService(repo, c, cfg, nil)
	e.srv = httptest.NewServer(NewHandler(e.svc, opts))
	t.Cleanup(e.srv.Close)
	return e
}

func (e *env) do(method, path, body string) (*http.Response, []byte) {
	e.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, rd)
	if err != nil {
		e.t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func decodeProduct(t *testing.T, b []byte) catalog.Product {
	t.Helper()
	var p catalog.Product
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatalf("bad product JSON %q: %v", b, err)
	}
	return p
}

func errorText(t *testing.T, b []byte) string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil || m["error"] == "" {
		t.Fatalf("expected a JSON error body, got %q (%v)", b, err)
	}
	return m["error"]
}

func TestGetMissThenHitAndPlacement(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{})

	r1, b1 := e.do("GET", "/products/3", "")
	if r1.StatusCode != 200 || r1.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("first: %d X-Cache=%q", r1.StatusCode, r1.Header.Get("X-Cache"))
	}
	if ct := r1.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type %q", ct)
	}
	r2, b2 := e.do("GET", "/products/3", "")
	if r2.StatusCode != 200 || r2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("second: %d X-Cache=%q", r2.StatusCode, r2.Header.Get("X-Cache"))
	}
	if p1, p2 := decodeProduct(t, b1), decodeProduct(t, b2); p1.ID != 3 || p1.Name != p2.Name || !p1.UpdatedAt.Equal(p2.UpdatedAt) {
		t.Fatalf("bodies differ: %+v vs %+v", p1, p2)
	}
	if e.repo.Reads() != 1 {
		t.Fatalf("db reads = %d, want 1", e.repo.Reads())
	}

	// The header names the node that really holds the entry (and only it).
	owner := r2.Header.Get("X-Cache-Node")
	if owner == "" {
		t.Fatal("missing X-Cache-Node")
	}
	for id, n := range e.nodes {
		_, has := n.Store().Get("product:3")
		if has != (id == owner) {
			t.Fatalf("node %s has=%v but owner is %s", id, has, owner)
		}
	}
}

func TestGetNotFoundAndNegativeCache(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{})
	r1, b1 := e.do("GET", "/products/999", "")
	if r1.StatusCode != 404 || r1.Header.Get("X-Cache") != "MISS" || errorText(t, b1) != "product not found" {
		t.Fatalf("first: %d %q %s", r1.StatusCode, r1.Header.Get("X-Cache"), b1)
	}
	r2, _ := e.do("GET", "/products/999", "")
	if r2.StatusCode != 404 || r2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("second: %d X-Cache=%q (a cached not-found is a hit)", r2.StatusCode, r2.Header.Get("X-Cache"))
	}
	if e.repo.Reads() != 1 {
		t.Fatalf("db reads = %d", e.repo.Reads())
	}
}

func TestBadIDs(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{})
	for _, id := range []string{"abc", "0", "-4", "1.5", "99999999999999999999999"} {
		for _, m := range []string{"GET", "DELETE"} {
			r, b := e.do(m, "/products/"+id, "")
			if r.StatusCode != 400 {
				t.Errorf("%s /products/%s = %d, want 400", m, id, r.StatusCode)
				continue
			}
			errorText(t, b)
		}
	}
	if e.repo.Reads() != 0 {
		t.Fatal("bad ids must not reach the database")
	}
}

func TestList(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{})
	r, b := e.do("GET", "/products?limit=5&offset=3", "")
	if r.StatusCode != 200 {
		t.Fatalf("status %d", r.StatusCode)
	}
	var out struct{ Products []catalog.Product }
	if err := json.Unmarshal(b, &out); err != nil || len(out.Products) != 5 || out.Products[0].ID != 4 {
		t.Fatalf("list: %s (%v)", b, err)
	}
	// Defaults and clamping.
	_, b = e.do("GET", "/products", "")
	json.Unmarshal(b, &out)
	if len(out.Products) != 20 {
		t.Fatalf("default page = %d, want 20 (only 20 seeded)", len(out.Products))
	}
	_, b = e.do("GET", "/products?offset=1000", "")
	if !strings.Contains(string(b), `"products":[]`) {
		t.Fatalf("empty page must be [] not null: %s", b)
	}
	if r, _ := e.do("GET", "/products?limit=abc", ""); r.StatusCode != 400 {
		t.Fatalf("limit=abc -> %d", r.StatusCode)
	}
}

func TestCreateUpdateDeleteKeepCacheConsistent(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{})

	// Create: 201 + Location, then readable.
	r, b := e.do("POST", "/products", `{"name":"  Gadget ","description":"d","price_cents":1999,"stock":3}`)
	if r.StatusCode != 201 {
		t.Fatalf("create: %d %s", r.StatusCode, b)
	}
	created := decodeProduct(t, b)
	if created.Name != "Gadget" || created.ID != 21 || r.Header.Get("Location") != "/products/21" {
		t.Fatalf("create: %+v loc=%q", created, r.Header.Get("Location"))
	}

	// Read twice so it is cached, then update and make sure no stale copy is served.
	e.do("GET", "/products/21", "")
	if r, _ := e.do("GET", "/products/21", ""); r.Header.Get("X-Cache") != "HIT" {
		t.Fatal("expected a cache hit before the update")
	}
	r, b = e.do("PUT", "/products/21", `{"name":"Gadget v2","price_cents":2500,"stock":9}`)
	if r.StatusCode != 200 || decodeProduct(t, b).Name != "Gadget v2" {
		t.Fatalf("update: %d %s", r.StatusCode, b)
	}
	r, b = e.do("GET", "/products/21", "")
	if r.Header.Get("X-Cache") != "MISS" || decodeProduct(t, b).Name != "Gadget v2" || decodeProduct(t, b).PriceCents != 2500 {
		t.Fatalf("after update: X-Cache=%q %s", r.Header.Get("X-Cache"), b)
	}

	// Delete: 204, then 404 (not served from a stale cache entry).
	e.do("GET", "/products/21", "") // cached again
	if r, _ := e.do("DELETE", "/products/21", ""); r.StatusCode != 204 {
		t.Fatalf("delete: %d", r.StatusCode)
	}
	if r, _ := e.do("GET", "/products/21", ""); r.StatusCode != 404 {
		t.Fatalf("get after delete: %d", r.StatusCode)
	}
	if r, _ := e.do("DELETE", "/products/21", ""); r.StatusCode != 404 {
		t.Fatalf("second delete: %d", r.StatusCode)
	}
	if r, _ := e.do("PUT", "/products/777", `{"name":"x"}`); r.StatusCode != 404 {
		t.Fatalf("update missing: %d", r.StatusCode)
	}
}

func TestWriteValidation(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{})
	cases := map[string]string{
		"empty name":      `{"name":"","price_cents":1}`,
		"negative price":  `{"name":"x","price_cents":-5}`,
		"negative stock":  `{"name":"x","stock":-1}`,
		"unknown field":   `{"name":"x","colour":"red"}`,
		"wrong type":      `{"name":"x","price_cents":"cheap"}`,
		"not json":        `name=x`,
		"empty body":      ``,
		"trailing data":   `{"name":"x"} {"name":"y"}`,
		"huge body":       `{"name":"x","description":"` + strings.Repeat("a", 70<<10) + `"}`,
		"description cap": `{"name":"x","description":"` + strings.Repeat("a", 2001) + `"}`,
	}
	for name, body := range cases {
		r, b := e.do("POST", "/products", body)
		if r.StatusCode != 400 {
			t.Errorf("%s: status %d, want 400 (%s)", name, r.StatusCode, b)
			continue
		}
		errorText(t, b)
	}
	if r, _ := e.do("PUT", "/products/1", `{"name":""}`); r.StatusCode != 400 {
		t.Errorf("PUT invalid: %d", r.StatusCode)
	}
	if n := len(e.mustList()); n != 20 {
		t.Fatalf("rejected writes changed the data: %d products", n)
	}
}

func (e *env) mustList() []catalog.Product {
	ps, err := e.svc.List(context.Background(), 100, 0)
	if err != nil {
		e.t.Fatal(err)
	}
	return ps
}

func TestWrongMethod(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{})
	if r, _ := e.do("PATCH", "/products/1", `{}`); r.StatusCode != 405 {
		t.Fatalf("PATCH -> %d, want 405", r.StatusCode)
	}
	if r, _ := e.do("GET", "/nope", ""); r.StatusCode != 404 {
		t.Fatalf("unknown path -> %d", r.StatusCode)
	}
}

// The headline property: losing a cache node must not break the API.
func TestCacheNodeFailureFallsBackToDatabase(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{})

	// Find a product that lives on cache-a, warm it, then kill cache-a.
	var id int64
	for i := int64(1); i <= 20; i++ {
		if e.svc.CacheNode(i) == "cache-a" {
			id = i
			break
		}
	}
	if id == 0 {
		t.Skip("no sample id maps to cache-a")
	}
	path := "/products/" + strconv.FormatInt(id, 10)
	e.do("GET", path, "")
	if r, _ := e.do("GET", path, ""); r.Header.Get("X-Cache") != "HIT" {
		t.Fatal("expected a hit while the node is up")
	}
	e.nodes["cache-a"].Stop(false)

	before := e.repo.Reads()
	for i := 0; i < 3; i++ {
		r, b := e.do("GET", path, "")
		if r.StatusCode != 200 || r.Header.Get("X-Cache") != "MISS" || decodeProduct(t, b).ID != id {
			t.Fatalf("with the node down: %d X-Cache=%q %s", r.StatusCode, r.Header.Get("X-Cache"), b)
		}
	}
	if got := e.repo.Reads() - before; got != 3 {
		t.Fatalf("db reads = %d, want 3 (every read falls through)", got)
	}
	// Writes still work too.
	if r, _ := e.do("PUT", path, `{"name":"still works"}`); r.StatusCode != 200 {
		t.Fatalf("update with the cache node down: %d", r.StatusCode)
	}
	// Products on the surviving node are unaffected.
	for i := int64(1); i <= 20; i++ {
		if e.svc.CacheNode(i) == "cache-b" {
			p := "/products/" + strconv.FormatInt(i, 10)
			e.do("GET", p, "")
			if r, _ := e.do("GET", p, ""); r.Header.Get("X-Cache") != "HIT" {
				t.Fatalf("cache-b should still serve hits")
			}
			break
		}
	}
	var st catalog.Stats
	_, b := e.do("GET", "/stats", "")
	if err := json.Unmarshal(b, &st); err != nil || st.CacheErrors == 0 {
		t.Fatalf("stats should show cache errors: %s (%v)", b, err)
	}
}

func TestLoadTimeoutIs504(t *testing.T) {
	e := newEnv(t, catalog.Config{LoadTimeout: 50 * time.Millisecond},
		func(r catalog.Repository) catalog.Repository { return catalog.WithLatency(r, 2*time.Second) }, Options{})
	start := time.Now()
	r, b := e.do("GET", "/products/1", "")
	if r.StatusCode != 504 {
		t.Fatalf("status %d (%s), want 504", r.StatusCode, b)
	}
	if time.Since(start) > time.Second {
		t.Fatal("did not time out promptly")
	}
	errorText(t, b)
}

func TestHealthz(t *testing.T) {
	var down bool
	e := newEnv(t, catalog.Config{}, nil, Options{Ping: func(context.Context) error {
		if down {
			return errors.New("connection refused")
		}
		return nil
	}})
	if r, _ := e.do("GET", "/healthz", ""); r.StatusCode != 200 {
		t.Fatalf("healthy: %d", r.StatusCode)
	}
	down = true
	r, b := e.do("GET", "/healthz", "")
	if r.StatusCode != 503 || strings.Contains(string(b), "refused") {
		t.Fatalf("unhealthy: %d %s (internal error text must not leak)", r.StatusCode, b)
	}
}

func TestPanicIsRecovered(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{Ping: func(context.Context) error { panic("boom") }})
	r, b := e.do("GET", "/healthz", "")
	if r.StatusCode != 500 || strings.Contains(string(b), "boom") {
		t.Fatalf("panic: %d %s", r.StatusCode, b)
	}
	// The server keeps serving.
	if r, _ := e.do("GET", "/products/1", ""); r.StatusCode != 200 {
		t.Fatalf("after panic: %d", r.StatusCode)
	}
}

func TestStatsEndpoint(t *testing.T) {
	e := newEnv(t, catalog.Config{}, nil, Options{})
	e.do("GET", "/products/1", "")
	e.do("GET", "/products/1", "")
	_, b := e.do("GET", "/stats", "")
	var st catalog.Stats
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	if st.CacheHits != 1 || st.CacheMisses != 1 || st.DBReads != 1 || st.HitRatio != 0.5 || len(st.CacheNodes) != 2 {
		t.Fatalf("stats: %s", b)
	}
}
