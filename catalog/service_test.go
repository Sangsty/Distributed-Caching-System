package catalog

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeCache is an in-memory Cache that records the TTLs it was given and can be
// told to fail.
type fakeCache struct {
	mu      sync.Mutex
	data    map[string][]byte
	ttls    map[string]time.Duration
	failGet bool
	failSet bool
	failDel bool
	sets    int
	dels    int
}

var errCacheDown = errors.New("cache node down")

func newFakeCache() *fakeCache {
	return &fakeCache{data: map[string][]byte{}, ttls: map[string]time.Duration{}}
}

func (f *fakeCache) Get(key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failGet {
		return nil, false, errCacheDown
	}
	v, ok := f.data[key]
	return v, ok, nil
}

func (f *fakeCache) Set(key string, val []byte, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSet {
		return errCacheDown
	}
	f.data[key] = val
	f.ttls[key] = ttl
	f.sets++
	return nil
}

func (f *fakeCache) Del(key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failDel {
		return false, errCacheDown
	}
	f.dels++
	_, ok := f.data[key]
	delete(f.data, key)
	return ok, nil
}

func (f *fakeCache) Owner(string) (string, bool) { return "node-x", true }
func (f *fakeCache) Nodes() []string             { return []string{"node-x"} }
func (f *fakeCache) Version() uint64             { return 7 }

func (f *fakeCache) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.data[key]
	return ok
}

func (f *fakeCache) put(key string, val []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = val
}

func newTestService(t *testing.T) (*Service, *MemoryRepo, *fakeCache) {
	t.Helper()
	repo := NewMemoryRepo()
	repo.Seed(10)
	fc := newFakeCache()
	return NewService(repo, fc, Config{}, nil), repo, fc
}

func TestGetMissThenHit(t *testing.T) {
	s, repo, _ := newTestService(t)
	ctx := context.Background()

	p1, src, err := s.Get(ctx, 3)
	if err != nil || src != SourceDB || p1.ID != 3 {
		t.Fatalf("first get: %+v %v %v", p1, src, err)
	}
	p2, src, err := s.Get(ctx, 3)
	if err != nil || src != SourceCache {
		t.Fatalf("second get: %v %v", src, err)
	}
	if p2.ID != p1.ID || p2.Name != p1.Name || p2.PriceCents != p1.PriceCents || p2.Stock != p1.Stock || !p2.UpdatedAt.Equal(p1.UpdatedAt) {
		t.Fatalf("cached copy differs: %+v vs %+v", p2, p1)
	}
	if repo.Reads() != 1 {
		t.Fatalf("db reads = %d, want 1", repo.Reads())
	}
	st := s.Stats()
	if st.CacheHits != 1 || st.CacheMisses != 1 || st.DBReads != 1 || st.HitRatio != 0.5 {
		t.Fatalf("stats: %+v", st)
	}
	if st.RingVersion != 7 || len(st.CacheNodes) != 1 {
		t.Fatalf("ring info missing from stats: %+v", st)
	}
}

func TestGetTTLIsJittered(t *testing.T) {
	repo := NewMemoryRepo()
	repo.Seed(200)
	fc := newFakeCache()
	s := NewService(repo, fc, Config{TTL: 100 * time.Second, TTLJitter: 0.1}, nil)
	for id := int64(1); id <= 200; id++ {
		if _, _, err := s.Get(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	lo, hi := time.Duration(1<<62), time.Duration(0)
	for _, ttl := range fc.ttls {
		if ttl < 90*time.Second || ttl > 110*time.Second {
			t.Fatalf("ttl %v outside ±10%% of 100s", ttl)
		}
		if ttl < lo {
			lo = ttl
		}
		if ttl > hi {
			hi = ttl
		}
	}
	if hi-lo < time.Second {
		t.Fatalf("ttls barely vary (%v..%v): jitter not applied", lo, hi)
	}
}

func TestNegativeCaching(t *testing.T) {
	s, repo, fc := newTestService(t)
	ctx := context.Background()

	if _, src, err := s.Get(ctx, 999); !errors.Is(err, ErrNotFound) || src != SourceDB {
		t.Fatalf("first: %v %v", src, err)
	}
	if got := fc.ttls[cacheKey(999)]; got != 15*time.Second {
		t.Fatalf("negative ttl = %v, want 15s", got)
	}
	if _, src, err := s.Get(ctx, 999); !errors.Is(err, ErrNotFound) || src != SourceCache {
		t.Fatalf("second: %v %v", src, err)
	}
	if repo.Reads() != 1 {
		t.Fatalf("db reads = %d, want 1 (the second miss must come from the negative entry)", repo.Reads())
	}
	if st := s.Stats(); st.NegativeHits != 1 || st.CacheMisses != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestCreateClearsNegativeEntry(t *testing.T) {
	s, repo, fc := newTestService(t)
	ctx := context.Background()
	// Cache "id 11 does not exist", then create product 11.
	if _, _, err := s.Get(ctx, 11); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if !fc.has(cacheKey(11)) {
		t.Fatal("negative entry should be cached")
	}
	p, err := s.Create(ctx, Input{Name: "New", PriceCents: 500, Stock: 1})
	if err != nil || p.ID != 11 {
		t.Fatalf("create: %+v %v", p, err)
	}
	if fc.has(cacheKey(11)) {
		t.Fatal("create must invalidate the negative entry for the new id")
	}
	got, _, err := s.Get(ctx, 11)
	if err != nil || got.Name != "New" {
		t.Fatalf("get after create: %+v %v", got, err)
	}
	_ = repo
}

func TestUpdateInvalidates(t *testing.T) {
	s, _, fc := newTestService(t)
	ctx := context.Background()
	if _, _, err := s.Get(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if !fc.has(cacheKey(2)) {
		t.Fatal("expected cached")
	}
	upd, err := s.Update(ctx, 2, Input{Name: "Renamed", PriceCents: 1, Stock: 2})
	if err != nil || upd.Name != "Renamed" {
		t.Fatalf("update: %+v %v", upd, err)
	}
	if fc.has(cacheKey(2)) {
		t.Fatal("update must delete the cached copy")
	}
	got, src, err := s.Get(ctx, 2)
	if err != nil || src != SourceDB || got.Name != "Renamed" {
		t.Fatalf("read after update: %+v %v %v", got, src, err)
	}
}

func TestDeleteInvalidates(t *testing.T) {
	s, _, fc := newTestService(t)
	ctx := context.Background()
	if _, _, err := s.Get(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if fc.has(cacheKey(4)) {
		t.Fatal("delete must remove the cached copy")
	}
	if _, _, err := s.Get(ctx, 4); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	if err := s.Delete(ctx, 4); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}

func TestWritesValidateAndDoNotTouchCacheOnFailure(t *testing.T) {
	s, _, fc := newTestService(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, Input{Name: "  ", PriceCents: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("create invalid: %v", err)
	}
	if _, err := s.Update(ctx, 1, Input{Name: "x", PriceCents: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("update invalid: %v", err)
	}
	if _, err := s.Update(ctx, 999, Input{Name: "x"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing: %v", err)
	}
	if fc.dels != 0 {
		t.Fatalf("failed writes must not invalidate (dels=%d)", fc.dels)
	}
}

func TestCacheDownFallsBackToDB(t *testing.T) {
	s, repo, fc := newTestService(t)
	fc.failGet, fc.failSet, fc.failDel = true, true, true
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		p, src, err := s.Get(ctx, 5)
		if err != nil || src != SourceDB || p.ID != 5 {
			t.Fatalf("get %d: %+v %v %v", i, p, src, err)
		}
	}
	if repo.Reads() != 3 {
		t.Fatalf("db reads = %d, want 3 (no cache)", repo.Reads())
	}
	// Writes still succeed with the cache down.
	if _, err := s.Update(ctx, 5, Input{Name: "ok", PriceCents: 1}); err != nil {
		t.Fatalf("update with cache down: %v", err)
	}
	if st := s.Stats(); st.CacheErrors == 0 {
		t.Fatalf("cache errors not counted: %+v", st)
	}
}

func TestCorruptCacheEntryIsDropped(t *testing.T) {
	s, repo, fc := newTestService(t)
	ctx := context.Background()
	fc.put(cacheKey(6), []byte("{not json"))
	p, src, err := s.Get(ctx, 6)
	if err != nil || src != SourceDB || p.ID != 6 {
		t.Fatalf("corrupt: %+v %v %v", p, src, err)
	}
	// The entry was replaced by a good one.
	if _, src, _ := s.Get(ctx, 6); src != SourceCache {
		t.Fatal("after repair the entry should be served from cache")
	}
	// An entry whose id does not match its key is also rejected.
	fc.put(cacheKey(7), []byte(`{"id":99,"name":"wrong"}`))
	p, src, err = s.Get(ctx, 7)
	if err != nil || src != SourceDB || p.ID != 7 {
		t.Fatalf("mismatched id: %+v %v %v", p, src, err)
	}
	if repo.Reads() != 2 {
		t.Fatalf("db reads = %d, want 2", repo.Reads())
	}
}

// gatedRepo blocks Get until released, so a test can pile up concurrent readers.
type gatedRepo struct {
	Repository
	started chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func (g *gatedRepo) Get(ctx context.Context, id int64) (Product, error) {
	if g.calls.Add(1) == 1 {
		close(g.started)
	}
	select {
	case <-g.release:
	case <-ctx.Done():
		return Product{}, ctx.Err()
	}
	return g.Repository.Get(ctx, id)
}

func TestConcurrentMissesAreCoalesced(t *testing.T) {
	base := NewMemoryRepo()
	base.Seed(5)
	g := &gatedRepo{Repository: base, started: make(chan struct{}), release: make(chan struct{})}
	s := NewService(g, newFakeCache(), Config{}, nil)

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	run := func() {
		defer wg.Done()
		p, _, err := s.Get(context.Background(), 2)
		if err == nil && p.ID != 2 {
			err = errors.New("wrong product")
		}
		errs <- err
	}
	wg.Add(1)
	go run()
	<-g.started // the leader is now inside the database call
	for i := 1; i < n; i++ {
		wg.Add(1)
		go run()
	}
	// Give the followers time to join the in-flight load, then let it finish.
	time.Sleep(100 * time.Millisecond)
	close(g.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if c := g.calls.Load(); c != 1 {
		t.Fatalf("database calls = %d, want 1 for %d concurrent misses", c, n)
	}
	if st := s.Stats(); st.DBReads != 1 || st.Coalesced < 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestLoadSurvivesCallerCancellation(t *testing.T) {
	base := NewMemoryRepo()
	base.Seed(5)
	g := &gatedRepo{Repository: base, started: make(chan struct{}), release: make(chan struct{})}
	fc := newFakeCache()
	s := NewService(g, fc, Config{}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := s.Get(ctx, 1)
		done <- err
	}()
	<-g.started
	cancel() // the request that started the load goes away mid-flight
	close(g.release)
	if err := <-done; err != nil {
		t.Fatalf("load should be independent of the caller's context, got %v", err)
	}
	if !fc.has(cacheKey(1)) {
		t.Fatal("result should still have been cached")
	}
}

func TestLoadTimeout(t *testing.T) {
	base := NewMemoryRepo()
	base.Seed(1)
	g := &gatedRepo{Repository: base, started: make(chan struct{}), release: make(chan struct{})}
	s := NewService(g, newFakeCache(), Config{LoadTimeout: 50 * time.Millisecond}, nil)
	start := time.Now()
	_, _, err := s.Get(context.Background(), 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("load did not honour its timeout")
	}
}

func TestDatabaseErrorIsNotCached(t *testing.T) {
	boom := errors.New("db exploded")
	repo := &errRepo{Repository: NewMemoryRepo(), err: boom}
	fc := newFakeCache()
	s := NewService(repo, fc, Config{}, nil)
	if _, _, err := s.Get(context.Background(), 1); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if fc.has(cacheKey(1)) {
		t.Fatal("a database error must never be cached")
	}
}

type errRepo struct {
	Repository
	err error
}

func (e *errRepo) Get(context.Context, int64) (Product, error) { return Product{}, e.err }

func TestListClamping(t *testing.T) {
	repo := NewMemoryRepo()
	repo.Seed(150)
	s := NewService(repo, NopCache{}, Config{}, nil)
	ctx := context.Background()
	cases := []struct {
		limit, offset int
		wantLen       int
		wantFirst     int64
	}{
		{0, 0, 20, 1},
		{-5, -3, 20, 1},
		{500, 0, 100, 1},
		{10, 145, 5, 146},
		{10, 1000, 0, 0},
	}
	for _, c := range cases {
		got, err := s.List(ctx, c.limit, c.offset)
		if err != nil || len(got) != c.wantLen {
			t.Fatalf("List(%d,%d): len=%d err=%v, want len %d", c.limit, c.offset, len(got), err, c.wantLen)
		}
		if c.wantLen > 0 && got[0].ID != c.wantFirst {
			t.Fatalf("List(%d,%d): first id %d, want %d", c.limit, c.offset, got[0].ID, c.wantFirst)
		}
	}
}

func TestNopCacheAlwaysHitsDB(t *testing.T) {
	repo := NewMemoryRepo()
	repo.Seed(3)
	s := NewService(repo, NopCache{}, Config{}, nil)
	for i := 0; i < 4; i++ {
		if _, src, err := s.Get(context.Background(), 1); err != nil || src != SourceDB {
			t.Fatalf("%v %v", src, err)
		}
	}
	if repo.Reads() != 4 {
		t.Fatalf("reads = %d", repo.Reads())
	}
	if st := s.Stats(); st.CacheNodes == nil || len(st.CacheNodes) != 0 {
		t.Fatalf("CacheNodes should be an empty (non-nil) slice for JSON, got %#v", st.CacheNodes)
	}
}

func TestCacheNode(t *testing.T) {
	s, _, _ := newTestService(t)
	if got := s.CacheNode(1); got != "node-x" {
		t.Fatalf("CacheNode = %q", got)
	}
}

func TestInputValidate(t *testing.T) {
	long := make([]rune, 201)
	for i := range long {
		long[i] = 'a'
	}
	bad := []Input{
		{Name: ""},
		{Name: "   "},
		{Name: string(long)},
		{Name: "ok", PriceCents: -1},
		{Name: "ok", Stock: -1},
		{Name: "ok", Description: string(make([]byte, 2001))},
	}
	for i, in := range bad {
		if err := in.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d: err = %v, want ErrInvalid", i, err)
		}
	}
	good := Input{Name: "  Widget  ", PriceCents: 0, Stock: 0}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
}
