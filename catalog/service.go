package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"time"

	"cachekit/flight"
)

// Source says where a read was answered from.
type Source string

const (
	SourceCache Source = "cache"
	SourceDB    Source = "db"
)

// notFoundMarker is cached for ids that do not exist ("negative caching"), so
// repeated lookups of a missing id do not each hit the database.
var notFoundMarker = []byte("!nf")

// Config tunes the Service. Zero values select the defaults noted below.
type Config struct {
	TTL         time.Duration // lifetime of a cached product (default 60s)
	NegativeTTL time.Duration // lifetime of a cached "does not exist" (default 15s)
	TTLJitter   float64       // ± fraction applied to TTL so keys do not all expire together (default 0.1)
	LoadTimeout time.Duration // cap on one database load (default 3s)
}

func (c *Config) defaults() {
	if c.TTL <= 0 {
		c.TTL = 60 * time.Second
	}
	if c.NegativeTTL <= 0 {
		c.NegativeTTL = 15 * time.Second
	}
	if c.TTLJitter <= 0 || c.TTLJitter >= 1 {
		c.TTLJitter = 0.1
	}
	if c.LoadTimeout <= 0 {
		c.LoadTimeout = 3 * time.Second
	}
}

// Service implements the cache-aside pattern for products:
//
//	read:  cache hit -> return.  miss -> load from the database (one load per key
//	       at a time, see flight) -> store in the cache -> return.
//	write: update the database first, then delete the cached copy; the next read
//	       reloads it.
//
// The cache is an optimisation, never a dependency: if a cache node is down or
// slow, reads fall back to the database and writes still succeed.
type Service struct {
	repo   Repository
	cache  Cache
	cfg    Config
	flight flight.Group
	log    *slog.Logger

	hits, negHits, misses, cacheErrors atomic.Uint64
	dbReads, coalesced, invalidations  atomic.Uint64
	lastWarn                           atomic.Int64 // unix nanos of the last cache warning (rate limit)
}

// NewService creates a Service. log may be nil.
func NewService(repo Repository, cache Cache, cfg Config, log *slog.Logger) *Service {
	cfg.defaults()
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Service{repo: repo, cache: cache, cfg: cfg, log: log}
}

func cacheKey(id int64) string { return "product:" + strconv.FormatInt(id, 10) }

// jitter spreads a TTL by ±cfg.TTLJitter, so a batch of keys cached at the same
// moment does not expire (and stampede the database) at the same moment.
func (s *Service) jitter(ttl time.Duration) time.Duration {
	f := 1 + (rand.Float64()*2-1)*s.cfg.TTLJitter
	return time.Duration(float64(ttl) * f)
}

// warnCache logs a cache problem at most once every 5 seconds, so a dead node
// does not turn every request into a log line.
func (s *Service) warnCache(op string, err error) {
	now := time.Now().UnixNano()
	last := s.lastWarn.Load()
	if now-last < int64(5*time.Second) || !s.lastWarn.CompareAndSwap(last, now) {
		return
	}
	s.log.Warn("cache unavailable, falling back to the database", "op", op, "err", err)
}

// Get returns a product and where it was served from. A missing product is
// ErrNotFound (and is itself cached briefly).
func (s *Service) Get(ctx context.Context, id int64) (Product, Source, error) {
	key := cacheKey(id)

	// fill is false when the cache just failed us: writing to a node that could not
	// answer a read would only cost a second timeout for no benefit.
	fill := true
	b, found, err := s.cache.Get(key)
	switch {
	case err != nil:
		fill = false
		s.cacheErrors.Add(1)
		s.warnCache("get", err)
	case found && bytes.Equal(b, notFoundMarker):
		s.negHits.Add(1)
		return Product{}, SourceCache, ErrNotFound
	case found:
		var p Product
		if json.Unmarshal(b, &p) == nil && p.ID == id {
			s.hits.Add(1)
			return p, SourceCache, nil
		}
		// Unreadable or mismatched entry: drop it and treat it as a miss.
		s.misses.Add(1)
		s.invalidate(key)
	default:
		s.misses.Add(1)
	}

	v, err, coalesced := s.flight.Do(key, func() (any, error) { return s.load(ctx, id, key, fill) })
	if coalesced {
		s.coalesced.Add(1)
	}
	if err != nil {
		return Product{}, SourceDB, err
	}
	return v.(Product), SourceDB, nil
}

// load reads one product from the database and fills the cache. It runs under
// flight.Do, shared by every request waiting on the same key, so it must not
// depend on any single caller's context: if the request that started the load
// is cancelled, the others still need the result. It gets its own timeout.
func (s *Service) load(ctx context.Context, id int64, key string, fill bool) (any, error) {
	lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.LoadTimeout)
	defer cancel()

	s.dbReads.Add(1)
	p, err := s.repo.Get(lctx, id)
	switch {
	case errors.Is(err, ErrNotFound):
		if fill {
			s.cacheSet(key, notFoundMarker, s.cfg.NegativeTTL)
		}
		return nil, ErrNotFound
	case err != nil:
		return nil, err
	}
	if fill {
		if b, err := json.Marshal(p); err == nil {
			s.cacheSet(key, b, s.jitter(s.cfg.TTL))
		}
	}
	return p, nil
}

func (s *Service) cacheSet(key string, val []byte, ttl time.Duration) {
	if err := s.cache.Set(key, val, ttl); err != nil {
		s.cacheErrors.Add(1)
		s.warnCache("set", err)
	}
}

// invalidate deletes a cached key. If the node is unreachable the stale copy
// lives on until its TTL expires; the TTL is what bounds staleness.
func (s *Service) invalidate(key string) {
	s.invalidations.Add(1)
	if _, err := s.cache.Del(key); err != nil {
		s.cacheErrors.Add(1)
		s.warnCache("del", err)
	}
}

// CacheNode returns the ID of the cache node that owns this product's key.
func (s *Service) CacheNode(id int64) string {
	node, _ := s.cache.Owner(cacheKey(id))
	return node
}

// List returns a page of products straight from the database (list pages are
// not cached). limit is clamped to 1..100 (default 20), offset to >= 0.
func (s *Service) List(ctx context.Context, limit, offset int) ([]Product, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	return s.repo.List(ctx, limit, offset)
}

// Create inserts a product. The new id's cache key is invalidated in case a
// negative entry ("id does not exist") was cached for it earlier.
func (s *Service) Create(ctx context.Context, in Input) (Product, error) {
	if err := in.Validate(); err != nil {
		return Product{}, err
	}
	p, err := s.repo.Create(ctx, in)
	if err != nil {
		return Product{}, err
	}
	s.invalidate(cacheKey(p.ID))
	return p, nil
}

// Update changes a product: database first, then invalidate the cached copy.
func (s *Service) Update(ctx context.Context, id int64, in Input) (Product, error) {
	if err := in.Validate(); err != nil {
		return Product{}, err
	}
	p, err := s.repo.Update(ctx, id, in)
	if err != nil {
		return Product{}, err
	}
	s.invalidate(cacheKey(id))
	return p, nil
}

// Delete removes a product and its cached copy.
func (s *Service) Delete(ctx context.Context, id int64) error {
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	s.invalidate(cacheKey(id))
	return nil
}

// Stats is a snapshot of the service's counters. Every Get lands in exactly one
// of hits / negative_hits / misses / cache_errors.
type Stats struct {
	CacheHits     uint64   `json:"cache_hits"`
	NegativeHits  uint64   `json:"cache_negative_hits"`
	CacheMisses   uint64   `json:"cache_misses"`
	CacheErrors   uint64   `json:"cache_errors"` // failed cache calls of any kind (get, set, del)
	DBReads       uint64   `json:"db_reads"`     // product loads that reached the database
	Coalesced     uint64   `json:"coalesced_loads"`
	Invalidations uint64   `json:"invalidations"`
	HitRatio      float64  `json:"hit_ratio"` // (hits + negative hits) / all Gets that reached the cache
	CacheNodes    []string `json:"cache_nodes"`
	RingVersion   uint64   `json:"ring_version"`
}

// Stats returns the current counters.
func (s *Service) Stats() Stats {
	st := Stats{
		CacheHits: s.hits.Load(), NegativeHits: s.negHits.Load(), CacheMisses: s.misses.Load(),
		CacheErrors: s.cacheErrors.Load(), DBReads: s.dbReads.Load(), Coalesced: s.coalesced.Load(),
		Invalidations: s.invalidations.Load(), CacheNodes: s.cache.Nodes(), RingVersion: s.cache.Version(),
	}
	if st.CacheNodes == nil {
		st.CacheNodes = []string{}
	}
	if total := st.CacheHits + st.NegativeHits + st.CacheMisses; total > 0 {
		st.HitRatio = float64(st.CacheHits+st.NegativeHits) / float64(total)
	}
	return st
}
