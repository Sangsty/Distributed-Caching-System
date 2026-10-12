// Package store is the in-memory storage engine of a cache node: a
// size-bounded LRU map with per-key TTL.
//
// Expiry is lazy (an expired key is dropped when it is next touched) plus a
// background janitor that samples the table so keys nobody reads again do not
// sit in memory forever. Values are stored as given; callers must not modify a
// slice after passing it to Set, nor the slice returned by Get.
package store

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	key string
	val []byte
	exp int64 // unix nanoseconds; 0 means no expiry
}

// Stats is a snapshot of the store's counters.
type Stats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64 // removed because the store was full
	Expired   uint64 // removed because their TTL elapsed
	Items     int
}

// Store is a concurrency-safe LRU cache with TTLs.
type Store struct {
	mu    sync.Mutex
	max   int // <= 0 means unlimited
	ll    *list.List
	items map[string]*list.Element
	now   func() time.Time // replaceable in tests

	hits, misses, evictions, expired uint64
}

// New creates a store holding at most maxItems keys (<= 0 means unlimited).
// When full, the least recently used key is evicted.
func New(maxItems int) *Store {
	return &Store{
		max:   maxItems,
		ll:    list.New(),
		items: make(map[string]*list.Element),
		now:   time.Now,
	}
}

func (s *Store) isExpired(e *entry) bool {
	return e.exp != 0 && s.now().UnixNano() >= e.exp
}

// removeLocked drops an element from both structures.
func (s *Store) removeLocked(el *list.Element) {
	s.ll.Remove(el)
	delete(s.items, el.Value.(*entry).key)
}

// liveLocked returns the element for key if it exists and has not expired.
// An expired element is removed on the spot (lazy expiry).
func (s *Store) liveLocked(key string) (*list.Element, bool) {
	el, ok := s.items[key]
	if !ok {
		return nil, false
	}
	if s.isExpired(el.Value.(*entry)) {
		s.removeLocked(el)
		s.expired++
		return nil, false
	}
	return el, true
}

// Get returns the value for key and marks it most recently used.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.liveLocked(key)
	if !ok {
		s.misses++
		return nil, false
	}
	s.ll.MoveToFront(el)
	s.hits++
	return el.Value.(*entry).val, true
}

// Set stores val under key. ttl <= 0 means the key never expires. An existing
// key has its value and TTL replaced.
func (s *Store) Set(key string, val []byte, ttl time.Duration) {
	var exp int64
	s.mu.Lock()
	defer s.mu.Unlock()
	if ttl > 0 {
		exp = s.now().Add(ttl).UnixNano()
	}
	if el, ok := s.items[key]; ok {
		e := el.Value.(*entry)
		e.val, e.exp = val, exp
		s.ll.MoveToFront(el)
		return
	}
	s.items[key] = s.ll.PushFront(&entry{key: key, val: val, exp: exp})
	if s.max > 0 && s.ll.Len() > s.max {
		s.removeLocked(s.ll.Back())
		s.evictions++
	}
}

// Del removes key and reports whether a live key was removed.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.liveLocked(key)
	if !ok {
		return false
	}
	s.removeLocked(el)
	return true
}

// Expire sets a new TTL on an existing key and reports whether the key
// existed. ttl <= 0 deletes the key (same as Redis EXPIRE with a
// non-positive value).
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.liveLocked(key)
	if !ok {
		return false
	}
	if ttl <= 0 {
		s.removeLocked(el)
		return true
	}
	el.Value.(*entry).exp = s.now().Add(ttl).UnixNano()
	return true
}

// TTL returns the remaining time to live. exists is false for a missing or
// expired key; hasTTL is false for a key without expiry.
func (s *Store) TTL(key string) (remaining time.Duration, exists, hasTTL bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.liveLocked(key)
	if !ok {
		return 0, false, false
	}
	e := el.Value.(*entry)
	if e.exp == 0 {
		return 0, true, false
	}
	return time.Duration(e.exp - s.now().UnixNano()), true, true
}

// Len returns the number of stored keys. It may include expired keys that the
// janitor has not swept yet.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ll.Len()
}

// Flush removes every key. Counters are kept.
func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ll.Init()
	s.items = make(map[string]*list.Element)
}

// Stats returns a snapshot of the counters.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{Hits: s.hits, Misses: s.misses, Evictions: s.evictions, Expired: s.expired, Items: s.ll.Len()}
}

// Sweep examines up to limit keys (limit <= 0 means all) and removes the
// expired ones. It returns how many it removed. Bounded sampling keeps the
// lock hold time small on big tables.
func (s *Store) Sweep(limit int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed, seen := 0, 0
	for _, el := range s.items { // Go's map iteration order is randomised, so repeated sweeps cover different keys
		if limit > 0 && seen >= limit {
			break
		}
		seen++
		if s.isExpired(el.Value.(*entry)) {
			s.removeLocked(el) // deleting during range is safe in Go
			s.expired++
			removed++
		}
	}
	return removed
}

// StartJanitor sweeps up to sample keys every interval until the returned
// stop function is called. stop blocks until the janitor goroutine has exited.
func (s *Store) StartJanitor(interval time.Duration, sample int) (stop func()) {
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.Sweep(sample)
			case <-done:
				return
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-exited
	}
}
