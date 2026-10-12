package catalog

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryRepo is an in-memory Repository. The tests use it, and
// `demo -store memory` uses it to run the whole stack without a database.
type MemoryRepo struct {
	mu     sync.RWMutex
	items  map[int64]Product
	nextID int64
	reads  atomic.Int64
}

// NewMemoryRepo returns an empty repository; IDs start at 1.
func NewMemoryRepo() *MemoryRepo {
	return &MemoryRepo{items: make(map[int64]Product), nextID: 1}
}

// Seed adds n products with the same shape as deploy/postgres/init.sql.
func (m *MemoryRepo) Seed(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for g := 1; g <= n; g++ {
		id := m.nextID
		m.nextID++
		m.items[id] = Product{
			ID:          id,
			Name:        fmt.Sprintf("Product %d", g),
			Description: fmt.Sprintf("Description of product %d", g),
			PriceCents:  int64(100 + (g*37)%9900),
			Stock:       g % 50,
			UpdatedAt:   time.Now().UTC(),
		}
	}
}

// Reads returns how many Get calls reached this repository, which is the number
// of "database reads" the cache failed to absorb.
func (m *MemoryRepo) Reads() int64 { return m.reads.Load() }

func (m *MemoryRepo) Get(ctx context.Context, id int64) (Product, error) {
	m.reads.Add(1)
	if err := ctx.Err(); err != nil {
		return Product{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.items[id]
	if !ok {
		return Product{}, ErrNotFound
	}
	return p, nil
}

func (m *MemoryRepo) List(ctx context.Context, limit, offset int) ([]Product, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	ids := make([]int64, 0, len(m.items))
	for id := range m.items {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := []Product{}
	for i := offset; i < len(ids) && len(out) < limit; i++ {
		out = append(out, m.items[ids[i]])
	}
	m.mu.RUnlock()
	return out, nil
}

func (m *MemoryRepo) Create(ctx context.Context, in Input) (Product, error) {
	if err := ctx.Err(); err != nil {
		return Product{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	p := Product{ID: m.nextID, Name: in.Name, Description: in.Description, PriceCents: in.PriceCents, Stock: in.Stock, UpdatedAt: time.Now().UTC()}
	m.nextID++
	m.items[p.ID] = p
	return p, nil
}

func (m *MemoryRepo) Update(ctx context.Context, id int64, in Input) (Product, error) {
	if err := ctx.Err(); err != nil {
		return Product{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.items[id]
	if !ok {
		return Product{}, ErrNotFound
	}
	p.Name, p.Description, p.PriceCents, p.Stock, p.UpdatedAt = in.Name, in.Description, in.PriceCents, in.Stock, time.Now().UTC()
	m.items[id] = p
	return p, nil
}

func (m *MemoryRepo) Delete(ctx context.Context, id int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.items[id]; !ok {
		return ErrNotFound
	}
	delete(m.items, id)
	return nil
}
