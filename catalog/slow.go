package catalog

import (
	"context"
	"time"
)

// WithLatency wraps a Repository and delays every call by d. It exists for
// demos and benchmarks: a database on localhost answers in about a millisecond,
// which hides what a cache is for. Report any numbers measured with it as
// "simulated DB latency".
func WithLatency(r Repository, d time.Duration) Repository {
	if d <= 0 {
		return r
	}
	return slowRepo{r: r, d: d}
}

type slowRepo struct {
	r Repository
	d time.Duration
}

func (s slowRepo) wait(ctx context.Context) error {
	t := time.NewTimer(s.d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s slowRepo) Get(ctx context.Context, id int64) (Product, error) {
	if err := s.wait(ctx); err != nil {
		return Product{}, err
	}
	return s.r.Get(ctx, id)
}

func (s slowRepo) List(ctx context.Context, limit, offset int) ([]Product, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.r.List(ctx, limit, offset)
}

func (s slowRepo) Create(ctx context.Context, in Input) (Product, error) {
	if err := s.wait(ctx); err != nil {
		return Product{}, err
	}
	return s.r.Create(ctx, in)
}

func (s slowRepo) Update(ctx context.Context, id int64, in Input) (Product, error) {
	if err := s.wait(ctx); err != nil {
		return Product{}, err
	}
	return s.r.Update(ctx, id, in)
}

func (s slowRepo) Delete(ctx context.Context, id int64) error {
	if err := s.wait(ctx); err != nil {
		return err
	}
	return s.r.Delete(ctx, id)
}
