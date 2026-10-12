package catalog

import "context"

// Repository is the source of truth for products. Implementations return
// ErrNotFound when a product does not exist.
type Repository interface {
	Get(ctx context.Context, id int64) (Product, error)
	List(ctx context.Context, limit, offset int) ([]Product, error)
	Create(ctx context.Context, in Input) (Product, error)
	Update(ctx context.Context, id int64, in Input) (Product, error)
	Delete(ctx context.Context, id int64) error
}
