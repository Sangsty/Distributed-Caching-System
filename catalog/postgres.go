package catalog

import (
	"context"
	"database/sql"
	"errors"
)

// PostgresRepo is the Repository backed by PostgreSQL. It uses only
// database/sql, so it works with any registered driver; cmd/demo registers pgx.
type PostgresRepo struct{ db *sql.DB }

// NewPostgres wraps an open *sql.DB.
func NewPostgres(db *sql.DB) *PostgresRepo { return &PostgresRepo{db: db} }

// The SQL is parameterised ($1...), never built by string concatenation.
const (
	productCols = "id, name, description, price_cents, stock, updated_at"

	sqlGet    = "SELECT " + productCols + " FROM products WHERE id = $1"
	sqlList   = "SELECT " + productCols + " FROM products ORDER BY id LIMIT $1 OFFSET $2"
	sqlCreate = "INSERT INTO products (name, description, price_cents, stock) VALUES ($1, $2, $3, $4) RETURNING " + productCols
	sqlUpdate = "UPDATE products SET name = $2, description = $3, price_cents = $4, stock = $5, updated_at = now() WHERE id = $1 RETURNING " + productCols
	sqlDelete = "DELETE FROM products WHERE id = $1"
)

type scanner interface{ Scan(dest ...any) error }

func scanProduct(s scanner) (Product, error) {
	var p Product
	err := s.Scan(&p.ID, &p.Name, &p.Description, &p.PriceCents, &p.Stock, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Product{}, ErrNotFound
	}
	return p, err
}

func (r *PostgresRepo) Get(ctx context.Context, id int64) (Product, error) {
	return scanProduct(r.db.QueryRowContext(ctx, sqlGet, id))
}

func (r *PostgresRepo) List(ctx context.Context, limit, offset int) ([]Product, error) {
	rows, err := r.db.QueryContext(ctx, sqlList, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Product{}
	for rows.Next() {
		p, err := scanProduct(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *PostgresRepo) Create(ctx context.Context, in Input) (Product, error) {
	return scanProduct(r.db.QueryRowContext(ctx, sqlCreate, in.Name, in.Description, in.PriceCents, in.Stock))
}

func (r *PostgresRepo) Update(ctx context.Context, id int64, in Input) (Product, error) {
	return scanProduct(r.db.QueryRowContext(ctx, sqlUpdate, id, in.Name, in.Description, in.PriceCents, in.Stock))
}

func (r *PostgresRepo) Delete(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx, sqlDelete, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
