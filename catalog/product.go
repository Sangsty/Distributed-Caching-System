// Package catalog is the demo application's domain: a product catalog stored in
// a database (the source of truth) and fronted by the distributed cache using
// the cache-aside pattern (see Service).
package catalog

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// Errors returned by the package. Their text is shown to API clients, so it
// carries no internal detail.
var (
	ErrNotFound = errors.New("product not found")
	ErrInvalid  = errors.New("invalid product")
)

// Product is one catalog row.
type Product struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	PriceCents  int64     `json:"price_cents"`
	Stock       int       `json:"stock"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Input is the writable part of a product (create and update).
type Input struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	PriceCents  int64  `json:"price_cents"`
	Stock       int    `json:"stock"`
}

const (
	maxNameLen = 200
	maxDescLen = 2000
)

// Validate checks the input and trims the name. The limits mirror the
// database's constraints, so bad input is rejected with a clear 400 before it
// can become a database error.
func (in *Input) Validate() error {
	in.Name = strings.TrimSpace(in.Name)
	switch {
	case in.Name == "":
		return fmt.Errorf("%w: name is required", ErrInvalid)
	case utf8.RuneCountInString(in.Name) > maxNameLen:
		return fmt.Errorf("%w: name is longer than %d characters", ErrInvalid, maxNameLen)
	case utf8.RuneCountInString(in.Description) > maxDescLen:
		return fmt.Errorf("%w: description is longer than %d characters", ErrInvalid, maxDescLen)
	case in.PriceCents < 0:
		return fmt.Errorf("%w: price_cents must not be negative", ErrInvalid)
	case in.Stock < 0:
		return fmt.Errorf("%w: stock must not be negative", ErrInvalid)
	case in.Stock > math.MaxInt32: // the column is a 32-bit INTEGER
		return fmt.Errorf("%w: stock is too large", ErrInvalid)
	}
	return nil
}
