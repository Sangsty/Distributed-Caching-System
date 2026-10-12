//go:build pgintegration

package catalog

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Opt-in check of PostgresRepo against a real database with the schema from
// deploy/postgres/init.sql. With the compose stack running:
//
//	$env:TEST_DATABASE_URL = "postgres://cachekit:cachekit@localhost:5433/catalog?sslmode=disable"
//	go test -tags pgintegration -run Integration -v ./catalog
//
// It creates and then deletes its own rows and leaves the seed data alone.
func TestPostgresIntegration(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("cannot reach the database: %v", err)
	}
	repo := NewPostgres(db)

	if _, err := repo.Get(ctx, 1<<40); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing id: %v, want ErrNotFound", err)
	}

	created, err := repo.Create(ctx, Input{Name: "it-test", Description: "d", PriceCents: 123, Stock: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Delete(context.Background(), created.ID) // cleanup even if the test fails below
	if created.ID == 0 || created.Name != "it-test" || created.PriceCents != 123 || created.UpdatedAt.IsZero() {
		t.Fatalf("created: %+v", created)
	}

	got, err := repo.Get(ctx, created.ID)
	if err != nil || got.ID != created.ID || got.Stock != 4 {
		t.Fatalf("get: %+v %v", got, err)
	}

	time.Sleep(10 * time.Millisecond)
	upd, err := repo.Update(ctx, created.ID, Input{Name: "it-test-2", PriceCents: 456, Stock: 5})
	if err != nil || upd.Name != "it-test-2" || upd.PriceCents != 456 || !upd.UpdatedAt.After(created.UpdatedAt) {
		t.Fatalf("update: %+v %v", upd, err)
	}
	if _, err := repo.Update(ctx, 1<<40, Input{Name: "x"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing: %v", err)
	}

	page, err := repo.List(ctx, 5, 0)
	if err != nil || len(page) == 0 || len(page) > 5 {
		t.Fatalf("list: %d %v", len(page), err)
	}
	for i := 1; i < len(page); i++ {
		if page[i].ID <= page[i-1].ID {
			t.Fatal("list is not ordered by id")
		}
	}

	// The CHECK constraints are the last line of defence behind Validate.
	if _, err := repo.Create(ctx, Input{Name: "bad", PriceCents: -1}); err == nil {
		t.Fatal("negative price should violate the CHECK constraint")
	}

	if err := repo.Delete(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}
