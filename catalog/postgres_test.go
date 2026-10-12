package catalog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file tests PostgresRepo's Go side (argument order, scanning, error
// mapping) against a scripted fake database/sql driver, so it runs anywhere.
// The SQL text itself was checked against a real PostgreSQL 16; for a check
// against your own database run the opt-in test:
//
//	TEST_DATABASE_URL=postgres://... go test -tags pgintegration ./catalog

type fakeResult struct {
	cols     []string
	rows     [][]driver.Value
	affected int64
	err      error
}

type fakeCall struct {
	query string
	args  []driver.Value
}

type fakeDB struct {
	mu      sync.Mutex
	calls   []fakeCall
	handler func(query string, args []driver.Value) fakeResult
}

func (f *fakeDB) Open(string) (driver.Conn, error) { return &fakeConn{f}, nil }

type fakeConn struct{ db *fakeDB }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return nil, errors.New("tx unsupported") }

func (c *fakeConn) run(query string, nv []driver.NamedValue) fakeResult {
	args := make([]driver.Value, len(nv))
	for i, a := range nv {
		args[i] = a.Value
	}
	c.db.mu.Lock()
	c.db.calls = append(c.db.calls, fakeCall{query, args})
	c.db.mu.Unlock()
	return c.db.handler(query, args)
}

func (c *fakeConn) QueryContext(_ context.Context, query string, nv []driver.NamedValue) (driver.Rows, error) {
	r := c.run(query, nv)
	if r.err != nil {
		return nil, r.err
	}
	return &fakeRows{cols: r.cols, rows: r.rows}, nil
}

func (c *fakeConn) ExecContext(_ context.Context, query string, nv []driver.NamedValue) (driver.Result, error) {
	r := c.run(query, nv)
	if r.err != nil {
		return nil, r.err
	}
	return driver.RowsAffected(r.affected), nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

var productColNames = strings.Split(strings.ReplaceAll(productCols, " ", ""), ",")

func newFakePG(t *testing.T, h func(string, []driver.Value) fakeResult) (*PostgresRepo, *fakeDB) {
	t.Helper()
	f := &fakeDB{handler: h}
	db := sql.OpenDB(connector{f})
	t.Cleanup(func() { db.Close() })
	return NewPostgres(db), f
}

type connector struct{ f *fakeDB }

func (c connector) Connect(context.Context) (driver.Conn, error) { return &fakeConn{c.f}, nil }
func (c connector) Driver() driver.Driver                        { return c.f }

var ts = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func productRow(id int64, name string) []driver.Value {
	return []driver.Value{id, name, "desc " + name, int64(250), int64(7), ts}
}

func TestPostgresGet(t *testing.T) {
	repo, f := newFakePG(t, func(q string, a []driver.Value) fakeResult {
		if a[0] == int64(5) {
			return fakeResult{cols: productColNames, rows: [][]driver.Value{productRow(5, "five")}}
		}
		return fakeResult{cols: productColNames} // no rows
	})
	p, err := repo.Get(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	want := Product{ID: 5, Name: "five", Description: "desc five", PriceCents: 250, Stock: 7, UpdatedAt: ts}
	if p != want {
		t.Fatalf("got %+v, want %+v", p, want)
	}
	if f.calls[0].query != sqlGet {
		t.Fatalf("query = %q", f.calls[0].query)
	}
	if _, err := repo.Get(context.Background(), 6); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing row: %v, want ErrNotFound", err)
	}
}

func TestPostgresGetPropagatesDBError(t *testing.T) {
	boom := errors.New("connection refused")
	repo, _ := newFakePG(t, func(string, []driver.Value) fakeResult { return fakeResult{err: boom} })
	if _, err := repo.Get(context.Background(), 1); !errors.Is(err, boom) || errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestPostgresList(t *testing.T) {
	repo, f := newFakePG(t, func(string, []driver.Value) fakeResult {
		return fakeResult{cols: productColNames, rows: [][]driver.Value{productRow(1, "a"), productRow(2, "b")}}
	})
	got, err := repo.List(context.Background(), 10, 40)
	if err != nil || len(got) != 2 || got[1].Name != "b" {
		t.Fatalf("list: %+v %v", got, err)
	}
	if f.calls[0].query != sqlList || f.calls[0].args[0] != int64(10) || f.calls[0].args[1] != int64(40) {
		t.Fatalf("call = %+v (limit must be $1, offset $2)", f.calls[0])
	}

	empty, _ := newFakePG(t, func(string, []driver.Value) fakeResult { return fakeResult{cols: productColNames} })
	got, err = empty.List(context.Background(), 5, 0)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty list should be a non-nil empty slice (JSON [] not null): %#v %v", got, err)
	}
}

func TestPostgresCreateAndUpdateArgOrder(t *testing.T) {
	repo, f := newFakePG(t, func(string, []driver.Value) fakeResult {
		return fakeResult{cols: productColNames, rows: [][]driver.Value{productRow(9, "n")}}
	})
	in := Input{Name: "n", Description: "d", PriceCents: 300, Stock: 4}

	if _, err := repo.Create(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	c := f.calls[0]
	if c.query != sqlCreate || len(c.args) != 4 || c.args[0] != "n" || c.args[1] != "d" || c.args[2] != int64(300) || c.args[3] != int64(4) {
		t.Fatalf("create call: %+v", c)
	}

	if _, err := repo.Update(context.Background(), 9, in); err != nil {
		t.Fatal(err)
	}
	u := f.calls[1]
	// $1 is the id in the update statement, then name, description, price, stock.
	if u.query != sqlUpdate || len(u.args) != 5 || u.args[0] != int64(9) || u.args[1] != "n" || u.args[2] != "d" || u.args[3] != int64(300) || u.args[4] != int64(4) {
		t.Fatalf("update call: %+v", u)
	}
}

func TestPostgresUpdateMissingRow(t *testing.T) {
	repo, _ := newFakePG(t, func(string, []driver.Value) fakeResult { return fakeResult{cols: productColNames} })
	if _, err := repo.Update(context.Background(), 1, Input{Name: "x"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestPostgresDelete(t *testing.T) {
	repo, f := newFakePG(t, func(q string, a []driver.Value) fakeResult {
		if a[0] == int64(1) {
			return fakeResult{affected: 1}
		}
		return fakeResult{affected: 0}
	})
	if err := repo.Delete(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if f.calls[0].query != sqlDelete {
		t.Fatalf("query = %q", f.calls[0].query)
	}
	if err := repo.Delete(context.Background(), 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleting a missing row: %v, want ErrNotFound", err)
	}
	failing, _ := newFakePG(t, func(string, []driver.Value) fakeResult { return fakeResult{err: errors.New("fk violation")} })
	if err := failing.Delete(context.Background(), 1); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("db error should pass through, got %v", err)
	}
}

func TestSQLUsesOnlyPlaceholders(t *testing.T) {
	// Guard against someone later formatting values into the SQL text.
	for _, q := range []string{sqlGet, sqlList, sqlCreate, sqlUpdate, sqlDelete} {
		if strings.Contains(q, "%") || strings.Contains(q, "'") {
			t.Errorf("suspicious SQL: %s", q)
		}
	}
}
