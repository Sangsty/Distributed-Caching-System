package server

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"cachekit/resp"
	"cachekit/store"
)

// testConn is a tiny RESP client for talking to a test server.
type testConn struct {
	t  *testing.T
	nc net.Conn
	rd *resp.Reader
	wr *resp.Writer
}

func startServer(t *testing.T) (*Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(store.New(0), Options{})
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Close()
		if err := <-done; err != nil {
			t.Errorf("Serve returned %v after Close", err)
		}
	})
	return srv, ln.Addr().String()
}

func dial(t *testing.T, addr string) *testConn {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return &testConn{t: t, nc: nc, rd: resp.NewReader(nc), wr: resp.NewWriter(nc)}
}

func (c *testConn) do(args ...string) resp.Value {
	c.t.Helper()
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	c.wr.WriteArgs(bs...)
	if err := c.wr.Flush(); err != nil {
		c.t.Fatal(err)
	}
	c.nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	v, err := c.rd.ReadValue()
	if err != nil {
		c.t.Fatalf("%v: %v", args, err)
	}
	return v
}

func wantSimple(t *testing.T, v resp.Value, s string) {
	t.Helper()
	if v.Type != resp.Simple || string(v.Str) != s {
		t.Fatalf("got %c %q, want +%s", v.Type, v.Str, s)
	}
}
func wantInt(t *testing.T, v resp.Value, n int64) {
	t.Helper()
	if v.Type != resp.Integer || v.Int != n {
		t.Fatalf("got %c %d, want :%d", v.Type, v.Int, n)
	}
}
func wantBulk(t *testing.T, v resp.Value, s string) {
	t.Helper()
	if v.Type != resp.Bulk || v.Null || string(v.Str) != s {
		t.Fatalf("got %c null=%v %q, want bulk %q", v.Type, v.Null, v.Str, s)
	}
}
func wantErr(t *testing.T, v resp.Value, prefix string) {
	t.Helper()
	if v.Type != resp.Error || !strings.HasPrefix(string(v.Str), prefix) {
		t.Fatalf("got %c %q, want error starting %q", v.Type, v.Str, prefix)
	}
}

func TestBasicCommands(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)

	wantSimple(t, c.do("PING"), "PONG")
	wantBulk(t, c.do("PING", "hi"), "hi")
	wantBulk(t, c.do("ECHO", "yo"), "yo")

	if v := c.do("GET", "k"); v.Type != resp.Bulk || !v.Null {
		t.Fatalf("missing key should be a null bulk, got %+v", v)
	}
	wantSimple(t, c.do("SET", "k", "v1"), "OK")
	wantBulk(t, c.do("GET", "k"), "v1")
	wantSimple(t, c.do("set", "k", "v2"), "OK") // command names are case-insensitive
	wantBulk(t, c.do("get", "k"), "v2")
	wantInt(t, c.do("DBSIZE"), 1)

	wantSimple(t, c.do("SET", "empty", ""), "OK")
	wantBulk(t, c.do("GET", "empty"), "") // an empty value is not a miss

	wantInt(t, c.do("DEL", "k", "empty", "nope"), 2)
	wantInt(t, c.do("DBSIZE"), 0)
	wantSimple(t, c.do("SET", "a", "1"), "OK")
	wantSimple(t, c.do("FLUSHALL"), "OK")
	wantInt(t, c.do("DBSIZE"), 0)
}

func TestBinarySafeValues(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)
	val := "line1\r\nline2\x00\xff"
	wantSimple(t, c.do("SET", "bin", val), "OK")
	wantBulk(t, c.do("GET", "bin"), val)
}

func TestExpiryCommands(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)

	wantSimple(t, c.do("SET", "k", "v", "EX", "100"), "OK")
	if v := c.do("TTL", "k"); v.Type != resp.Integer || v.Int < 99 || v.Int > 100 {
		t.Fatalf("TTL = %+v", v)
	}
	wantSimple(t, c.do("SET", "p", "v", "PX", "50"), "OK")
	time.Sleep(120 * time.Millisecond)
	if v := c.do("GET", "p"); !v.Null {
		t.Fatal("PX key should have expired")
	}
	wantInt(t, c.do("TTL", "p"), -2)

	wantSimple(t, c.do("SET", "plain", "v"), "OK")
	wantInt(t, c.do("TTL", "plain"), -1)
	wantInt(t, c.do("EXPIRE", "plain", "100"), 1)
	if v := c.do("TTL", "plain"); v.Int < 99 {
		t.Fatalf("TTL after EXPIRE = %d", v.Int)
	}
	wantInt(t, c.do("EXPIRE", "missing", "10"), 0)
	wantInt(t, c.do("EXPIRE", "plain", "0"), 1) // non-positive deletes
	wantInt(t, c.do("TTL", "plain"), -2)
}

func TestErrors(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)
	wantErr(t, c.do("NOPE"), "ERR unknown command 'NOPE'")
	wantErr(t, c.do("GET"), "ERR wrong number of arguments for 'get'")
	wantErr(t, c.do("GET", "a", "b"), "ERR wrong number of arguments")
	wantErr(t, c.do("SET", "k"), "ERR wrong number of arguments for 'set'")
	wantErr(t, c.do("DEL"), "ERR wrong number of arguments")
	wantErr(t, c.do("SET", "k", "v", "EX"), "ERR syntax error")
	wantErr(t, c.do("SET", "k", "v", "EX", "abc"), "ERR value is not an integer")
	wantErr(t, c.do("SET", "k", "v", "EX", "0"), "ERR invalid expire time")
	wantErr(t, c.do("SET", "k", "v", "EX", "-5"), "ERR invalid expire time")
	wantErr(t, c.do("SET", "k", "v", "NX"), "ERR syntax error")
	wantErr(t, c.do("EXPIRE", "k", "x"), "ERR value is not an integer")
	// The connection must still work after errors.
	wantSimple(t, c.do("PING"), "PONG")
}

func TestToolingHandshakeCommands(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)
	if v := c.do("CONFIG", "GET", "save"); v.Type != resp.Array || len(v.Array) != 2 || string(v.Array[0].Str) != "save" {
		t.Fatalf("CONFIG GET save = %+v", v)
	}
	if v := c.do("CONFIG", "GET", "appendonly"); v.Type != resp.Array || len(v.Array) != 2 || string(v.Array[1].Str) != "no" {
		t.Fatalf("CONFIG GET appendonly = %+v", v)
	}
	if v := c.do("CONFIG", "GET", "whatever"); v.Type != resp.Array || len(v.Array) != 0 {
		t.Fatalf("CONFIG GET unknown = %+v", v)
	}
	if v := c.do("COMMAND"); v.Type != resp.Array {
		t.Fatalf("COMMAND = %+v", v)
	}
	wantErr(t, c.do("CONFIG", "SET", "a", "b"), "ERR")
}

func TestInfo(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)
	c.do("SET", "a", "1")
	c.do("GET", "a")
	c.do("GET", "missing")
	v := c.do("INFO")
	if v.Type != resp.Bulk {
		t.Fatalf("INFO = %+v", v)
	}
	info := string(v.Str)
	for _, want := range []string{"cachekit_version:", "connected_clients:1", "keyspace_hits:1", "keyspace_misses:1", "db0:keys=1"} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO missing %q:\n%s", want, info)
		}
	}
}

func TestInlineCommands(t *testing.T) {
	_, addr := startServer(t)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprint(nc, "SET color blue\r\nGET color\r\n")
	rd := resp.NewReader(nc)
	if v, _ := rd.ReadValue(); string(v.Str) != "OK" {
		t.Fatalf("inline SET reply %+v", v)
	}
	if v, _ := rd.ReadValue(); string(v.Str) != "blue" {
		t.Fatalf("inline GET reply %+v", v)
	}
}

func TestPipelining(t *testing.T) {
	srv, addr := startServer(t)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.SetDeadline(time.Now().Add(5 * time.Second))

	// 500 commands in a single write; replies must come back complete and in order.
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	const n = 500
	for i := 0; i < n; i++ {
		w.WriteArgs([]byte("SET"), []byte(fmt.Sprint("k", i)), []byte(fmt.Sprint("v", i)))
		w.WriteArgs([]byte("GET"), []byte(fmt.Sprint("k", i)))
	}
	w.Flush()
	if _, err := nc.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	rd := resp.NewReader(nc)
	for i := 0; i < n; i++ {
		if v, err := rd.ReadValue(); err != nil || string(v.Str) != "OK" {
			t.Fatalf("reply %d to SET: %+v %v", i, v, err)
		}
		if v, err := rd.ReadValue(); err != nil || string(v.Str) != fmt.Sprint("v", i) {
			t.Fatalf("reply %d to GET: %+v %v", i, v, err)
		}
	}
	if got := srv.commands.Load(); got != 2*n {
		t.Fatalf("commands processed = %d, want %d", got, 2*n)
	}
}

func TestQuitClosesConnection(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)
	wantSimple(t, c.do("QUIT"), "OK")
	c.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.rd.ReadValue(); err == nil {
		t.Fatal("connection should be closed after QUIT")
	}
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	_, addr := startServer(t)
	nc, _ := net.Dial("tcp", addr)
	defer nc.Close()
	nc.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprint(nc, "*1\r\n$abc\r\n")
	rd := resp.NewReader(nc)
	v, err := rd.ReadValue()
	if err != nil || v.Type != resp.Error || !strings.Contains(string(v.Str), "Protocol error") {
		t.Fatalf("got %+v %v", v, err)
	}
	if _, err := rd.ReadValue(); err == nil {
		t.Fatal("connection should close after a protocol error")
	}
}

func TestIdleTimeout(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	srv := New(store.New(0), Options{IdleTimeout: 100 * time.Millisecond})
	go srv.Serve(ln)
	defer srv.Close()
	nc, _ := net.Dial("tcp", ln.Addr().String())
	defer nc.Close()
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := nc.Read(buf); err == nil {
		t.Fatal("idle connection should have been closed by the server")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("server did not close the idle connection in time")
	}
}

func TestCloseDisconnectsClientsAndRejectsServe(t *testing.T) {
	srv, addr := startServer(t)
	c := dial(t, addr)
	wantSimple(t, c.do("PING"), "PONG")
	srv.Close()
	c.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	c.wr.WriteArgs([]byte("PING"))
	c.wr.Flush()
	if _, err := c.rd.ReadValue(); err == nil {
		t.Fatal("client should be disconnected after Close")
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	if err := srv.Serve(ln); err != ErrClosed {
		t.Fatalf("Serve after Close = %v, want ErrClosed", err)
	}
	if err := srv.Close(); err != nil { // second Close is a no-op
		t.Fatal(err)
	}
}

func TestManyConcurrentClients(t *testing.T) {
	_, addr := startServer(t)
	errs := make(chan error, 20)
	for g := 0; g < 20; g++ {
		go func(g int) {
			nc, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer nc.Close()
			nc.SetDeadline(time.Now().Add(5 * time.Second))
			rd, wr := resp.NewReader(nc), resp.NewWriter(nc)
			for i := 0; i < 200; i++ {
				k, v := fmt.Sprintf("g%d-%d", g, i), fmt.Sprintf("val-%d-%d", g, i)
				wr.WriteArgs([]byte("SET"), []byte(k), []byte(v))
				wr.WriteArgs([]byte("GET"), []byte(k))
				if err := wr.Flush(); err != nil {
					errs <- err
					return
				}
				if _, err := rd.ReadValue(); err != nil {
					errs <- err
					return
				}
				got, err := rd.ReadValue()
				if err != nil || string(got.Str) != v {
					errs <- fmt.Errorf("client %d key %s: %+v %v", g, k, got, err)
					return
				}
			}
			errs <- nil
		}(g)
	}
	for g := 0; g < 20; g++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
