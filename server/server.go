// Package server is the TCP front end of a cache node. It speaks RESP, so
// redis-cli and redis-benchmark work against it, and it implements this
// command subset:
//
//	PING [msg]            ECHO msg
//	GET key               SET key value [EX seconds | PX milliseconds]
//	DEL key [key ...]     EXPIRE key seconds     TTL key
//	DBSIZE                FLUSHALL / FLUSHDB
//	INFO                  CONFIG GET save|appendonly / COMMAND (stubs for redis-cli/benchmark)
//	QUIT
//
// Each connection is served by its own goroutine. Replies are flushed only when
// the client has no more pipelined input buffered, so pipelining is cheap.
package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cachekit/resp"
	"cachekit/store"
)

// Version is reported by INFO.
const Version = "0.1.0"

// ErrClosed is returned by Serve when the server was already closed.
var ErrClosed = errors.New("server: closed")

// Options tunes a Server. Zero values select the defaults noted below.
type Options struct {
	IdleTimeout  time.Duration // close connections silent for this long (default 5m)
	WriteTimeout time.Duration // per-command reply deadline (default 10s)
	Logger       *slog.Logger  // default: discard
}

// Server serves one store over RESP.
type Server struct {
	st   *store.Store
	opts Options

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup

	started    time.Time
	commands   atomic.Uint64
	totalConns atomic.Uint64
}

// New creates a server for st.
func New(st *store.Store, opts Options) *Server {
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = 5 * time.Minute
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 10 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	return &Server{st: st, opts: opts, conns: make(map[net.Conn]struct{}), started: time.Now()}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// Serve accepts connections on ln until Close is called (returns nil) or the
// listener fails (returns the error).
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.ln = ln
	s.mu.Unlock()

	for {
		c, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		// Registering under the lock, after the closed check, guarantees Close
		// either sees this connection or this loop sees closed == true.
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
			return nil
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		s.totalConns.Add(1)
		go s.handle(c)
	}
}

// Close stops accepting, closes every client connection and waits for the
// handlers to finish.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.ln != nil {
		s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		c.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
	}()

	rd, wr := resp.NewReader(c), resp.NewWriter(c)
	for {
		now := time.Now()
		c.SetReadDeadline(now.Add(s.opts.IdleTimeout))
		c.SetWriteDeadline(now.Add(s.opts.WriteTimeout))

		args, err := rd.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				wr.WriteError("ERR Protocol error: " + err.Error())
				wr.Flush()
			}
			return // EOF, reset, idle timeout, or protocol error: drop the connection
		}

		quit := s.exec(args, wr)
		s.commands.Add(1)

		// Pipelining: only flush once the client's batch of commands is used up.
		if quit || rd.Buffered() == 0 {
			if err := wr.Flush(); err != nil {
				return
			}
		}
		if quit {
			return
		}
	}
}

func wrongArgs(wr *resp.Writer, name string) {
	wr.WriteError("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
}

// exec runs one command and writes its reply. It returns true for QUIT.
func (s *Server) exec(args [][]byte, wr *resp.Writer) (quit bool) {
	name := strings.ToUpper(string(args[0]))
	switch name {
	case "PING":
		switch len(args) {
		case 1:
			wr.WriteSimple("PONG")
		case 2:
			wr.WriteBulk(args[1])
		default:
			wrongArgs(wr, name)
		}

	case "ECHO":
		if len(args) != 2 {
			wrongArgs(wr, name)
			return
		}
		wr.WriteBulk(args[1])

	case "GET":
		if len(args) != 2 {
			wrongArgs(wr, name)
			return
		}
		if v, ok := s.st.Get(string(args[1])); ok {
			wr.WriteBulk(v)
		} else {
			wr.WriteNull()
		}

	case "SET":
		if len(args) < 3 {
			wrongArgs(wr, name)
			return
		}
		ttl, errMsg := parseSetOptions(args[3:])
		if errMsg != "" {
			wr.WriteError(errMsg)
			return
		}
		// args[2] is a fresh slice owned by us (see resp.Reader.ReadCommand).
		s.st.Set(string(args[1]), args[2], ttl)
		wr.WriteSimple("OK")

	case "DEL":
		if len(args) < 2 {
			wrongArgs(wr, name)
			return
		}
		var n int64
		for _, k := range args[1:] {
			if s.st.Del(string(k)) {
				n++
			}
		}
		wr.WriteInt(n)

	case "EXPIRE":
		if len(args) != 3 {
			wrongArgs(wr, name)
			return
		}
		secs, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			wr.WriteError("ERR value is not an integer or out of range")
			return
		}
		if s.st.Expire(string(args[1]), time.Duration(secs)*time.Second) {
			wr.WriteInt(1)
		} else {
			wr.WriteInt(0)
		}

	case "TTL":
		if len(args) != 2 {
			wrongArgs(wr, name)
			return
		}
		rem, exists, hasTTL := s.st.TTL(string(args[1]))
		switch {
		case !exists:
			wr.WriteInt(-2)
		case !hasTTL:
			wr.WriteInt(-1)
		default:
			wr.WriteInt(int64((rem + time.Second - 1) / time.Second)) // round up: a live key never reports 0
		}

	case "DBSIZE":
		wr.WriteInt(int64(s.st.Len()))

	case "FLUSHALL", "FLUSHDB":
		s.st.Flush()
		wr.WriteSimple("OK")

	case "INFO":
		wr.WriteBulkString(s.info())

	case "CONFIG":
		// redis-benchmark asks "CONFIG GET save" / "CONFIG GET appendonly" on
		// start-up and warns if it gets no [name, value] pair back.
		if len(args) == 3 && strings.EqualFold(string(args[1]), "GET") {
			switch strings.ToLower(string(args[2])) {
			case "save":
				wr.WriteArrayHeader(2)
				wr.WriteBulkString("save")
				wr.WriteBulkString("")
			case "appendonly":
				wr.WriteArrayHeader(2)
				wr.WriteBulkString("appendonly")
				wr.WriteBulkString("no")
			default:
				wr.WriteArrayHeader(0)
			}
		} else {
			wr.WriteError("ERR CONFIG subcommand not supported")
		}

	case "COMMAND":
		wr.WriteArrayHeader(0) // redis-cli asks for command docs on connect

	case "QUIT":
		wr.WriteSimple("OK")
		return true

	default:
		n := string(args[0])
		if len(n) > 64 {
			n = n[:64]
		}
		wr.WriteError("ERR unknown command '" + n + "'")
	}
	return false
}

// parseSetOptions handles SET's optional "EX seconds" / "PX milliseconds".
func parseSetOptions(opts [][]byte) (ttl time.Duration, errMsg string) {
	for i := 0; i < len(opts); i++ {
		opt := strings.ToUpper(string(opts[i]))
		if opt != "EX" && opt != "PX" {
			return 0, "ERR syntax error"
		}
		if i+1 >= len(opts) {
			return 0, "ERR syntax error"
		}
		n, err := strconv.ParseInt(string(opts[i+1]), 10, 64)
		if err != nil {
			return 0, "ERR value is not an integer or out of range"
		}
		if n <= 0 || n > int64(1<<53) { // upper bound keeps the Duration math from overflowing
			return 0, "ERR invalid expire time in 'set' command"
		}
		if opt == "EX" {
			ttl = time.Duration(n) * time.Second
		} else {
			ttl = time.Duration(n) * time.Millisecond
		}
		i++
	}
	return ttl, ""
}

func (s *Server) info() string {
	st := s.st.Stats()
	s.mu.Lock()
	clients := len(s.conns)
	s.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\ncachekit_version:%s\r\nuptime_in_seconds:%d\r\n", Version, int64(time.Since(s.started).Seconds()))
	fmt.Fprintf(&b, "# Clients\r\nconnected_clients:%d\r\n", clients)
	fmt.Fprintf(&b, "# Stats\r\ntotal_connections_received:%d\r\ntotal_commands_processed:%d\r\n", s.totalConns.Load(), s.commands.Load())
	fmt.Fprintf(&b, "keyspace_hits:%d\r\nkeyspace_misses:%d\r\nexpired_keys:%d\r\nevicted_keys:%d\r\n", st.Hits, st.Misses, st.Expired, st.Evictions)
	fmt.Fprintf(&b, "# Keyspace\r\ndb0:keys=%d\r\n", st.Items)
	return b.String()
}
