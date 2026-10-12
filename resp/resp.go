// Package resp implements the subset of the Redis wire protocol (RESP2) that
// cachekit speaks: commands arrive as arrays of bulk strings (or as simple
// inline text, for telnet/netcat), and replies are simple strings, errors,
// integers, bulk strings or arrays.
//
// Because the framing is standard RESP, redis-cli and redis-benchmark can talk
// to a cachekit node unchanged.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Limits protect a node from a client that announces absurd sizes.
const (
	MaxBulkLen  = 16 << 20 // largest single value (16 MiB)
	MaxArrayLen = 1024     // most arguments in one command
	maxDepth    = 8        // nesting limit when parsing replies
)

// ErrProtocol marks malformed input. Network errors (EOF, timeouts) are
// returned as-is, so callers can tell the two apart with errors.Is.
var ErrProtocol = errors.New("resp: protocol error")

func protoErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, a...))
}

// ---------------------------------------------------------------- reading

// Reader decodes RESP from a stream. It is not safe for concurrent use.
type Reader struct{ br *bufio.Reader }

// NewReader wraps r in a buffered RESP reader.
func NewReader(r io.Reader) *Reader { return &Reader{br: bufio.NewReaderSize(r, 16<<10)} }

// Buffered returns how many bytes are already read from the network but not
// yet consumed. The server uses it to batch replies for pipelined clients.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// readLine returns one line without its terminator. The slice is only valid
// until the next read. Both "\r\n" and a bare "\n" are accepted.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, protoErr("line too long")
		}
		return nil, err
	}
	line = line[:len(line)-1]
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, nil
}

func parseInt(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, protoErr("invalid integer %q", b)
	}
	return n, nil
}

// readBody reads exactly n bytes plus the trailing CRLF.
func (r *Reader) readBody(n int) ([]byte, error) {
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, protoErr("bulk string not terminated by CRLF")
	}
	return buf[:n], nil
}

// ReadCommand reads one client command as a list of arguments. Empty lines
// and empty arrays are skipped. Every returned slice is a fresh copy that the
// caller may keep (the store keeps SET values without copying again).
func (r *Reader) ReadCommand() ([][]byte, error) {
	for {
		first, err := r.br.Peek(1)
		if err != nil {
			return nil, err
		}
		if first[0] != '*' {
			// Inline command, e.g. "PING\r\n" typed into telnet.
			line, err := r.readLine()
			if err != nil {
				return nil, err
			}
			fields := bytes.Fields(line)
			if len(fields) == 0 {
				continue
			}
			args := make([][]byte, len(fields))
			for i, f := range fields {
				args[i] = append([]byte(nil), f...) // line memory is reused
			}
			return args, nil
		}

		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		n, err := parseInt(line[1:])
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			continue // "*0" / "*-1": nothing to execute
		}
		if n > MaxArrayLen {
			return nil, protoErr("too many arguments (%d)", n)
		}
		args := make([][]byte, n)
		for i := range args {
			hdr, err := r.readLine()
			if err != nil {
				return nil, err
			}
			if len(hdr) == 0 || hdr[0] != '$' {
				return nil, protoErr("expected '$', got %q", hdr)
			}
			size, err := parseInt(hdr[1:])
			if err != nil {
				return nil, err
			}
			if size < 0 || size > MaxBulkLen {
				return nil, protoErr("invalid bulk length %d", size)
			}
			if args[i], err = r.readBody(int(size)); err != nil {
				return nil, err
			}
		}
		return args, nil
	}
}

// Type identifies the kind of a reply Value.
type Type byte

// Reply types, named by their RESP prefix byte.
const (
	Simple  Type = '+'
	Error   Type = '-'
	Integer Type = ':'
	Bulk    Type = '$'
	Array   Type = '*'
)

// Value is one decoded server reply.
type Value struct {
	Type  Type
	Str   []byte // Simple, Error and Bulk
	Int   int64  // Integer
	Null  bool   // null bulk string or null array
	Array []Value
}

// ReadValue reads one reply (used by the client library and tests).
func (r *Reader) ReadValue() (Value, error) { return r.readValue(0) }

func (r *Reader) readValue(depth int) (Value, error) {
	if depth > maxDepth {
		return Value{}, protoErr("reply nested too deeply")
	}
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, protoErr("empty reply line")
	}
	t, rest := Type(line[0]), line[1:]
	switch t {
	case Simple, Error:
		return Value{Type: t, Str: append([]byte(nil), rest...)}, nil
	case Integer:
		n, err := parseInt(rest)
		return Value{Type: t, Int: n}, err
	case Bulk:
		n, err := parseInt(rest)
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Value{Type: t, Null: true}, nil
		}
		if n > MaxBulkLen {
			return Value{}, protoErr("bulk reply too large (%d)", n)
		}
		body, err := r.readBody(int(n))
		return Value{Type: t, Str: body}, err
	case Array:
		n, err := parseInt(rest)
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Value{Type: t, Null: true}, nil
		}
		if n > MaxArrayLen {
			return Value{}, protoErr("array reply too large (%d)", n)
		}
		v := Value{Type: t, Array: make([]Value, n)}
		for i := range v.Array {
			if v.Array[i], err = r.readValue(depth + 1); err != nil {
				return Value{}, err
			}
		}
		return v, nil
	default:
		return Value{}, protoErr("unknown reply type %q", line[0])
	}
}

// ---------------------------------------------------------------- writing

// Writer encodes RESP into a buffer. Write errors are sticky: the first one is
// remembered and returned by Flush, so callers check once per batch.
type Writer struct{ bw *bufio.Writer }

// NewWriter wraps w in a buffered RESP writer.
func NewWriter(w io.Writer) *Writer { return &Writer{bw: bufio.NewWriterSize(w, 16<<10)} }

// Flush sends everything buffered so far.
func (w *Writer) Flush() error { return w.bw.Flush() }

// oneLine strips CR/LF so user-controlled text cannot inject extra replies.
func oneLine(s string) string {
	if strings.ContainsAny(s, "\r\n") {
		s = strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
	}
	return s
}

// WriteSimple writes "+<s>\r\n".
func (w *Writer) WriteSimple(s string) { w.bw.WriteString("+" + oneLine(s) + "\r\n") }

// WriteError writes "-<s>\r\n". By convention s starts with an error code such as "ERR".
func (w *Writer) WriteError(s string) { w.bw.WriteString("-" + oneLine(s) + "\r\n") }

// WriteInt writes ":<n>\r\n".
func (w *Writer) WriteInt(n int64) {
	w.bw.WriteByte(':')
	w.bw.WriteString(strconv.FormatInt(n, 10))
	w.bw.WriteString("\r\n")
}

// WriteBulk writes a length-prefixed binary-safe string.
func (w *Writer) WriteBulk(b []byte) {
	w.bw.WriteByte('$')
	w.bw.WriteString(strconv.Itoa(len(b)))
	w.bw.WriteString("\r\n")
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

// WriteBulkString is WriteBulk for a string.
func (w *Writer) WriteBulkString(s string) {
	w.bw.WriteByte('$')
	w.bw.WriteString(strconv.Itoa(len(s)))
	w.bw.WriteString("\r\n")
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// WriteNull writes the null bulk string ("key not found").
func (w *Writer) WriteNull() { w.bw.WriteString("$-1\r\n") }

// WriteArrayHeader starts an array of n elements; the caller then writes n values.
func (w *Writer) WriteArrayHeader(n int) {
	w.bw.WriteByte('*')
	w.bw.WriteString(strconv.Itoa(n))
	w.bw.WriteString("\r\n")
}

// WriteArgs writes a command as an array of bulk strings (client side).
func (w *Writer) WriteArgs(args ...[]byte) {
	w.WriteArrayHeader(len(args))
	for _, a := range args {
		w.WriteBulk(a)
	}
}
