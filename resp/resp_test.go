package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func cmd(t *testing.T, input string) [][]byte {
	t.Helper()
	args, err := NewReader(strings.NewReader(input)).ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand(%q): %v", input, err)
	}
	return args
}

func strs(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}

func TestReadCommandArray(t *testing.T) {
	got := strs(cmd(t, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n"))
	if len(got) != 3 || got[0] != "SET" || got[1] != "k" || got[2] != "hello" {
		t.Fatalf("got %v", got)
	}
}

func TestReadCommandBinarySafe(t *testing.T) {
	// A value containing CRLF and a NUL byte must survive intact.
	val := "a\r\nb\x00c"
	input := "*2\r\n$3\r\nGET\r\n$" + "6" + "\r\n" + val + "\r\n"
	got := cmd(t, input)
	if string(got[1]) != val {
		t.Fatalf("got %q want %q", got[1], val)
	}
}

func TestReadCommandInline(t *testing.T) {
	got := strs(cmd(t, "SET  key   value\r\n"))
	if len(got) != 3 || got[0] != "SET" || got[2] != "value" {
		t.Fatalf("got %v", got)
	}
	got = strs(cmd(t, "PING\n")) // bare LF is accepted
	if len(got) != 1 || got[0] != "PING" {
		t.Fatalf("got %v", got)
	}
}

func TestReadCommandSkipsEmpty(t *testing.T) {
	got := strs(cmd(t, "\r\n\r\n*0\r\n*-1\r\nPING\r\n"))
	if len(got) != 1 || got[0] != "PING" {
		t.Fatalf("got %v", got)
	}
}

func TestReadCommandPipelined(t *testing.T) {
	r := NewReader(strings.NewReader("PING\r\n*2\r\n$3\r\nGET\r\n$1\r\nx\r\nPING\r\n"))
	for i, want := range []string{"PING", "GET", "PING"} {
		args, err := r.ReadCommand()
		if err != nil || string(args[0]) != want {
			t.Fatalf("command %d: %v %v", i, args, err)
		}
	}
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF at end, got %v", err)
	}
}

func TestReadCommandByteAtATime(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n"
	r := NewReader(iotest.OneByteReader(strings.NewReader(input)))
	args, err := r.ReadCommand()
	if err != nil || len(args) != 3 || string(args[2]) != "hello" {
		t.Fatalf("got %v, %v", strs(args), err)
	}
}

func TestReadCommandProtocolErrors(t *testing.T) {
	bad := map[string]string{
		"non-numeric array len": "*x\r\n",
		"missing $":             "*1\r\nGET\r\n",
		"negative bulk":         "*1\r\n$-5\r\n",
		"bulk too large":        "*1\r\n$99999999999\r\n",
		"too many args":         "*100000\r\n",
		"bad terminator":        "*1\r\n$3\r\nabcXX",
		"line too long":         "*1\r\n$" + strings.Repeat("9", 20000) + "\r\n",
	}
	for name, input := range bad {
		_, err := NewReader(strings.NewReader(input)).ReadCommand()
		if !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: want ErrProtocol, got %v", name, err)
		}
	}
}

func TestReadCommandTruncated(t *testing.T) {
	_, err := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$5\r\nab")).ReadCommand()
	if err == nil || errors.Is(err, ErrProtocol) {
		t.Fatalf("truncated input should surface an I/O error, got %v", err)
	}
	if _, err := NewReader(strings.NewReader("")).ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("empty input: want EOF, got %v", err)
	}
}

func TestWriterAndReadValueRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteSimple("OK")
	w.WriteError("ERR boom")
	w.WriteInt(-42)
	w.WriteBulk([]byte("va\r\nlue"))
	w.WriteBulkString("")
	w.WriteNull()
	w.WriteArrayHeader(2)
	w.WriteBulkString("a")
	w.WriteInt(7)
	w.WriteArrayHeader(0)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	r := NewReader(&buf)
	check := func(want Value) {
		t.Helper()
		got, err := r.ReadValue()
		if err != nil {
			t.Fatal(err)
		}
		if got.Type != want.Type || string(got.Str) != string(want.Str) || got.Int != want.Int ||
			got.Null != want.Null || len(got.Array) != len(want.Array) {
			t.Fatalf("got %+v want %+v", got, want)
		}
	}
	check(Value{Type: Simple, Str: []byte("OK")})
	check(Value{Type: Error, Str: []byte("ERR boom")})
	check(Value{Type: Integer, Int: -42})
	check(Value{Type: Bulk, Str: []byte("va\r\nlue")})
	check(Value{Type: Bulk, Str: []byte("")})
	check(Value{Type: Bulk, Null: true})
	arr, err := r.ReadValue()
	if err != nil || arr.Type != Array || len(arr.Array) != 2 ||
		string(arr.Array[0].Str) != "a" || arr.Array[1].Int != 7 {
		t.Fatalf("array: %+v %v", arr, err)
	}
	check(Value{Type: Array}) // empty array
}

func TestWriterStripsNewlinesFromSimpleAndError(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteError("ERR bad\r\n+INJECTED")
	w.Flush()
	if got := buf.String(); strings.Count(got, "\r\n") != 1 || !strings.HasSuffix(got, "\r\n") {
		t.Fatalf("reply was not confined to one line: %q", got)
	}
}

func TestWriteArgsMatchesReadCommand(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteArgs([]byte("SET"), []byte("k"), []byte("v\r\nv"))
	w.Flush()
	args, err := NewReader(&buf).ReadCommand()
	if err != nil || len(args) != 3 || string(args[2]) != "v\r\nv" {
		t.Fatalf("got %v, %v", strs(args), err)
	}
}

func TestReadValueErrors(t *testing.T) {
	for name, in := range map[string]string{
		"unknown type": "?what\r\n",
		"bad integer":  ":abc\r\n",
		"empty line":   "\r\n",
		"deep nesting": strings.Repeat("*1\r\n", 20) + ":1\r\n",
	} {
		if _, err := NewReader(strings.NewReader(in)).ReadValue(); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: want ErrProtocol, got %v", name, err)
		}
	}
}

func TestBuffered(t *testing.T) {
	r := NewReader(strings.NewReader("PING\r\nPING\r\n"))
	r.ReadCommand()
	if r.Buffered() == 0 {
		t.Fatal("second command should still be buffered")
	}
	r.ReadCommand()
	if r.Buffered() != 0 {
		t.Fatal("buffer should be drained")
	}
}
