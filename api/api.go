// Package api is the demo application's HTTP/JSON interface to the product
// catalog. It is deliberately thin: parse and validate the request, call
// catalog.Service, translate the result (and errors) into HTTP.
//
//	GET    /products/{id}        one product; headers X-Cache (HIT|MISS) and X-Cache-Node
//	GET    /products?limit&offset  a page of products (not cached)
//	POST   /products             create          -> 201 + Location
//	PUT    /products/{id}        replace fields  -> 200
//	DELETE /products/{id}        delete          -> 204
//	GET    /healthz              200 if the database answers, else 503
//	GET    /stats                cache-aside counters and ring membership
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"cachekit/catalog"
)

const (
	maxBodyBytes   = 64 << 10 // request bodies above this are rejected
	defaultTimeout = 5 * time.Second
)

// Options configures the handler. Zero values select the defaults noted.
type Options struct {
	// Ping checks that the database is reachable (for /healthz). nil means
	// "always healthy", which is right for the in-memory store.
	Ping func(ctx context.Context) error
	// RequestTimeout bounds one request's work (default 5s).
	RequestTimeout time.Duration
	Logger         *slog.Logger
}

type handler struct {
	svc  *catalog.Service
	opts Options
	log  *slog.Logger
}

// NewHandler returns the demo API as an http.Handler.
func NewHandler(svc *catalog.Service, opts Options) http.Handler {
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = defaultTimeout
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	h := &handler{svc: svc, opts: opts, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /products/{id}", h.getProduct)
	mux.HandleFunc("GET /products", h.listProducts)
	mux.HandleFunc("POST /products", h.createProduct)
	mux.HandleFunc("PUT /products/{id}", h.updateProduct)
	mux.HandleFunc("DELETE /products/{id}", h.deleteProduct)
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /stats", h.stats)
	return h.middleware(mux)
}

// middleware adds the per-request timeout and turns a panic in a handler into
// a 500 instead of a dropped connection.
func (h *handler) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				h.log.Error("handler panic", "method", r.Method, "path", r.URL.Path, "panic", fmt.Sprint(v))
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		ctx, cancel := context.WithTimeout(r.Context(), h.opts.RequestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ---- helpers ----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		status, b = http.StatusInternalServerError, []byte(`{"error":"internal error"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(append(b, '\n'))
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// fail maps a service error to a status code. Unexpected errors are logged in
// full but shown to the client without detail.
func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, catalog.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, catalog.ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		h.log.Warn("request timed out", "method", r.Method, "path", r.URL.Path)
		writeError(w, http.StatusGatewayTimeout, "timed out")
	case errors.Is(err, context.Canceled):
		// The client went away; the status is never seen.
		writeError(w, http.StatusServiceUnavailable, "request cancelled")
	default:
		h.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// pathID parses the {id} path segment: a positive 64-bit integer.
func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		return 0, fmt.Errorf("%w: id must be a positive integer", catalog.ErrInvalid)
	}
	return id, nil
}

// decodeInput reads a JSON request body into a catalog.Input. Unknown fields
// and trailing data are rejected so typos do not pass silently.
func decodeInput(w http.ResponseWriter, r *http.Request) (catalog.Input, error) {
	var in catalog.Input
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return in, fmt.Errorf("%w: body larger than %d bytes", catalog.ErrInvalid, maxBodyBytes)
		}
		return in, fmt.Errorf("%w: malformed JSON body: %v", catalog.ErrInvalid, err)
	}
	if dec.More() {
		return in, fmt.Errorf("%w: unexpected data after the JSON object", catalog.ErrInvalid)
	}
	return in, nil
}

// ---- handlers ---------------------------------------------------------------

func (h *handler) getProduct(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	p, src, err := h.svc.Get(r.Context(), id)

	// Set the cache headers whether or not the product exists: a cached
	// "not found" is still a cache hit.
	if src == catalog.SourceCache {
		w.Header().Set("X-Cache", "HIT")
	} else {
		w.Header().Set("X-Cache", "MISS")
	}
	if node := h.svc.CacheNode(id); node != "" {
		w.Header().Set("X-Cache-Node", node)
	}

	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *handler) listProducts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, offset := 0, 0
	for name, dst := range map[string]*int{"limit": &limit, "offset": &offset} {
		if v := q.Get(name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				h.fail(w, r, fmt.Errorf("%w: %s must be an integer", catalog.ErrInvalid, name))
				return
			}
			*dst = n
		}
	}
	ps, err := h.svc.List(r.Context(), limit, offset)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"products": ps})
}

func (h *handler) createProduct(w http.ResponseWriter, r *http.Request) {
	in, err := decodeInput(w, r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	p, err := h.svc.Create(r.Context(), in)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/products/"+strconv.FormatInt(p.ID, 10))
	writeJSON(w, http.StatusCreated, p)
}

func (h *handler) updateProduct(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	in, err := decodeInput(w, r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	p, err := h.svc.Update(r.Context(), id, in)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *handler) deleteProduct(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if err := h.svc.Delete(r.Context(), id); err != nil {
		h.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) healthz(w http.ResponseWriter, r *http.Request) {
	if h.opts.Ping != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := h.opts.Ping(ctx); err != nil {
			h.log.Warn("health check failed", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "error": "database unreachable"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.svc.Stats())
}
