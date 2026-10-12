package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"cachekit/cluster"
)

const (
	maxBody     = 4 << 10
	maxLongPoll = 30 * time.Second
)

// Handler returns the coordinator's HTTP API:
//
//	POST /heartbeat    {"id","addr"}  register / keep alive / rejoin
//	POST /deregister   {"id"}         graceful leave
//	GET  /ring         current membership; with ?epoch=E&since=V&wait=10s it
//	                   long-polls until the membership changes
//	GET  /status       all nodes incl. dead ones, with ring ownership
//	GET  /events       membership log; ?since=SEQ
//	GET  /healthz
func (c *Coordinator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /heartbeat", c.handleHeartbeat)
	mux.HandleFunc("POST /deregister", c.handleDeregister)
	mux.HandleFunc("GET /ring", c.handleRing)
	mux.HandleFunc("GET /status", c.handleStatus)
	mux.HandleFunc("GET /events", c.handleEvents)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func (c *Coordinator) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var info cluster.NodeInfo
	if !decode(w, r, &info) {
		return
	}
	if err := c.Heartbeat(info); err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, ErrInvalidNode) {
			code = http.StatusBadRequest
		}
		writeErr(w, code, err.Error())
		return
	}
	m := c.Membership()
	writeJSON(w, http.StatusOK, map[string]any{"epoch": m.Epoch, "version": m.Version})
}

func (c *Coordinator) handleDeregister(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID string `json:"id"`
	}
	if !decode(w, r, &body) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"removed": c.Deregister(body.ID)})
}

func (c *Coordinator) handleRing(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sinceStr := q.Get("since")
	if sinceStr == "" { // no snapshot to compare against: answer immediately
		writeJSON(w, http.StatusOK, c.Membership())
		return
	}
	since, err := strconv.ParseUint(sinceStr, 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "since must be an unsigned integer")
		return
	}
	wait := 10 * time.Second
	if ws := q.Get("wait"); ws != "" {
		d, err := time.ParseDuration(ws)
		if err != nil || d < 0 {
			writeErr(w, http.StatusBadRequest, "wait must be a duration such as 10s")
			return
		}
		wait = d
	}
	if wait > maxLongPoll {
		wait = maxLongPoll
	}
	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()
	writeJSON(w, http.StatusOK, c.WaitForChange(ctx, q.Get("epoch"), since))
}

func (c *Coordinator) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, c.Status())
}

func (c *Coordinator) handleEvents(w http.ResponseWriter, r *http.Request) {
	var since uint64
	if s := r.URL.Query().Get("since"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "since must be an unsigned integer")
			return
		}
		since = v
	}
	writeJSON(w, http.StatusOK, c.Events(since))
}
