package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// HTTPTransport carries Raft RPCs as JSON over HTTP; peers are addressed by ID.
type HTTPTransport struct {
	Peers  map[string]string // id → base URL
	Client *http.Client
}

func (t HTTPTransport) post(ctx context.Context, peer, path string, in, out any) error {
	base, ok := t.Peers[peer]
	if !ok {
		return fmt.Errorf("raft: unknown peer %q", peer)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("raft: %s%s: %s", base, path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (t HTTPTransport) RequestVote(ctx context.Context, peer string, req VoteRequest) (VoteResponse, error) {
	var resp VoteResponse
	return resp, t.post(ctx, peer, "/raft/vote", req, &resp)
}

func (t HTTPTransport) AppendEntries(ctx context.Context, peer string, req AppendRequest) (AppendResponse, error) {
	var resp AppendResponse
	return resp, t.post(ctx, peer, "/raft/append", req, &resp)
}

// Handler serves the receiving side of the RPCs plus a status page.
func Handler(n *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var req VoteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, n.HandleRequestVote(req))
	})
	mux.HandleFunc("POST /raft/append", func(w http.ResponseWriter, r *http.Request) {
		var req AppendRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, n.HandleAppendEntries(req))
	})
	mux.HandleFunc("GET /raft/status", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, n.Status()) })
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
