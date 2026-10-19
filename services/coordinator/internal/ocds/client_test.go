package ocds

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const pageTmpl = `{"pagination":{"has_next":%t},"results":[{"id":"seace_v3-2026-%s","source":"seace_v3","year":"2026","month":"%s","timestamp":"2026-10-17T12:30:14.004441-05:00","files":{"json":"j","sha":"s"}}]}`

func TestListFilesFollowsPagination(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != userAgent {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.URL.Query().Get("page") {
		case "1":
			fmt.Fprintf(w, pageTmpl, true, "10", "10")
		case "2":
			fmt.Fprintf(w, pageTmpl, false, "09", "09")
		}
	}))
	defer srv.Close()

	files, err := NewClient(srv.URL).ListFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Month != 10 || files[1].Month != 9 {
		t.Fatalf("unexpected files: %+v", files)
	}
	if files[0].Published.IsZero() {
		t.Fatal("published timestamp not parsed")
	}
}

func TestListFilesRetriesTransientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, pageTmpl, false, "09", "09")
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.backoff = time.Millisecond
	if _, err := c.ListFiles(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3 calls, got %d", calls.Load())
	}
}

func TestListFilesDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	if _, err := NewClient(srv.URL).ListFiles(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", calls.Load())
	}
}
