package portal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func testServer(t *testing.T, b Behaviour) (*httptest.Server, int) {
	t.Helper()
	dir := t.TempDir()
	ids := make([]string, 0, 230)
	for i := 0; i < 230; i++ {
		ids = append(ids, fmt.Sprintf("%d", 100000+i))
	}
	writeArchive(t, dir, "seace_v3-2026-09.json.zip", ids)
	store, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(store, b, "http://mock").Handler())
	t.Cleanup(srv.Close)
	return srv, len(ids)
}

func get(t *testing.T, url, ua string) (*http.Response, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("User-Agent", ua)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)
	return resp, body
}

func TestGenericUserAgentIsForbidden(t *testing.T) {
	srv, _ := testServer(t, Behaviour{BlockUserAgents: []string{"curl/"}})
	resp, _ := get(t, srv.URL+"/api/v1/files", "curl/8.0")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	resp, _ = get(t, srv.URL+"/api/v1/files", "ocds-distributed/0.1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestPageSizeAboveLimitIsNotFound(t *testing.T) {
	srv, _ := testServer(t, Behaviour{})
	resp, _ := get(t, srv.URL+"/api/v1/recordsAfter?size=101", "test")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestFollowingNextLinksVisitsEverything(t *testing.T) {
	srv, total := testServer(t, Behaviour{})
	next := srv.URL + "/api/v1/recordsAfter?size=100"
	seen := 0
	for next != "" {
		resp, body := get(t, next, "test")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		seen += len(body["records"].([]any))
		links := body["links"].(map[string]any)
		next, _ = links["next"].(string)
		next = strings.Replace(next, "http://mock", srv.URL, 1)
	}
	if seen != total {
		t.Fatalf("saw %d, want %d", seen, total)
	}
}

func TestFileEndpointsServeZipAndSHA(t *testing.T) {
	srv, _ := testServer(t, Behaviour{})
	_, body := get(t, srv.URL+"/api/v1/files", "test")
	results := body["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("results = %v", results)
	}
	files := results[0].(map[string]any)["files"].(map[string]any)

	resp, _ := get(t, strings.Replace(files["json"].(string), "http://mock", srv.URL, 1), "test")
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/zip" {
		t.Fatalf("zip: status=%d type=%s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	resp, _ = get(t, strings.Replace(files["sha"].(string), "http://mock", srv.URL, 1), "test")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sha status = %d", resp.StatusCode)
	}
}

func TestLatencyGrowsWithConcurrencyAndThrottles(t *testing.T) {
	srv, _ := testServer(t, Behaviour{BaseLatency: 20 * time.Millisecond, PerInflight: 20 * time.Millisecond, ThrottleAbove: 4})
	var wg sync.WaitGroup
	statuses := make([]int, 8)
	for i := range statuses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, _ := get(t, srv.URL+"/api/v1/recordsAfter?size=1", "test")
			statuses[i] = resp.StatusCode
		}(i)
	}
	wg.Wait()
	throttled := 0
	for _, s := range statuses {
		if s == http.StatusTooManyRequests {
			throttled++
		}
	}
	if throttled == 0 {
		t.Fatalf("expected some 429s, statuses = %v", statuses)
	}
}
