package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/angelariasus/ocds-distributed/services/api/gen/queryv1"
	"github.com/angelariasus/ocds-distributed/services/api/internal/cache"
)

type fakeReader struct{ calls int }

func (f *fakeReader) TopBuyers(_ context.Context, year, limit int32) ([]*queryv1.Buyer, error) {
	f.calls++
	return []*queryv1.Buyer{{Ruc: "20131257750", Nombre: "ESSALUD", EsEssalud: true, Items: 207}}, nil
}

func (f *fakeReader) BuyerSummary(_ context.Context, ruc string, _ int32) (*queryv1.BuyerSummaryResponse, error) {
	f.calls++
	if ruc != "20131257750" {
		return nil, errors.New("no rows in result set")
	}
	return &queryv1.BuyerSummaryResponse{Buyer: &queryv1.Buyer{Ruc: ruc}, ConAdenda: 3}, nil
}

func (f *fakeReader) SearchItems(context.Context, string, int32, int32) ([]*queryv1.Item, error) {
	f.calls++
	return []*queryv1.Item{{Ocid: "ocds-1", Descripcion: "PARACETAMOL"}}, nil
}

type memBackend struct{ data map[string][]byte }

func (m *memBackend) Get(_ context.Context, k string) ([]byte, bool, error) {
	v, ok := m.data[k]
	return v, ok, nil
}
func (m *memBackend) Set(_ context.Context, k string, v []byte, _ time.Duration) error {
	m.data[k] = v
	return nil
}
func (m *memBackend) Incr(context.Context, string) (int64, error)         { return 1, nil }
func (m *memBackend) GetInt(context.Context, string) (int64, bool, error) { return 0, false, nil }

func newServer() (*Server, *fakeReader) {
	r := &fakeReader{}
	return New("api-test", r, cache.New(&memBackend{data: map[string][]byte{}}, time.Minute), nil), r
}

func TestRestTopBuyersIsServedFromCacheTheSecondTime(t *testing.T) {
	s, reader := newServer()
	srv := httptest.NewServer(s.REST())
	defer srv.Close()
	var first, second map[string]any
	for _, dst := range []*map[string]any{&first, &second} {
		resp, err := http.Get(srv.URL + "/v1/top-buyers?year=2026&limit=5")
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("status=%v err=%v", resp.StatusCode, err)
		}
		json.NewDecoder(resp.Body).Decode(dst)
		resp.Body.Close()
	}
	if reader.calls != 1 {
		t.Fatalf("reader called %d times, want 1", reader.calls)
	}
	p1, p2 := first["provenance"].(map[string]any), second["provenance"].(map[string]any)
	if p1["cache_hit"] != false || p2["cache_hit"] != true || p2["source"] != "cache" || p2["served_by"] != "api-test" {
		t.Fatalf("provenance: first=%v second=%v", p1, p2)
	}
}

func TestRestMapsErrorsToStatusCodes(t *testing.T) {
	s, _ := newServer()
	srv := httptest.NewServer(s.REST())
	defer srv.Close()
	cases := map[string]int{
		"/v1/buyers/00000000000":  404,
		"/v1/items?q=ab":          400,
		"/v1/predict/lead-time":   405, // GET on a POST route
		"/v1/buyers/20131257750":  200,
		"/v1/items?q=paracetamol": 200,
	}
	for path, want := range cases {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("%s: status %d, want %d", path, resp.StatusCode, want)
		}
	}
}

func TestPredictWithoutModelServingIsUnavailable(t *testing.T) {
	s, _ := newServer()
	if _, err := s.PredictLeadTime(context.Background(), &queryv1.PredictLeadTimeRequest{}); err == nil {
		t.Fatal("expected unavailable")
	}
}
