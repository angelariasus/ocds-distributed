package authz

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func payload(sub string, roles ...string) string {
	raw, _ := json.Marshal(map[string]any{"sub": sub, "roles": roles, "iss": "ocds-distributed"})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func call(pol Policy, operation, header string) int {
	h := pol.HTTP(operation, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if header != "" {
		req.Header.Set(PayloadHeader, header)
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec.Code
}

func TestRolesMapToOperations(t *testing.T) {
	pol := DefaultPolicy()
	cases := []struct {
		operation string
		header    string
		want      int
	}{
		{"top_buyers", payload("ana", "reader"), 200},
		{"top_buyers", payload("luis", "analyst"), 200},
		{"predict", payload("ana", "reader"), 403},
		{"predict", payload("luis", "analyst"), 200},
		{"top_buyers", "", 401},
		{"top_buyers", "%%%not-base64", 401},
		{"top_buyers", payload("", "reader"), 401},
		{"unknown_operation", payload("luis", "analyst"), 403},
	}
	for _, c := range cases {
		if got := call(pol, c.operation, c.header); got != c.want {
			t.Errorf("%s with %q: %d, want %d", c.operation, c.header, got, c.want)
		}
	}
}

func TestPaddedPayloadIsAccepted(t *testing.T) {
	p, err := ParsePayload(payload("ana", "reader") + "==")
	if err != nil || p.Subject != "ana" {
		t.Fatalf("p=%+v err=%v", p, err)
	}
}

func TestUnaryInterceptor(t *testing.T) {
	pol := DefaultPolicy()
	icpt := pol.Unary(map[string]string{"/query.v1.Query/PredictLeadTime": "predict"})
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/query.v1.Query/PredictLeadTime"}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(PayloadHeader, payload("ana", "reader")))
	if _, err := icpt(ctx, nil, info, handler); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reader predicting: %v", err)
	}
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs(PayloadHeader, payload("luis", "analyst")))
	if out, err := icpt(ctx, nil, info, handler); err != nil || out != "ok" {
		t.Fatalf("analyst predicting: %v %v", out, err)
	}
	if _, err := icpt(context.Background(), nil, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("anonymous: %v", err)
	}
	unguarded := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	if _, err := icpt(context.Background(), nil, unguarded, handler); err != nil {
		t.Fatalf("health check must not need a token: %v", err)
	}
}
