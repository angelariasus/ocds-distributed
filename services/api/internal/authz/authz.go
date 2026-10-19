// Package authz turns the identity Envoy already authenticated into an
// authorization decision. Envoy verifies the JWT signature, issuer, audience
// and expiry against the JWKS and forwards the verified payload in a header it
// strips from incoming requests first; the API only decides what that
// identity may do. The API is reachable only through Envoy (mutual TLS), so
// the header cannot be forged by a client.
package authz

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const PayloadHeader = "x-jwt-payload"

var decisions = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "api_authz_total", Help: "Authorization decisions by operation and outcome.",
}, []string{"operation", "decision"})

var (
	ErrUnauthenticated = errors.New("no verified identity")
	ErrForbidden       = errors.New("role not allowed")
)

type Principal struct {
	Subject string   `json:"sub"`
	Roles   []string `json:"roles"`
}

// Policy maps an operation to the roles allowed to perform it.
type Policy map[string][]string

func DefaultPolicy() Policy {
	read := []string{"reader", "analyst"}
	return Policy{
		"top_buyers": read,
		"buyer":      read,
		"items":      read,
		"predict":    {"analyst"},
	}
}

func ParsePayload(header string) (Principal, error) {
	if header == "" {
		return Principal{}, ErrUnauthenticated
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(header, "="))
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	var p Principal
	if err := json.Unmarshal(raw, &p); err != nil || p.Subject == "" {
		return Principal{}, ErrUnauthenticated
	}
	return p, nil
}

func (pol Policy) Check(operation string, p Principal) error {
	allowed, ok := pol[operation]
	if !ok {
		return ErrForbidden
	}
	for _, r := range p.Roles {
		if slices.Contains(allowed, r) {
			return nil
		}
	}
	return ErrForbidden
}

// HTTP wraps a handler for one operation.
func (pol Policy) HTTP(operation string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := ParsePayload(r.Header.Get(PayloadHeader))
		if err == nil {
			err = pol.Check(operation, p)
		}
		switch {
		case errors.Is(err, ErrUnauthenticated):
			decisions.WithLabelValues(operation, "unauthenticated").Inc()
			http.Error(w, `{"error":"authentication required"}`, http.StatusUnauthorized)
		case errors.Is(err, ErrForbidden):
			decisions.WithLabelValues(operation, "forbidden").Inc()
			http.Error(w, `{"error":"role not allowed for `+operation+`"}`, http.StatusForbidden)
		default:
			decisions.WithLabelValues(operation, "allowed").Inc()
			next(w, r)
		}
	}
}

// Unary authorizes gRPC calls; methods map to operations by name.
func (pol Policy) Unary(methods map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		operation, guarded := methods[info.FullMethod]
		if !guarded {
			return handler(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		var header string
		if v := md.Get(PayloadHeader); len(v) > 0 {
			header = v[0]
		}
		p, err := ParsePayload(header)
		if err != nil {
			decisions.WithLabelValues(operation, "unauthenticated").Inc()
			return nil, status.Error(codes.Unauthenticated, "authentication required")
		}
		if err := pol.Check(operation, p); err != nil {
			decisions.WithLabelValues(operation, "forbidden").Inc()
			return nil, status.Errorf(codes.PermissionDenied, "role not allowed for %s", operation)
		}
		decisions.WithLabelValues(operation, "allowed").Inc()
		return handler(ctx, req)
	}
}
