// Package transport adapts the node and membership interfaces to gRPC and
// converts between proto messages and internal types.
package transport

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/angelariasus/ocds-distributed/services/catalog/gen/catalogv1"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/membership"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/store"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/vclock"
)

type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewClient() *Client { return &Client{conns: map[string]*grpc.ClientConn{}} }

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *Client) internal(addr string) (catalogv1.InternalClient, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	return catalogv1.NewInternalClient(conn), nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
}

// node.Peer

func (c *Client) ReplicaGet(ctx context.Context, addr, key string) ([]store.Version, error) {
	cl, err := c.internal(addr)
	if err != nil {
		return nil, err
	}
	resp, err := cl.ReplicaGet(ctx, &catalogv1.ReplicaGetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return FromProtoVersions(resp.Versions), nil
}

func (c *Client) ReplicaPut(ctx context.Context, addr, key string, v store.Version, hintFor string) error {
	cl, err := c.internal(addr)
	if err != nil {
		return err
	}
	_, err = cl.ReplicaPut(ctx, &catalogv1.ReplicaPutRequest{Key: key, Version: ToProtoVersion(v), HintFor: hintFor})
	return err
}

func (c *Client) TreeLevel(ctx context.Context, addr, peer string, depth int) ([][]byte, error) {
	cl, err := c.internal(addr)
	if err != nil {
		return nil, err
	}
	resp, err := cl.TreeLevel(ctx, &catalogv1.TreeLevelRequest{Depth: uint32(depth), Peer: peer})
	if err != nil {
		return nil, err
	}
	return resp.Hashes, nil
}

func (c *Client) BucketKeys(ctx context.Context, addr, peer string, bucket uint32) (map[string][]store.Version, error) {
	cl, err := c.internal(addr)
	if err != nil {
		return nil, err
	}
	resp, err := cl.BucketKeys(ctx, &catalogv1.BucketKeysRequest{Bucket: bucket, Peer: peer})
	if err != nil {
		return nil, err
	}
	out := map[string][]store.Version{}
	for i, key := range resp.Keys {
		if i < len(resp.Versions) {
			out[key] = append(out[key], FromProtoVersion(resp.Versions[i]))
		}
	}
	return out, nil
}

// membership.Transport

func (c *Client) Ping(ctx context.Context, addr string, from membership.Member, view []membership.Member) ([]membership.Member, error) {
	cl, err := c.internal(addr)
	if err != nil {
		return nil, err
	}
	resp, err := cl.Ping(ctx, &catalogv1.PingRequest{From: from.ID, Members: ToProtoMembers(view)})
	if err != nil {
		return nil, err
	}
	return FromProtoMembers(resp.Members), nil
}

func (c *Client) PingReq(ctx context.Context, via string, target membership.Member) (bool, error) {
	cl, err := c.internal(via)
	if err != nil {
		return false, err
	}
	resp, err := cl.PingReq(ctx, &catalogv1.PingReqRequest{TargetId: target.ID, TargetAddr: target.Addr})
	if err != nil {
		return false, err
	}
	return resp.Reachable, nil
}

// conversions

func ToProtoClock(c vclock.Clock) *catalogv1.VectorClock {
	return &catalogv1.VectorClock{Entries: map[string]uint64(c.Copy())}
}

func FromProtoClock(c *catalogv1.VectorClock) vclock.Clock {
	if c == nil {
		return vclock.Clock{}
	}
	return vclock.Clock(c.Entries).Copy()
}

func ToProtoVersion(v store.Version) *catalogv1.Version {
	return &catalogv1.Version{Value: v.Value, Clock: ToProtoClock(v.Clock), Tombstone: v.Tombstone, UpdatedAtMs: v.UpdatedAt.UnixMilli()}
}

func FromProtoVersion(v *catalogv1.Version) store.Version {
	return store.Version{Value: v.Value, Clock: FromProtoClock(v.Clock), Tombstone: v.Tombstone, UpdatedAt: time.UnixMilli(v.UpdatedAtMs).UTC()}
}

func ToProtoVersions(vs []store.Version) []*catalogv1.Version {
	out := make([]*catalogv1.Version, 0, len(vs))
	for _, v := range vs {
		out = append(out, ToProtoVersion(v))
	}
	return out
}

func FromProtoVersions(vs []*catalogv1.Version) []store.Version {
	out := make([]store.Version, 0, len(vs))
	for _, v := range vs {
		out = append(out, FromProtoVersion(v))
	}
	return out
}

func ToProtoMembers(ms []membership.Member) []*catalogv1.GossipMember {
	out := make([]*catalogv1.GossipMember, 0, len(ms))
	for _, m := range ms {
		out = append(out, &catalogv1.GossipMember{Id: m.ID, Addr: m.Addr, State: m.State.String(), Incarnation: m.Incarnation, Heartbeat: m.Heartbeat})
	}
	return out
}

func FromProtoMembers(ms []*catalogv1.GossipMember) []membership.Member {
	out := make([]membership.Member, 0, len(ms))
	for _, m := range ms {
		out = append(out, membership.Member{ID: m.Id, Addr: m.Addr, State: membership.ParseState(m.State), Incarnation: m.Incarnation, Heartbeat: m.Heartbeat})
	}
	return out
}
