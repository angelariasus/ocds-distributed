package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/angelariasus/ocds-distributed/services/catalog/gen/catalogv1"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/transport"
)

// ctl is a small client for demos: catalog ctl [-addr host:port] status|get|put|delete ...
func ctl(args []string) error {
	fs := flag.NewFlagSet("ctl", flag.ContinueOnError)
	addr := fs.String("addr", env("CATALOG_ADDR", "localhost:7101"), "node to talk to")
	r := fs.Uint("r", 0, "read quorum override")
	w := fs.Uint("w", 0, "write quorum override")
	files := transport.TLSFromEnv()
	fs.StringVar(&files.Cert, "cert", files.Cert, "client certificate (empty for plaintext)")
	fs.StringVar(&files.Key, "key", files.Key, "client key")
	fs.StringVar(&files.CA, "ca", files.CA, "CA that signed the server certificate")
	if err := fs.Parse(args); err != nil {
		return err
	}
	creds, err := files.ClientCredentials()
	if err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: catalog ctl [-addr] status | get KEY | put KEY VALUE [CONTEXT] | delete KEY [CONTEXT]")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := catalogv1.NewCatalogClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch rest[0] {
	case "status":
		resp, err := client.Status(ctx, &catalogv1.StatusRequest{})
		if err != nil {
			return err
		}
		return printJSON(resp)
	case "get":
		if len(rest) < 2 {
			return fmt.Errorf("get KEY")
		}
		resp, err := client.Get(ctx, &catalogv1.GetRequest{Key: rest[1], R: uint32(*r)})
		if err != nil {
			return err
		}
		out := map[string]any{"replicas_answered": resp.ReplicasAnswered, "context": base64.StdEncoding.EncodeToString(resp.Context)}
		var versions []map[string]any
		for _, v := range resp.Versions {
			versions = append(versions, map[string]any{"value": string(v.Value), "clock": v.Clock.Entries, "updated_at": time.UnixMilli(v.UpdatedAtMs).UTC()})
		}
		out["versions"] = versions
		return printJSON(out)
	case "put":
		if len(rest) < 3 {
			return fmt.Errorf("put KEY VALUE [CONTEXT]")
		}
		context, err := contextArg(rest, 3)
		if err != nil {
			return err
		}
		resp, err := client.Put(ctx, &catalogv1.PutRequest{Key: rest[1], Value: []byte(rest[2]), Context: context, W: uint32(*w)})
		if err != nil {
			return err
		}
		var clock catalogv1.VectorClock
		proto.Unmarshal(resp.Context, &clock)
		return printJSON(map[string]any{"replicas_acked": resp.ReplicasAcked, "hinted": resp.Hinted, "clock": clock.Entries,
			"context": base64.StdEncoding.EncodeToString(resp.Context)})
	case "delete":
		if len(rest) < 2 {
			return fmt.Errorf("delete KEY [CONTEXT]")
		}
		context, err := contextArg(rest, 2)
		if err != nil {
			return err
		}
		resp, err := client.Delete(ctx, &catalogv1.DeleteRequest{Key: rest[1], Context: context, W: uint32(*w)})
		if err != nil {
			return err
		}
		return printJSON(resp)
	}
	return fmt.Errorf("unknown command %q", rest[0])
}

func contextArg(rest []string, idx int) ([]byte, error) {
	if len(rest) <= idx || strings.TrimSpace(rest[idx]) == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(rest[idx])
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
