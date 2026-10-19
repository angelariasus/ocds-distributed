package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/angelariasus/ocds-distributed/services/catalog/gen/catalogv1"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/transport"
)

// bench drives a mixed read/write load against the cluster for a fixed time
// and reports throughput and latency percentiles, so the cost of each R/W
// setting (and of failures) can be measured rather than argued.
func bench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	addrs := fs.String("addrs", env("CATALOG_ADDRS", "catalog1:7100,catalog2:7100,catalog3:7100,catalog4:7100,catalog5:7100"), "coordinators to spread requests over")
	duration := fs.Duration("duration", 15*time.Second, "how long to run")
	workers := fs.Int("concurrency", 16, "concurrent clients")
	keys := fs.Int("keys", 1000, "key space")
	writeRatio := fs.Float64("write-ratio", 0.5, "fraction of operations that are writes")
	r := fs.Uint("r", 0, "read quorum (0 = node default)")
	w := fs.Uint("w", 0, "write quorum (0 = node default)")
	label := fs.String("label", "", "name printed with the result")
	files := transport.TLSFromEnv()
	fs.StringVar(&files.Cert, "cert", files.Cert, "client certificate")
	fs.StringVar(&files.Key, "key", files.Key, "client key")
	fs.StringVar(&files.CA, "ca", files.CA, "CA")
	if err := fs.Parse(args); err != nil {
		return err
	}
	creds, err := files.ClientCredentials()
	if err != nil {
		return err
	}
	var clients []catalogv1.CatalogClient
	for _, a := range strings.Split(*addrs, ",") {
		conn, err := grpc.NewClient(strings.TrimSpace(a), grpc.WithTransportCredentials(creds))
		if err != nil {
			return err
		}
		defer conn.Close()
		clients = append(clients, catalogv1.NewCatalogClient(conn))
	}

	type sample struct {
		write bool
		ok    bool
		dur   time.Duration
	}
	var mu sync.Mutex
	var samples []sample
	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed))
			var local []sample
			for time.Now().Before(deadline) {
				c := clients[rnd.Intn(len(clients))]
				key := fmt.Sprintf("bench/%d", rnd.Intn(*keys))
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				start := time.Now()
				var err error
				write := rnd.Float64() < *writeRatio
				if write {
					_, err = c.Put(ctx, &catalogv1.PutRequest{Key: key, Value: []byte(start.String()), W: uint32(*w)})
				} else {
					_, err = c.Get(ctx, &catalogv1.GetRequest{Key: key, R: uint32(*r)})
				}
				cancel()
				local = append(local, sample{write: write, ok: err == nil, dur: time.Since(start)})
			}
			mu.Lock()
			samples = append(samples, local...)
			mu.Unlock()
		}(time.Now().UnixNano() + int64(i))
	}
	wg.Wait()

	report := func(kind string, keep func(sample) bool) {
		var durs []time.Duration
		errs := 0
		for _, s := range samples {
			if !keep(s) {
				continue
			}
			if !s.ok {
				errs++
				continue
			}
			durs = append(durs, s.dur)
		}
		total := len(durs) + errs
		if total == 0 {
			return
		}
		sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
		pct := func(p float64) string {
			if len(durs) == 0 {
				return "-"
			}
			return durs[int(p*float64(len(durs)-1))].Round(100 * time.Microsecond).String()
		}
		fmt.Printf("%-22s %-6s ops=%-6d ok/s=%-7.0f errors=%5.1f%%  p50=%-8s p95=%-8s p99=%s\n",
			*label, kind, total, float64(len(durs))/duration.Seconds(), 100*float64(errs)/float64(total), pct(0.50), pct(0.95), pct(0.99))
	}
	report("read", func(s sample) bool { return !s.write })
	report("write", func(s sample) bool { return s.write })
	return nil
}
