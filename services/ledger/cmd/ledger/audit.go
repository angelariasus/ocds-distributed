package main

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	ledgerlog "github.com/angelariasus/ocds-distributed/services/ledger/internal/log"
	"github.com/angelariasus/ocds-distributed/services/ledger/internal/merkle"
)

// audit plays an independent auditor: it keeps the last signed tree head it
// accepted in its own state file and, on every run, checks that
//  1. the new head is signed by the log's key,
//  2. the new tree is an append-only extension of the one it saw before
//     (consistency proof), and
//  3. the entries the log serves really hash to the root it signed.
//
// A log operator who edits history cannot satisfy (2) for an auditor that
// remembers an earlier head, which is why heads are gossiped to many auditors.
func audit(log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	url := fs.String("url", env("LEDGER_URL", "http://ledger:8100"), "ledger base URL")
	pubPath := fs.String("pubkey", env("LEDGER_PUBLIC_KEY", "/pki/ledger-signing.pub"), "log public key")
	statePath := fs.String("state", env("AUDITOR_STATE", "/auditor/sth.json"), "last accepted tree head")
	kind := fs.String("kind", "", "with -ref: verify the inclusion proof of the latest entry for kind/ref")
	ref := fs.String("ref", "", "artefact reference to prove")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pub, err := loadPublicKey(*pubPath)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Second}

	var sth ledgerlog.SignedTreeHead
	if err := getJSON(client, *url+"/v1/sth", &sth); err != nil {
		return err
	}
	if err := ledgerlog.VerifySTH(pub, sth); err != nil {
		return fail(log, "signature", err)
	}
	log.Info("tree head signature valid", "tree_size", sth.Size, "root", sth.Root)

	var prev *ledgerlog.SignedTreeHead
	if raw, err := os.ReadFile(*statePath); err == nil {
		prev = &ledgerlog.SignedTreeHead{}
		if err := json.Unmarshal(raw, prev); err != nil {
			return err
		}
	}
	if prev != nil {
		if err := checkConsistency(client, *url, pub, *prev, sth); err != nil {
			return fail(log, "consistency", err)
		}
		log.Info("append-only since last audit", "previous_size", prev.Size, "current_size", sth.Size)
	} else {
		log.Info("first audit: no previous head to compare against")
	}

	if err := recompute(client, *url, sth); err != nil {
		return fail(log, "recompute", err)
	}
	log.Info("entries hash to the signed root", "entries", sth.Size)

	if *kind != "" && *ref != "" {
		if err := proveInclusion(log, client, *url, sth, *kind, *ref); err != nil {
			return fail(log, "inclusion", err)
		}
	}

	raw, _ := json.Marshal(sth)
	if err := os.WriteFile(*statePath, raw, 0o644); err != nil {
		return err
	}
	log.Info("audit passed", "tree_size", sth.Size)
	return nil
}

func checkConsistency(client *http.Client, url string, pub ed25519.PublicKey, prev, cur ledgerlog.SignedTreeHead) error {
	if err := ledgerlog.VerifySTH(pub, prev); err != nil {
		return fmt.Errorf("stored head: %w", err)
	}
	if cur.Size < prev.Size {
		return fmt.Errorf("log shrank from %d to %d entries", prev.Size, cur.Size)
	}
	oldRoot, err := ledgerlog.ParseHash(prev.Root)
	if err != nil {
		return err
	}
	newRoot, err := ledgerlog.ParseHash(cur.Root)
	if err != nil {
		return err
	}
	if prev.Size == 0 {
		return nil
	}
	var resp struct {
		Consistency []string `json:"consistency"`
	}
	if err := getJSON(client, fmt.Sprintf("%s/v1/proof/consistency?first=%d&second=%d", url, prev.Size, cur.Size), &resp); err != nil {
		return err
	}
	proof := make([]merkle.Hash, 0, len(resp.Consistency))
	for _, h := range resp.Consistency {
		p, err := ledgerlog.ParseHash(h)
		if err != nil {
			return err
		}
		proof = append(proof, p)
	}
	if !merkle.VerifyConsistency(prev.Size, cur.Size, oldRoot, newRoot, proof) {
		return fmt.Errorf("tree of size %d (root %s…) is not a prefix of the tree of size %d (root %s…): history was rewritten",
			prev.Size, prev.Root[:12], cur.Size, cur.Root[:12])
	}
	return nil
}

func recompute(client *http.Client, url string, sth ledgerlog.SignedTreeHead) error {
	var leaves []merkle.Hash
	for start := 0; start < sth.Size; start += 1000 {
		var page struct {
			Entries []struct {
				Entry ledgerlog.Entry `json:"entry"`
			} `json:"entries"`
		}
		if err := getJSON(client, fmt.Sprintf("%s/v1/entries?start=%d&end=%d", url, start, min(start+1000, sth.Size)), &page); err != nil {
			return err
		}
		for _, e := range page.Entries {
			leaves = append(leaves, merkle.LeafHash(e.Entry.Canonical()))
		}
	}
	if len(leaves) != sth.Size {
		return fmt.Errorf("log served %d entries for a head of size %d", len(leaves), sth.Size)
	}
	if got := ledgerlog.Hex(merkle.Root(leaves)); got != sth.Root {
		return fmt.Errorf("entries hash to %s…, head says %s…", got[:12], sth.Root[:12])
	}
	return nil
}

func getJSON(client *http.Client, url string, out any) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func fail(log *slog.Logger, check string, err error) error {
	log.Error("AUDIT FAILED", "check", check, "error", err)
	return errors.New("audit failed: " + check)
}

// proveInclusion checks one entry against the signed root using only its
// audit path: O(log n) hashes, no need to download the log.
func proveInclusion(log *slog.Logger, client *http.Client, url string, sth ledgerlog.SignedTreeHead, kind, ref string) error {
	var resp struct {
		Index     int             `json:"index"`
		TreeSize  int             `json:"tree_size"`
		Entry     ledgerlog.Entry `json:"entry"`
		AuditPath []string        `json:"audit_path"`
	}
	q := fmt.Sprintf("%s/v1/proof/inclusion?kind=%s&ref=%s&tree_size=%d", url, kind, ref, sth.Size)
	if err := getJSON(client, q, &resp); err != nil {
		return err
	}
	path := make([]merkle.Hash, 0, len(resp.AuditPath))
	for _, h := range resp.AuditPath {
		p, err := ledgerlog.ParseHash(h)
		if err != nil {
			return err
		}
		path = append(path, p)
	}
	root, err := ledgerlog.ParseHash(sth.Root)
	if err != nil {
		return err
	}
	leaf := merkle.LeafHash(resp.Entry.Canonical())
	if !merkle.VerifyInclusion(leaf, resp.Index, sth.Size, path, root) {
		return fmt.Errorf("entry %d (%s %s) is not in the signed tree", resp.Index, kind, ref)
	}
	log.Info("inclusion proved", "kind", kind, "ref", ref, "digest", resp.Entry.Digest, "index", resp.Index,
		"tree_size", sth.Size, "proof_hashes", len(path))
	return nil
}
