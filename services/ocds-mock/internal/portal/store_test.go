package portal

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeArchive(t *testing.T, dir, name string, tenderIDs []string) []byte {
	t.Helper()
	records := make([]map[string]any, 0, len(tenderIDs))
	for i, id := range tenderIDs {
		records = append(records, map[string]any{
			"ocid": "ocds-test-" + id,
			"compiledRelease": map[string]any{
				"date":   fmt.Sprintf("2026-09-%02dT00:00:00Z", i+1),
				"tender": map[string]any{"id": id},
			},
		})
	}
	inner, err := json.Marshal(map[string]any{"records": records})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("inner.json")
	w.Write(inner)
	zw.Close()
	if err := os.WriteFile(filepath.Join(dir, name), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return inner
}

func TestLoadSortsRecordsDescendingByTenderID(t *testing.T) {
	dir := t.TempDir()
	writeArchive(t, dir, "seace_v3-2026-09.json.zip", []string{"5000", "499999", "50000", "999999"})
	writeArchive(t, dir, "seace_v2-2010-01.json.zip", []string{"7"})

	s, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Files) != 2 || s.Files[0].ID() != "seace_v3-2026-09" {
		t.Fatalf("files = %+v", s.Files)
	}
	var got []string
	for _, r := range s.Records {
		got = append(got, r.Source+"/"+r.TenderID)
	}
	want := []string{"seace_v3/999999", "seace_v3/50000", "seace_v3/5000", "seace_v3/499999", "seace_v2/7"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestAfterWalksEveryRecordExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	ids := make([]string, 0, 250)
	for i := 0; i < 250; i++ {
		ids = append(ids, fmt.Sprintf("%d", 1000+i*7))
	}
	writeArchive(t, dir, "seace_v3-2026-09.json.zip", ids)
	s, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]int{}
	var cursor *Cursor
	for {
		page := s.After(cursor, 100)
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			seen[r.OCID]++
		}
		c := page[len(page)-1].Cursor()
		parsed, err := ParseCursor(c.String())
		if err != nil {
			t.Fatal(err)
		}
		cursor = &parsed
	}
	if len(seen) != len(ids) {
		t.Fatalf("saw %d records, want %d", len(seen), len(ids))
	}
	for ocid, n := range seen {
		if n != 1 {
			t.Fatalf("%s seen %d times", ocid, n)
		}
	}
}

func TestAfterSupportsJumpingIntoTheKeyspace(t *testing.T) {
	dir := t.TempDir()
	writeArchive(t, dir, "seace_v3-2026-09.json.zip", []string{"5000", "499999", "50000", "999999", "500000"})
	s, _ := Load(dir)

	c, _ := ParseCursor("seace_v3,500000,0,0,x")
	page := s.After(&c, 10)
	var got []string
	for _, r := range page {
		got = append(got, r.TenderID)
	}
	if fmt.Sprint(got) != fmt.Sprint([]string{"50000", "5000", "499999"}) {
		t.Fatalf("got %v", got)
	}
}

func TestSHAIsOfInnerJSON(t *testing.T) {
	dir := t.TempDir()
	inner := writeArchive(t, dir, "seace_v3-2026-09.json.zip", []string{"1"})
	s, _ := Load(dir)
	sum := sha256.Sum256(inner)
	if s.Files[0].SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("sha mismatch")
	}
}
