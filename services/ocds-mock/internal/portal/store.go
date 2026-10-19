// Package portal serves a faithful subset of the OCDS portal API from locally
// stored monthly archives, so load and chaos experiments never touch the real
// government service.
package portal

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var archiveName = regexp.MustCompile(`^([a-z0-9_]+)-(\d{4})-(\d{2})\.json\.zip$`)

type File struct {
	Source    string
	Year      int
	Month     int
	Published time.Time
	Zip       []byte
	SHA256    string
}

func (f File) ID() string { return fmt.Sprintf("%s-%04d-%02d", f.Source, f.Year, f.Month) }

// Record keeps the raw JSON of a record next to the fields the cursor sorts on.
type Record struct {
	Raw      json.RawMessage
	Source   string
	TenderID string
	DateMs   int64
	OCID     string
	Date     string
}

type Store struct {
	Files   []File
	Records []Record // sorted in cursor order
}

func Load(dir string) (*Store, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{}
	for _, e := range entries {
		m := archiveName.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		f, records, err := parseArchive(m[1], atoi(m[2]), atoi(m[3]), data, info.ModTime())
		if err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		s.Files = append(s.Files, f)
		s.Records = append(s.Records, records...)
	}
	sort.Slice(s.Files, func(i, j int) bool { return s.Files[i].ID() > s.Files[j].ID() })
	sort.Slice(s.Records, func(i, j int) bool { return less(s.Records[j], s.Records[i]) })
	return s, nil
}

func parseArchive(source string, year, month int, data []byte, modTime time.Time) (File, []Record, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return File{}, nil, err
	}
	if len(zr.File) != 1 {
		return File{}, nil, fmt.Errorf("expected one member, found %d", len(zr.File))
	}
	rc, err := zr.File[0].Open()
	if err != nil {
		return File{}, nil, err
	}
	defer rc.Close()
	inner, err := io.ReadAll(rc)
	if err != nil {
		return File{}, nil, err
	}
	sum := sha256.Sum256(inner)

	var pkg struct {
		Records []json.RawMessage `json:"records"`
	}
	if err := json.Unmarshal(inner, &pkg); err != nil {
		return File{}, nil, err
	}
	records := make([]Record, 0, len(pkg.Records))
	for _, raw := range pkg.Records {
		r, err := parseRecord(raw, source)
		if err != nil {
			return File{}, nil, err
		}
		records = append(records, r)
	}
	f := File{Source: source, Year: year, Month: month, Published: modTime, Zip: data, SHA256: hex.EncodeToString(sum[:])}
	return f, records, nil
}

func parseRecord(raw json.RawMessage, source string) (Record, error) {
	var rec struct {
		OCID     string `json:"ocid"`
		Compiled struct {
			Date   string `json:"date"`
			Tender struct {
				ID string `json:"id"`
			} `json:"tender"`
		} `json:"compiledRelease"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		return Record{}, err
	}
	var ms int64
	if t, err := time.Parse(time.RFC3339Nano, rec.Compiled.Date); err == nil {
		ms = t.UnixMilli()
	}
	return Record{
		Raw:      raw,
		Source:   source,
		TenderID: rec.Compiled.Tender.ID,
		DateMs:   ms,
		OCID:     rec.OCID,
		Date:     rec.Compiled.Date,
	}, nil
}

// less orders records ascending by the portal's sort key; pages are served in
// descending order, which is why Load sorts with the arguments swapped.
func less(a, b Record) bool {
	if a.Source != b.Source {
		return a.Source < b.Source
	}
	if a.TenderID != b.TenderID {
		return a.TenderID < b.TenderID
	}
	if a.DateMs != b.DateMs {
		return a.DateMs < b.DateMs
	}
	return a.OCID < b.OCID
}

// Cursor mirrors the shape of the portal's searchAfter value: source,tenderId,dateMs,dateMs,ocid.
type Cursor struct {
	Source   string
	TenderID string
	DateMs   int64
	OCID     string
}

func (c Cursor) String() string {
	return fmt.Sprintf("%s,%s,%d,%d,%s", c.Source, c.TenderID, c.DateMs, c.DateMs, c.OCID)
}

func ParseCursor(s string) (Cursor, error) {
	parts := strings.SplitN(s, ",", 5)
	if len(parts) != 5 {
		return Cursor{}, fmt.Errorf("searchAfter must have 5 fields, got %d", len(parts))
	}
	ms, _ := strconv.ParseInt(parts[2], 10, 64)
	return Cursor{Source: parts[0], TenderID: parts[1], DateMs: ms, OCID: parts[4]}, nil
}

func (r Record) Cursor() Cursor {
	return Cursor{Source: r.Source, TenderID: r.TenderID, DateMs: r.DateMs, OCID: r.OCID}
}

// After returns up to size records strictly after the cursor in descending order.
func (s *Store) After(c *Cursor, size int) []Record {
	start := 0
	if c != nil {
		probe := Record{Source: c.Source, TenderID: c.TenderID, DateMs: c.DateMs, OCID: c.OCID}
		start = sort.Search(len(s.Records), func(i int) bool { return less(s.Records[i], probe) })
	}
	end := min(start+size, len(s.Records))
	return s.Records[start:end]
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
