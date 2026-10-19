package ocds

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// The portal's WAF rejects generic user agents with 403.
const userAgent = "ocds-distributed/0.1 (+https://github.com/angelariasus/ocds-distributed)"

type File struct {
	ID        string
	Source    string
	Year      int
	Month     int
	Published time.Time
	JSONURL   string
	SHAURL    string
}

type Client struct {
	baseURL    string
	http       *http.Client
	maxRetries int
	backoff    time.Duration
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		http:       &http.Client{Timeout: 60 * time.Second},
		maxRetries: 5,
		backoff:    time.Second,
	}
}

type filesPage struct {
	Pagination struct {
		HasNext bool `json:"has_next"`
	} `json:"pagination"`
	Results []struct {
		ID        string `json:"id"`
		Source    string `json:"source"`
		Year      string `json:"year"`
		Month     string `json:"month"`
		Timestamp string `json:"timestamp"`
		Files     struct {
			JSON string `json:"json"`
			SHA  string `json:"sha"`
		} `json:"files"`
	} `json:"results"`
}

func (c *Client) ListFiles(ctx context.Context) ([]File, error) {
	var files []File
	for page := 1; ; page++ {
		var p filesPage
		q := url.Values{"page": {strconv.Itoa(page)}, "paginateBy": {"100"}}
		if err := c.getJSON(ctx, c.baseURL+"/files?"+q.Encode(), &p); err != nil {
			return nil, fmt.Errorf("list files page %d: %w", page, err)
		}
		for _, r := range p.Results {
			f, err := toFile(r.ID, r.Source, r.Year, r.Month, r.Timestamp, r.Files.JSON, r.Files.SHA)
			if err != nil {
				return nil, err
			}
			files = append(files, f)
		}
		if !p.Pagination.HasNext {
			return files, nil
		}
	}
}

func toFile(id, source, year, month, ts, jsonURL, shaURL string) (File, error) {
	y, err := strconv.Atoi(year)
	if err != nil {
		return File{}, fmt.Errorf("file %s: bad year %q", id, year)
	}
	m, err := strconv.Atoi(month)
	if err != nil {
		return File{}, fmt.Errorf("file %s: bad month %q", id, month)
	}
	published, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return File{}, fmt.Errorf("file %s: bad timestamp %q", id, ts)
	}
	return File{ID: id, Source: source, Year: y, Month: m, Published: published, JSONURL: jsonURL, SHAURL: shaURL}, nil
}

func (c *Client) getJSON(ctx context.Context, url string, out any) error {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.backoff << (attempt - 1)):
			}
		}
		retry, err := c.tryGetJSON(ctx, url, out)
		if err == nil || !retry {
			return err
		}
		lastErr = err
	}
	return lastErr
}

func (c *Client) tryGetJSON(ctx context.Context, url string, out any) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return ctx.Err() == nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return true, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return false, json.NewDecoder(resp.Body).Decode(out)
}
