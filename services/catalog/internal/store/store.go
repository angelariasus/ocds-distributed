// Package store keeps every causally concurrent version of a key (its
// siblings) in memory and appends each accepted write to a log so a restarted
// node comes back with the same state.
package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/angelariasus/ocds-distributed/services/catalog/internal/vclock"
)

type Version struct {
	Value     []byte       `json:"value"`
	Clock     vclock.Clock `json:"clock"`
	Tombstone bool         `json:"tombstone,omitempty"`
	UpdatedAt time.Time    `json:"updated_at"`
}

type entry struct {
	Key     string  `json:"key"`
	Version Version `json:"version"`
}

type Store struct {
	mu   sync.RWMutex
	data map[string][]Version
	wal  *os.File
}

// Open loads the log at path, or starts an in-memory store when path is empty.
func Open(path string) (*Store, error) {
	s := &Store{data: map[string][]Version{}}
	if path == "" {
		return s, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if err := s.replay(f); err != nil {
		f.Close()
		return nil, err
	}
	s.wal = f
	return s, nil
}

func (s *Store) replay(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 64<<20)
	for scanner.Scan() {
		var e entry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			// A torn final line after a crash is expected; anything else is corruption.
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		s.apply(e.Key, e.Version)
	}
	return scanner.Err()
}

func (s *Store) Close() error {
	if s.wal == nil {
		return nil
	}
	return s.wal.Close()
}

func (s *Store) Get(key string) []Version {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Version(nil), s.data[key]...)
}

// Put merges v into the key's siblings: versions v descends from are dropped,
// a v that is already dominated is ignored, and a concurrent v is kept
// alongside the others. It reports whether anything changed.
func (s *Store) Put(key string, v Version) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := s.apply(key, v)
	if !changed || s.wal == nil {
		return changed, nil
	}
	line, err := json.Marshal(entry{Key: key, Version: v})
	if err != nil {
		return true, err
	}
	if _, err := s.wal.Write(append(line, '\n')); err != nil {
		return true, err
	}
	return true, nil
}

func (s *Store) apply(key string, v Version) bool {
	kept := make([]Version, 0, len(s.data[key])+1)
	for _, existing := range s.data[key] {
		switch vclock.Compare(v.Clock, existing.Clock) {
		case vclock.Equal, vclock.Before:
			return false
		case vclock.Concurrent:
			kept = append(kept, existing)
		}
	}
	s.data[key] = append(kept, v)
	return true
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ForEach visits keys in sorted order with a read lock held.
func (s *Store) ForEach(fn func(key string, versions []Version)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fn(k, s.data[k])
	}
}
