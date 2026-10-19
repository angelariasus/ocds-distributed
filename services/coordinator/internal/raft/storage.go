package raft

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage writes the whole persistent state to a temporary file, fsyncs it
// and renames it over the previous one, so a crash leaves either the old or
// the new state on disk, never a torn mix.
type FileStorage struct {
	Path string
}

func (f FileStorage) Load() (PersistentState, error) {
	raw, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, err
	}
	var st PersistentState
	return st, json.Unmarshal(raw, &st)
}

func (f FileStorage) Save(st PersistentState) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.Path), ".raft-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), f.Path)
}

type MemoryStorage struct {
	mu sync.Mutex
	st PersistentState
}

func (m *MemoryStorage) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clone(m.st), nil
}

func (m *MemoryStorage) Save(st PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st = clone(st)
	return nil
}

func clone(st PersistentState) PersistentState {
	st.Log = append([]Entry(nil), st.Log...)
	return st
}
