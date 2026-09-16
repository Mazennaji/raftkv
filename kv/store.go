package kv

import (
	"fmt"
	"sync"

	"github.com/Mazennaji/raftkv/raft"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]string
	wal  *raft.WAL

	nextIndex uint64
}

func Open(walPath string) (*Store, error) {
	w, err := raft.NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("opening WAL: %w", err)
	}

	s := &Store{
		data:      make(map[string]string),
		wal:       w,
		nextIndex: 1,
	}

	err = w.Replay(func(entry LogEntryAlias) error {
		s.apply(entry)
		if entry.Index >= s.nextIndex {
			s.nextIndex = entry.Index + 1
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("replaying WAL: %w", err)
	}

	return s, nil
}

type LogEntryAlias = raft.LogEntry

func (s *Store) apply(entry raft.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch entry.Command.Op {
	case "PUT":
		s.data[entry.Command.Key] = entry.Command.Value
	case "DELETE":
		delete(s.data, entry.Command.Key)
	}
}

func (s *Store) Put(key, value string) error {
	entry := raft.LogEntry{
		Index: s.nextIndex,
		Term:  0,
		Command: raft.Command{
			Op:    "PUT",
			Key:   key,
			Value: value,
		},
	}

	if err := s.wal.Append(entry); err != nil {
		return fmt.Errorf("appending to WAL: %w", err)
	}

	s.apply(entry)
	s.nextIndex++
	return nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Close() error {
	return s.wal.Close()
}
