package kv

import (
	"sync"

	"github.com/Mazennaji/raftkv/raft"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]string
	node *raft.Node
}

func NewStore(node *raft.Node) *Store {
	return &Store{
		data: make(map[string]string),
		node: node,
	}
}

func (s *Store) Apply(entry raft.LogEntry) {
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
	_, err := s.node.Propose(raft.Command{
		Op:    "PUT",
		Key:   key,
		Value: value,
	})
	return err
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}
