package raft

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: path}, nil
}

func (w *WAL) Append(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	if _, err := w.file.Write(data); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Replay(apply func(LogEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return err
		}
		if err := apply(entry); err != nil {
			return err
		}
	}

	_, err := w.file.Seek(0, 2)
	return err
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
