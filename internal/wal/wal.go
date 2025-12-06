package wal

import (
	"encoding/gob"
	"io"
	"os"
	"sync"
)

// LogEntry represents a single command in the WAL.
type LogEntry struct {
	Term    int    // Term when entry was received by leader
	Command string // "SET", "DELETE"
	Key     string
	Value   string
}

// WAL represents a Write-Ahead Log backed by a file.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	enc  *gob.Encoder
}

// NewWAL creates or opens a WAL file at the specified path.
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		enc:  gob.NewEncoder(file),
	}, nil
}

// Write appends a log entry to the WAL file.
func (w *WAL) Write(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(entry)
}

// Recover reads the WAL file and returns all the log entries.
func Recover(path string) ([]LogEntry, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return []LogEntry{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []LogEntry
	dec := gob.NewDecoder(file)

	for {
		var entry LogEntry
		if err := dec.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
