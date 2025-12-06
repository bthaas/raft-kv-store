package wal

import (
	"os"
	"testing"
)

func TestWAL(t *testing.T) {
	tmpFile := "test_wal.log"
	defer os.Remove(tmpFile)

	// Write entries
	w, err := NewWAL(tmpFile)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	entries := []LogEntry{
		{Command: "SET", Key: "k1", Value: "v1"},
		{Command: "DELETE", Key: "k1", Value: ""},
	}

	for _, e := range entries {
		if err := w.Write(e); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Recover
	recovered, err := Recover(tmpFile)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(recovered))
	}

	for i, e := range recovered {
		if e != entries[i] {
			t.Errorf("Mismatch at index %d: expected %+v, got %+v", i, entries[i], e)
		}
	}
}
