package store

import (
	"testing"
)

func TestStore(t *testing.T) {
	s := NewStore()

	// Test Set and Get
	if err := s.Set("key1", "value1"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, ok := s.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Get failed: expected value1, got %s (ok=%v)", val, ok)
	}

	// Test Delete
	if err := s.Delete("key1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, ok = s.Get("key1")
	if ok {
		t.Errorf("Get after Delete failed: key should not exist")
	}
}

func TestStoreConcurrency(t *testing.T) {
	s := NewStore()
	done := make(chan bool)

	// Concurrent writes
	go func() {
		for i := 0; i < 1000; i++ {
			s.Set("key", "value")
		}
		done <- true
	}()

	// Concurrent reads
	go func() {
		for i := 0; i < 1000; i++ {
			s.Get("key")
		}
		done <- true
	}()

	<-done
	<-done
}
