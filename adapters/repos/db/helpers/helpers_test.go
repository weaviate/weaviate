package helpers

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/tracker"
)

// Mock tracker
type MockIdTracker struct{}

func (t MockIdTracker) GetIdForProperty(property string) (uint64, error) {
	return 1, nil
}

func TestMakePropertyPrefix(t *testing.T) {
	tracker ,err:= tracker.NewJsonPropertyIdTracker("/tmp/test.json")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	_, err = tracker.CreateProperty("property")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	prefix, err := MakePropertyPrefix("property", tracker)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expectedPrefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(expectedPrefix, 2)

	if !bytes.Equal(prefix, expectedPrefix) {
		t.Fatalf("Expected prefix to be %v, got %v", expectedPrefix, prefix)
	}
}

func TestMakePropertyKey(t *testing.T) {
	prefix := []byte{1, 2, 3}
	key := []byte{4, 5, 6}

	propertyKey := MakePropertyKey(prefix, key)
	expectedKey := []byte{4, 5, 6, 1, 2, 3}

	if !bytes.Equal(propertyKey, expectedKey) {
		t.Fatalf("Expected key to be %v, got %v", expectedKey, propertyKey)
	}
}

func TestMatchesPropertyKeyPrefix(t *testing.T) {
	prefix := []byte{1, 2, 3}
	key := []byte{4, 5, 6, 1, 2, 3}

	matches := MatchesPropertyKeyPrefix(prefix, key)
	if !matches {
		t.Fatalf("Expected key to match prefix, but it didn't")
	}
}

func TestUnMakePropertyKey(t *testing.T) {
	prefix := []byte{1, 2, 3}
	key := []byte{4, 5, 6, 1, 2, 3}

	unmadeKey := UnMakePropertyKey(prefix, key)
	expectedKey := []byte{4, 5, 6}

	if !bytes.Equal(unmadeKey, expectedKey) {
		t.Fatalf("Expected key to be %v, got %v", expectedKey, unmadeKey)
	}
}
