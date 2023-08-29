//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helpers

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/tracker"
)

func TestMakePropertyPrefix(t *testing.T) {
	path := t.TempDir() + "/test.json"
	tracker, err := tracker.NewJsonPropertyIdTracker(path)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	_, err = tracker.CreateProperty("property")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	prefix := MakePropertyPrefix("property", tracker)
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
	expectedKey := []byte{1, 2, 3, 4, 5, 6}

	if !bytes.Equal(propertyKey, expectedKey) {
		t.Fatalf("Expected key to be %v, got %v", expectedKey, propertyKey)
	}
}

func TestMatchesPropertyKeyPrefix(t *testing.T) {
	prefix := []byte{1, 2, 3}
	key := []byte{1, 2, 3, 4, 5, 6}

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

func TestMakePropertyKeyWithEmptyPrefix(t *testing.T) {
	prefix := []byte{}
	key := []byte{4, 5, 6}

	propertyKey := MakePropertyKey(prefix, key)
	if propertyKey != nil {
		t.Fatalf("Expected nil for empty prefix, got %v", propertyKey)
	}
}

func TestMatchesPropertyKeyPrefixWithEmptyPrefix(t *testing.T) {
	prefix := []byte{}
	key := []byte{4, 5, 6, 1, 2, 3}

	matches := MatchesPropertyKeyPrefix(prefix, key)
	if matches {
		t.Fatalf("Expected false for empty prefix, got true")
	}
}

func TestUnMakePropertyKeyWithEmptyPrefix(t *testing.T) {
	prefix := []byte{}
	key := []byte{4, 5, 6, 1, 2, 3}

	unmadeKey := UnMakePropertyKey(prefix, key)
	if unmadeKey != nil {
		t.Fatalf("Expected nil for empty prefix, got %v", unmadeKey)
	}
}
