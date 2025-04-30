//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringsetrange

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestSerializeDeserializeMemtable_WithTempFile(t *testing.T) {
	original := &Memtable{
		additions: map[uint64]uint64{
			1: 100,
			2: 200,
		},
		deletions: map[uint64]struct{}{
			42: {},
		},
	}

	// Create temp file
	tmpFile, err := os.CreateTemp("", "memtable-*.bin")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up

	// Serialize to file
	if err := SerializeMemtable(original, tmpFile); err != nil {
		t.Fatalf("Serialization to file failed: %v", err)
	}
	tmpFile.Close()

	// Reopen for reading
	tmpFile, err = os.Open(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open temp file: %v", err)
	}
	defer tmpFile.Close()

	// Setup logger to test logs
	logger := logrus.New()
	logger.SetOutput(testWriter{t})
	logger.SetLevel(logrus.DebugLevel)

	// Deserialize
	restored, err := DeserializeMemtable(tmpFile, logger)
	if err != nil {
		t.Fatalf("Deserialization from file failed: %v", err)
	}

	// Check data matches
	for k, v := range original.additions {
		if restored.additions[k] != v {
			t.Errorf("Addition mismatch: key %d got %d, want %d", k, restored.additions[k], v)
		}
	}
	for k := range original.deletions {
		if _, ok := restored.deletions[k]; !ok {
			t.Errorf("Missing deletion: key %d", k)
		}
	}
}

// Redirect logrus logs to test logs
type testWriter struct {
	t *testing.T
}

func (tw testWriter) Write(p []byte) (n int, err error) {
	tw.t.Logf("%s", p)
	return len(p), nil
}
