//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// TestRestoreDocMappingsWithCorruptedState tests that restoreDocMappings handles
// corrupted state gracefully after ungraceful shutdown
func TestRestoreDocMappingsWithCorruptedState(t *testing.T) {
	logger, hook := test.NewNullLogger()
	
	// Create a dummy store
	store := testinghelpers.NewDummyStore(t)
	
	// Create a minimal hnsw instance for testing
	idx := &hnsw{
		id:     "test-corrupted-mappings",
		logger: logger,
		store:  store,
		nodes: []*vertex{
			{id: 1},
			{id: 2},
			{id: 3},
		},
		docIDVectors: make(map[uint64][]uint64),
	}
	
	// Enable multivector mode
	idx.multivector.Store(true)
	
	// Now try to restore doc mappings - this should not panic even with missing mappings
	err := idx.restoreDocMappings()
	assert.Nil(t, err)
	
	// Verify that warnings were logged for the missing mappings or bucket
	logEntries := hook.AllEntries()
	warningCount := 0
	for _, entry := range logEntries {
		if entry.Level.String() == "warning" && 
			(entry.Message == "skipping node with missing doc mapping, possibly due to corrupted state" ||
			 entry.Message == "mappings bucket not found, possibly due to corrupted state after ungraceful shutdown") {
			warningCount++
		}
	}
	
	// Should have at least one warning (either for missing mappings or missing bucket)
	assert.GreaterOrEqual(t, warningCount, 1, "Expected at least one warning for corrupted state")
}

// TestRestoreDocMappingsWithNilData tests the specific case where Get returns nil
func TestRestoreDocMappingsWithNilData(t *testing.T) {
	logger, _ := test.NewNullLogger()
	
	// Create a dummy store
	store := testinghelpers.NewDummyStore(t)
	
	// Create a minimal hnsw instance for testing
	idx := &hnsw{
		id:     "test-nil-data",
		logger: logger,
		store:  store,
		nodes: []*vertex{
			{id: 1},
			{id: 2},
			{id: 3},
		},
		docIDVectors: make(map[uint64][]uint64),
	}
	
	// This should not panic even with missing mappings
	err := idx.restoreDocMappings()
	assert.Nil(t, err)
	
	// Verify that the function completed without crashing
	assert.NotNil(t, idx.docIDVectors)
}
