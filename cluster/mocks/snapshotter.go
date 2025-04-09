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

package mocks

import (
	"encoding/json"
	"io"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// MockSnapshotter implements the authorization.Snapshotter interface for testing
type MockSnapshotter struct {
	InputSnapshot    *authorization.Snapshot
	RestoreCalled    bool
	RestoredSnapshot *authorization.Snapshot
	SnapshotError    error // Add error for Snapshot method
	RestoreError     error // Add error for Restore method
}

// Snapshot returns a mock snapshot for testing
func (m *MockSnapshotter) Snapshot() (*authorization.Snapshot, error) {
	if m.SnapshotError != nil {
		return nil, m.SnapshotError
	}
	return m.InputSnapshot, nil
}

// Restore records that it was called and stores the restored data
func (m *MockSnapshotter) Restore(r io.Reader) error {
	m.RestoreCalled = true

	if m.RestoreError != nil {
		return m.RestoreError
	}

	// Try to decode the snapshot to capture the data
	var snapshot authorization.Snapshot
	if err := json.NewDecoder(r).Decode(&snapshot); err == nil {
		m.RestoredSnapshot = &snapshot
	}

	return nil
}
