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

import "bytes"

// Snapshot sink implementation for testing
type SnapshotSink struct {
	Buffer     *bytes.Buffer
	WriteError error
}

func (t *SnapshotSink) Write(p []byte) (n int, err error) {
	if t.WriteError != nil {
		return 0, t.WriteError
	}
	return t.Buffer.Write(p)
}

func (t *SnapshotSink) Close() error {
	return nil
}

func (t *SnapshotSink) ID() string {
	return "test-snapshot-id"
}

func (t *SnapshotSink) Cancel() error {
	return nil
}
