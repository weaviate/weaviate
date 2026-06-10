//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modulestorage

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestStorageBucket_ScanMissingBucket pins the fix for a nil-bucket panic in
// Scan: when the bolt bucket for the key was never created, Scan must behave
// like Get/Put (which nil-check) and yield an empty scan rather than panicking
// on b.Cursor(). A read-only-aware module storage that skips bucket creation is
// the first path to reach this.
func TestStorageBucket_ScanMissingBucket(t *testing.T) {
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	r, err := NewRepo(dir, logger)
	require.Nil(t, err)

	// A storageBucket whose bucket was never created (init() not called).
	sb := &storageBucket{bucketKey: []byte("never-created"), repo: r}

	calls := 0
	err = sb.Scan(func(k, v []byte) (bool, error) {
		calls++
		return true, nil
	})
	require.Nil(t, err)
	require.Equal(t, 0, calls, "scanning an absent bucket must yield nothing and must not panic")
}
