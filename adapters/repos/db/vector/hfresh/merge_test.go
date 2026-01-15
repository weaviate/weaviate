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

package hfresh

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// Merge a posting that does not exist
func TestMergePostingThatDoesNotExist(t *testing.T) {
	tf := createHFreshIndex(t)

	err := tf.Index.doMerge(t.Context(), 0)
	require.NoError(t, err)

	require.Len(t, tf.Logs.Entries, 2)

	entry := tf.Logs.Entries[1]
	require.Equal(t, logrus.DebugLevel, entry.Level)
	require.Equal(
		t,
		"posting not found, skipping merge operation",
		entry.Message,
	)
	require.Equal(t, uint64(0), entry.Data["postingID"])

	tf.Index.minPostingSize = 10
	tf.Index.Centroids.Insert(0, &Centroid{
		Uncompressed: []float32{1, 1, 1},
		Compressed:   []byte{1, 1, 1},
		Deleted:      false,
	})

	err = tf.Index.doMerge(t.Context(), 0)
	require.NoError(t, err)

	require.Len(t, tf.Logs.Entries, 3)

	entry = tf.Logs.Entries[2]
	require.Equal(t, logrus.DebugLevel, entry.Level)
}

// Merge a posting that is empty

//
