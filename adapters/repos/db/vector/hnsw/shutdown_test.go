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

package hnsw

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type unregisterCallbackCtrl struct {
	cyclemanager.CycleCallbackCtrl
	err error
}

func (c *unregisterCallbackCtrl) Unregister(ctx context.Context) error {
	return c.err
}

type dropCompressor struct {
	compressionhelpers.VectorCompressor
	drops int
	err   error
}

func (c *dropCompressor) Drop() error {
	c.drops++
	return c.err
}

func TestShutdownReleasesVectors(t *testing.T) {
	commitLogErr := errors.New("commit log is stuck")
	unregisterErr := errors.New("cycle is stuck")
	compressorErr := errors.New("compressed store is stuck")

	tests := []struct {
		name          string
		commitLogErr  error
		unregisterErr error
		compressed    bool
		compressorErr error
		// repeat the teardown to pin that the vectors are released only once
		shutdownTwice     bool
		dropAfterShutdown bool
		wantErrs          []string
	}{
		{
			name: "no failure",
		},
		{
			name:         "commit log shutdown fails",
			commitLogErr: commitLogErr,
			wantErrs:     []string{"shutdown commit log", commitLogErr.Error()},
		},
		{
			name:          "tombstone cleanup unregister fails",
			unregisterErr: unregisterErr,
			wantErrs:      []string{"unregister tombstone cleanup cycle", unregisterErr.Error()},
		},
		{
			name:          "commit log shutdown and unregister both fail",
			commitLogErr:  commitLogErr,
			unregisterErr: unregisterErr,
			wantErrs: []string{
				"shutdown commit log", commitLogErr.Error(),
				"unregister tombstone cleanup cycle", unregisterErr.Error(),
			},
		},
		{
			name:       "compressed, no failure",
			compressed: true,
		},
		{
			name:          "compressed, commit log shutdown and unregister both fail",
			commitLogErr:  commitLogErr,
			unregisterErr: unregisterErr,
			compressed:    true,
			wantErrs: []string{
				"shutdown commit log", commitLogErr.Error(),
				"unregister tombstone cleanup cycle", unregisterErr.Error(),
			},
		},
		{
			name:          "compressed, every step fails",
			commitLogErr:  commitLogErr,
			unregisterErr: unregisterErr,
			compressed:    true,
			compressorErr: compressorErr,
			wantErrs: []string{
				"shutdown commit log", commitLogErr.Error(),
				"unregister tombstone cleanup cycle", unregisterErr.Error(),
				"drop compressed store", compressorErr.Error(),
			},
		},
		{
			name:          "second shutdown after a failed one",
			commitLogErr:  commitLogErr,
			shutdownTwice: true,
			wantErrs:      []string{"shutdown commit log", commitLogErr.Error()},
		},
		{
			name:          "compressed, second shutdown after a failed one",
			commitLogErr:  commitLogErr,
			compressed:    true,
			shutdownTwice: true,
			wantErrs:      []string{"shutdown commit log", commitLogErr.Error()},
		},
		{
			name:              "drop after shutdown",
			dropAfterShutdown: true,
		},
		{
			name:              "compressed, drop after shutdown",
			compressed:        true,
			dropAfterShutdown: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := newFakeCache()
			compressor := &dropCompressor{err: test.compressorErr}
			index := &hnsw{
				shutdownCtxCancel:            func() {},
				commitLog:                    &failingCommitLogger{shutdownErr: test.commitLogErr},
				tombstoneCleanupCallbackCtrl: &unregisterCallbackCtrl{err: test.unregisterErr},
				cache:                        cache,
				compressor:                   compressor,
			}
			index.compressed.Store(test.compressed)

			err := index.Shutdown(context.Background())
			if test.shutdownTwice {
				err = index.Shutdown(context.Background())
			}
			if test.dropAfterShutdown {
				require.NoError(t, index.Drop(context.Background(), true))
			}

			if test.compressed {
				require.Equal(t, 1, compressor.drops)
				require.Equal(t, 0, cache.drops)
			} else {
				require.Equal(t, 1, cache.drops)
				require.Equal(t, 0, compressor.drops)
			}

			if len(test.wantErrs) == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range test.wantErrs {
				require.ErrorContains(t, err, want)
			}
		})
	}
}
