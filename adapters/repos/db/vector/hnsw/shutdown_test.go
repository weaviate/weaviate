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
	dropped bool
	err     error
}

func (c *dropCompressor) Drop() error {
	c.dropped = true
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
		wantErrs      []string
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

			if test.compressed {
				require.True(t, compressor.dropped)
				require.False(t, cache.dropped)
			} else {
				require.True(t, cache.dropped)
				require.False(t, compressor.dropped)
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
