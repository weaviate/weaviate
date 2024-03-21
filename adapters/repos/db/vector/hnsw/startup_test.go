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

package hnsw

import (
	"context"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestRestoreFromDisk(t *testing.T) {
	tempPath := t.TempDir()

	noopManager := cyclemanager.NewCallbackGroupNoop()
	logger := logrus.New()
	logger.Out = io.Discard
	l, err := NewCommitLogger(tempPath, "testfile", logger, noopManager)
	assert.Nil(t, err)
	l.AddNode(&vertex{id: 1, level: 0})
	l.SetEntryPointWithMaxLayer(100_000, 0)
	l.Flush()
	l.Shutdown(context.Background())

	index, _ := New(
		Config{
			RootPath: tempPath,
			ID:       "testfile",
			MakeCommitLoggerThunk: func() (CommitLogger, error) {
				return l, nil
			},
			DistanceProvider: distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				if id == 1 {
					return []float32{1, 2, 3}, nil
				}
				return nil, nil
			},
			TempVectorForIDThunk: func(ctx context.Context, id uint64, container *VectorSlice) ([]float32, error) {
				return nil, nil
			},
		}, ent.NewDefaultUserConfig(),
		noopManager, noopManager, noopManager)
	defer index.Shutdown(context.Background())
	assert.NotNil(t, index)
	assert.Equal(t, uint64(1), index.entryPointID)
}
