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

package sharding

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// fakeIncomingSchema reports a fixed applied RAFT index.
type fakeIncomingSchema struct {
	appliedIndex uint64
}

func (f fakeIncomingSchema) AppliedIndex() uint64 { return f.appliedIndex }

func (f fakeIncomingSchema) ReadOnlyClassWithVersion(context.Context, string, uint64) (*models.Class, error) {
	return nil, nil
}

// fakeIncomingIndex stands in for a resolved index. incomingRead only passes it to the caller's
// closure, so the embedded nil is never dereferenced.
type fakeIncomingIndex struct {
	RemoteIndexIncomingRepo
}

// fakeIncomingRepo hands out one index, or none when absent is set.
type fakeIncomingRepo struct {
	absent bool
	index  RemoteIndexIncomingRepo
}

func (f fakeIncomingRepo) GetIndexForIncomingSharding(schema.ClassName) RemoteIndexIncomingRepo {
	if f.absent {
		return nil
	}
	return f.index
}

// TestIncomingReadTellsLagFromAGenuineMiss pins what a replica answers when it cannot serve a
// read. The distinction is what stops a coordinator from spending its read budget on a node that
// will never hold the shard, and from giving up on one that is only behind.
func TestIncomingReadTellsLagFromAGenuineMiss(t *testing.T) {
	const (
		index = "Articles"
		shard = "tenant-7"
	)
	missingShard := enterrors.ErrLocalShardNotFound{Shard: shard}
	other := errors.New("bucket is corrupt")

	tests := []struct {
		name string
		// appliedIndex is what the replica has applied, requested what the read carries
		appliedIndex uint64
		requested    uint64
		indexAbsent  bool
		readErr      error

		wantUnprocessable bool
		wantLag           bool
		wantFinal         bool
	}{
		{
			name:              "behind the read's version, so a missing shard is lag",
			appliedIndex:      90,
			requested:         100,
			readErr:           missingShard,
			wantUnprocessable: true,
			wantLag:           true,
		},
		{
			name:              "caught up, so the same missing shard is final",
			appliedIndex:      100,
			requested:         100,
			readErr:           missingShard,
			wantUnprocessable: true,
			wantFinal:         true,
		},
		{
			name:              "ahead of the read's version is also caught up",
			appliedIndex:      150,
			requested:         100,
			readErr:           missingShard,
			wantUnprocessable: true,
			wantFinal:         true,
		},
		{
			name:              "no version sent at all cannot rule out lag",
			appliedIndex:      100,
			requested:         0,
			readErr:           missingShard,
			wantUnprocessable: true,
			wantLag:           true,
		},
		{
			name:              "a missing index while behind is lag",
			appliedIndex:      90,
			requested:         100,
			indexAbsent:       true,
			wantUnprocessable: true,
			wantLag:           true,
		},
		{
			name:              "a missing index while caught up is final",
			appliedIndex:      100,
			requested:         100,
			indexAbsent:       true,
			wantUnprocessable: true,
			wantFinal:         true,
		},
		{
			name:         "an unrelated failure is neither, whatever the versions",
			appliedIndex: 100,
			requested:    100,
			readErr:      other,
		},
		{
			name:         "and is not reclassified while behind either",
			appliedIndex: 90,
			requested:    100,
			readErr:      other,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rii := &RemoteIndexIncoming{
				repo:   fakeIncomingRepo{absent: tc.indexAbsent, index: fakeIncomingIndex{}},
				schema: fakeIncomingSchema{appliedIndex: tc.appliedIndex},
			}

			_, err := incomingRead(rii, index, shard, tc.requested,
				func(RemoteIndexIncomingRepo) (int, error) { return 0, tc.readErr })
			require.Error(t, err)

			assert.Equal(t, tc.wantUnprocessable, errors.As(err, &enterrors.ErrUnprocessable{}),
				"unprocessable is what the cluster API needs to pick 503 or 422 instead of 500")

			// Lag keeps the original miss wrapped, so the cluster API still reads it as
			// "not caught up" and answers 503.
			var lagShard enterrors.ErrLocalShardNotFound
			var lagIndex enterrors.ErrLocalIndexNotFound
			assert.Equal(t, tc.wantLag, errors.As(err, &lagShard) || errors.As(err, &lagIndex))

			// A final miss deliberately matches neither, or the coordinator keeps retrying.
			var final enterrors.ErrNotServedHere
			assert.Equal(t, tc.wantFinal, errors.As(err, &final))

			if !tc.wantUnprocessable {
				assert.ErrorIs(t, err, tc.readErr, "an unrelated failure must pass through untouched")
			}
		})
	}
}

// TestIncomingReadPassesResultsThrough guards the happy path: no classification, no error.
func TestIncomingReadPassesResultsThrough(t *testing.T) {
	rii := &RemoteIndexIncoming{
		repo:   fakeIncomingRepo{index: fakeIncomingIndex{}},
		schema: fakeIncomingSchema{appliedIndex: 100},
	}

	got, err := incomingRead(rii, "Articles", "tenant-7", 100,
		func(RemoteIndexIncomingRepo) (int, error) { return 42, nil })
	require.NoError(t, err)
	assert.Equal(t, 42, got)
}
