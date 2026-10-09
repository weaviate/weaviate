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

// fakeIncomingSchema reports a fixed applied RAFT index and a fixed placement.
type fakeIncomingSchema struct {
	appliedIndex uint64
	replicas     []string
	replicasErr  error
}

func (f fakeIncomingSchema) FSMAppliedIndex() uint64 { return f.appliedIndex }

func (f fakeIncomingSchema) ShardReplicas(class, shard string) ([]string, error) {
	return f.replicas, f.replicasErr
}

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

// Only a miss the replica is already current enough to be sure about is rewritten, and that is
// the whole point: a lagging replica keeps the error it had, so the retry that outlasts schema
// propagation still happens, while a replica that will never hold the shard says so once.
func TestIncomingReadTellsLagFromAGenuineMiss(t *testing.T) {
	const (
		index = "Articles"
		shard = "tenant-7"
	)
	missingShard := enterrors.ErrLocalShardNotFound{Shard: shard}
	other := errors.New("bucket is corrupt")
	// elsewhere excludes this node, here includes it.
	elsewhere, here := []string{"node-2"}, []string{"node-1", "node-2"}

	tests := []struct {
		name string
		// appliedIndex is what the replica has applied, requested what the read carries
		appliedIndex uint64
		requested    uint64
		indexAbsent  bool
		readErr      error
		// replicas is what placement says holds the shard, replicasErr that it cannot say.
		replicas    []string
		replicasErr error

		// wantFinal: rewritten to ErrNotServedHere, which the cluster API answers 422.
		// wantUnchanged: the error the caller already produced, left alone.
		wantFinal     bool
		wantUnchanged bool
	}{
		{
			name:          "behind the read's version, so a missing shard stays retryable",
			appliedIndex:  90,
			requested:     100,
			readErr:       missingShard,
			replicas:      elsewhere,
			wantUnchanged: true,
		},
		{
			name:         "caught up, so the same missing shard is final",
			appliedIndex: 100,
			requested:    100,
			readErr:      missingShard,
			replicas:     elsewhere,
			wantFinal:    true,
		},
		{
			name:         "ahead of the read's version is also caught up",
			appliedIndex: 150,
			requested:    100,
			readErr:      missingShard,
			replicas:     elsewhere,
			wantFinal:    true,
		},
		{
			name:          "no version sent at all cannot rule out lag",
			appliedIndex:  100,
			requested:     0,
			readErr:       missingShard,
			replicas:      elsewhere,
			wantUnchanged: true,
		},
		{
			name:         "a missing index while caught up is final",
			appliedIndex: 100,
			requested:    100,
			indexAbsent:  true,
			replicas:     elsewhere,
			wantFinal:    true,
		},
		{
			// The bug this rule exists for: restore, freeze, COLD-to-HOT activation and lazy
			// loading materialise shards outside the apply path, so the schema reads current
			// while the shard is still arriving. Calling that final cost a restored backup
			// every tenant's object count.
			name:          "caught up, but placement still puts the shard here, so it is coming",
			appliedIndex:  100,
			requested:     100,
			readErr:       missingShard,
			replicas:      here,
			wantUnchanged: true,
		},
		{
			name:          "a missing index whose shard is placed here is coming too",
			appliedIndex:  100,
			requested:     100,
			indexAbsent:   true,
			replicas:      here,
			wantUnchanged: true,
		},
		{
			name:          "placement that cannot be read leaves the miss retryable",
			appliedIndex:  100,
			requested:     100,
			readErr:       missingShard,
			replicasErr:   errors.New("schema is not readable"),
			wantUnchanged: true,
		},
		{
			name:          "an unrelated failure is neither, whatever the versions",
			appliedIndex:  100,
			requested:     100,
			readErr:       other,
			replicas:      elsewhere,
			wantUnchanged: true,
		},
		{
			name:          "and is not reclassified while behind either",
			appliedIndex:  90,
			requested:     100,
			readErr:       other,
			replicas:      elsewhere,
			wantUnchanged: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rii := &RemoteIndexIncoming{
				repo: fakeIncomingRepo{absent: tc.indexAbsent, index: fakeIncomingIndex{}},
				schema: fakeIncomingSchema{
					appliedIndex: tc.appliedIndex,
					replicas:     tc.replicas,
					replicasErr:  tc.replicasErr,
				},
				nodeName: "node-1",
			}

			_, err := incomingRead(rii, index, shard, tc.requested,
				func(RemoteIndexIncomingRepo) (int, error) { return 0, tc.readErr })
			require.Error(t, err)

			var final enterrors.ErrNotServedHere
			assert.Equal(t, tc.wantFinal, errors.As(err, &final),
				"a final miss must match nothing that reads as lag, or the coordinator keeps retrying")

			if tc.wantUnchanged {
				want := tc.readErr
				if tc.indexAbsent {
					want = enterrors.ErrLocalIndexNotFound{Index: index}
				}
				assert.ErrorIs(t, err, want, "lag must keep the error it had")
			}
		})
	}
}

// The one miss the facade has always wrapped itself: a class this node does not hold reads as
// not-ready while lag cannot be ruled out.
func TestIncomingReadKeepsAMissingIndexNotReady(t *testing.T) {
	rii := &RemoteIndexIncoming{
		repo:   fakeIncomingRepo{absent: true},
		schema: fakeIncomingSchema{appliedIndex: 90},
	}

	_, err := incomingRead(rii, "Articles", "tenant-7", 100,
		func(RemoteIndexIncomingRepo) (int, error) { return 0, nil })
	require.Error(t, err)

	assert.True(t, errors.As(err, &enterrors.ErrUnprocessable{}))
	var missing enterrors.ErrLocalIndexNotFound
	assert.True(t, errors.As(err, &missing), "still reads as not caught up")
}

// The happy path: no classification, no error.
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
