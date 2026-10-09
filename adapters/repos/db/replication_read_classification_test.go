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

package db

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// The replicated read path decides finality from placement, not from the schema version: a node
// caught up to the version a read was resolved against can still be waiting for a shard, because
// restore, freeze, COLD-to-HOT activation and lazy loading all materialise shards outside the
// apply path.
func TestClassifyReplicatedReadMissAsksPlacementNotTheVersion(t *testing.T) {
	const (
		class = "Articles"
		shard = "tenant-6"
		self  = "node-1"
	)
	missingShard := enterrors.ErrLocalShardNotFound{Shard: shard}
	missingIndex := enterrors.ErrLocalIndexNotFound{Index: class}
	other := errors.New("bucket is corrupt")

	tests := []struct {
		name         string
		appliedIndex uint64
		requested    uint64
		err          error
		replicas     []string
		replicasErr  error

		// wantFinal: rewritten to ErrNotServedHere, which the cluster API answers 422 and the
		// coordinator stops retrying. Otherwise the error is kept so the retry still happens.
		wantFinal bool
	}{
		{
			name:         "caught up and placement puts the shard elsewhere, so it is final",
			appliedIndex: 100,
			requested:    100,
			err:          missingShard,
			replicas:     []string{"node-2", "node-3"},
			wantFinal:    true,
		},
		{
			// The restore bug: calling this final cost every tenant its object count.
			name:         "caught up, but placement still puts the shard here, so it is coming",
			appliedIndex: 100,
			requested:    100,
			err:          missingShard,
			replicas:     []string{self, "node-2"},
		},
		{
			name:         "behind the read's version is lag whatever placement says",
			appliedIndex: 90,
			requested:    100,
			err:          missingShard,
			replicas:     []string{"node-2"},
		},
		{
			name:         "no version sent at all cannot rule out lag",
			appliedIndex: 100,
			requested:    0,
			err:          missingShard,
			replicas:     []string{"node-2"},
		},
		{
			name:         "a missing class placed elsewhere is final too",
			appliedIndex: 100,
			requested:    100,
			err:          missingIndex,
			replicas:     []string{"node-2"},
			wantFinal:    true,
		},
		{
			name:         "a missing class whose shard is placed here is still arriving",
			appliedIndex: 100,
			requested:    100,
			err:          missingIndex,
			replicas:     []string{self},
		},
		{
			name:         "placement that cannot be read leaves the miss retryable",
			appliedIndex: 100,
			requested:    100,
			err:          missingShard,
			replicasErr:  errors.New("schema is not readable"),
		},
		{
			name:         "an unrelated failure is not a miss and is not reclassified",
			appliedIndex: 100,
			requested:    100,
			err:          other,
			replicas:     []string{"node-2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := schemaUC.NewMockSchemaReader(t)
			reader.EXPECT().FSMAppliedIndex().Return(tc.appliedIndex).Maybe()
			getter := schemaUC.NewMockSchemaGetter(t)
			getter.EXPECT().ShardReplicas(class, shard).Return(tc.replicas, tc.replicasErr).Maybe()
			getter.EXPECT().NodeName().Return(self).Maybe()

			db := &DB{schemaReader: reader, schemaGetter: getter}
			err := db.classifyReplicatedReadMiss(tc.err, class, shard, tc.requested)
			require.Error(t, err)

			var final enterrors.ErrNotServedHere
			assert.Equal(t, tc.wantFinal, errors.As(err, &final),
				"a final miss must match nothing that reads as lag, or the coordinator keeps retrying")
			if !tc.wantFinal {
				assert.ErrorIs(t, err, tc.err, "anything but a final miss keeps the error it had")
			}
		})
	}
}

// A nil error is never classified, so a read that succeeded never touches the schema.
func TestClassifyReplicatedReadMissLeavesSuccessAlone(t *testing.T) {
	db := &DB{
		schemaReader: schemaUC.NewMockSchemaReader(t),
		schemaGetter: schemaUC.NewMockSchemaGetter(t),
	}
	require.NoError(t, db.classifyReplicatedReadMiss(nil, "Articles", "tenant-6", 100))
}
