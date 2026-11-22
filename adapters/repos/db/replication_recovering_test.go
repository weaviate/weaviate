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
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	esync "github.com/weaviate/weaviate/entities/sync"
)

// The replica coordinator drops a replica only on a refusal that still names the recovering cause.
func TestPeerReadsOfARecoveringShardCarryTheRecoveringCause(t *testing.T) {
	const shardName = "S1"
	id := strfmt.UUID("8c29da7a-600a-43dc-85fb-83ab2b08c294")

	tests := []struct {
		name string
		read func(ctx context.Context, idx *Index) error
	}{
		{"CountObjects", func(ctx context.Context, idx *Index) error {
			_, err := idx.IncomingCountObjects(ctx, shardName)
			return err
		}},
		{"FetchObject", func(ctx context.Context, idx *Index) error {
			_, err := idx.FetchObject(ctx, shardName, id)
			return err
		}},
		{"FetchObjects", func(ctx context.Context, idx *Index) error {
			_, err := idx.FetchObjects(ctx, shardName, []strfmt.UUID{id})
			return err
		}},
		{"DigestObjects", func(ctx context.Context, idx *Index) error {
			_, err := idx.DigestObjects(ctx, shardName, []strfmt.UUID{id})
			return err
		}},
		{"FindUUIDs", func(ctx context.Context, idx *Index) error {
			_, err := idx.IncomingFindUUIDs(ctx, shardName, nil, 0)
			return err
		}},
		{"Search", func(ctx context.Context, idx *Index) error {
			_, _, _, err := idx.IncomingSearch(ctx, shardName, nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
			return err
		}},
		{"Aggregate", func(ctx context.Context, idx *Index) error {
			_, err := idx.IncomingAggregate(ctx, shardName, aggregation.Params{}, nil)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &RecoveringShard{LazyLoadShard: &LazyLoadShard{}}
			rec.blockLoad(enterrors.ErrShardRecovering)
			logger, _ := test.NewNullLogger()
			idx := newTestIndex(t, logger, "C1", nil, map[string]ShardLike{shardName: rec})
			idx.shardCreateLocks = esync.NewKeyRWLocker()

			err := tt.read(t.Context(), idx)
			require.ErrorIs(t, err, enterrors.ErrShardRecovering)
			require.True(t, errors.As(err, &enterrors.ErrUnprocessable{}), "a recovering shard answers 422, got %v", err)
		})
	}
}
