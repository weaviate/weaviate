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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

// TestBatchWriteRefusalReportsOneErrorPerItem asserts that a batch refused as a
// whole reports an error for every item it was handed. The callers scatter the
// returned errors over the positions their shard group owns
// (Index.putObjectBatch, Index.AddReferencesBatch), so a shorter slice leaves
// the remaining positions nil, and a nil position is reported to the client as a
// written item. A batch of one hides this: the single error covers the single
// item. A refused delete also keeps the id at every position, because the
// verbose gRPC batch delete reply cannot encode a result without one.
func TestBatchWriteRefusalReportsOneErrorPerItem(t *testing.T) {
	className := "BatchWriteRefusal"

	idx, shard := refCountTestIndex(t, className)
	require.NoError(t, shard.SetStatusReadonly(statusReasonResourcePressure))
	unloadable := newColdShard(idx, shard.name+"_unloadable")
	_, writable := refCountTestIndex(t, className+"Writable")
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	tests := []struct {
		name string
		// wantErr is a substring of the refusal, so a batch that fails item by
		// item for an unrelated reason cannot stand in for the refused batch.
		wantErr string
		write   func(t *testing.T, count int) []error
	}{
		{
			name:    "read-only shard, objects",
			wantErr: "read-only",
			write: func(t *testing.T, count int) []error {
				return shard.PutObjectBatch(t.Context(), batchOfObjects(className, count))
			},
		},
		{
			name:    "read-only shard, references",
			wantErr: "read-only",
			write: func(t *testing.T, count int) []error {
				return shard.AddReferencesBatch(t.Context(), batchOfReferences(className, count))
			},
		},
		{
			name:    "unloadable shard, objects",
			wantErr: "memory pressure",
			write: func(t *testing.T, count int) []error {
				return unloadable.PutObjectBatch(t.Context(), batchOfObjects(className, count))
			},
		},
		{
			name:    "unloadable shard, references",
			wantErr: "memory pressure",
			write: func(t *testing.T, count int) []error {
				return unloadable.AddReferencesBatch(t.Context(), batchOfReferences(className, count))
			},
		},
		{
			name:    "read-only shard, deletes",
			wantErr: "read-only",
			write: func(t *testing.T, count int) []error {
				return deleteBatchErrors(t.Context(), t, shard, count)
			},
		},
		{
			name:    "unloadable shard, deletes",
			wantErr: "memory pressure",
			write: func(t *testing.T, count int) []error {
				return deleteBatchErrors(t.Context(), t, unloadable, count)
			},
		},
		{
			name:    "canceled context, deletes",
			wantErr: "begin batch",
			write: func(t *testing.T, count int) []error {
				return deleteBatchErrors(canceled, t, writable, count)
			},
		},
	}

	for _, test := range tests {
		for _, count := range []int{1, 2, 10, 1000} {
			t.Run(fmt.Sprintf("%s/%d", test.name, count), func(t *testing.T) {
				errs := test.write(t, count)

				require.Len(t, errs, count, "a refused batch must report one error per item")
				for pos := range errs {
					require.ErrorContainsf(t, errs[pos], test.wantErr,
						"position %d must carry the refusal", pos)
				}
			})
		}
	}
}

func batchOfObjects(className string, count int) []*storobj.Object {
	out := make([]*storobj.Object, count)
	for i := range out {
		out[i] = testObject(className)
	}
	return out
}

// deleteBatchErrors deletes count random ids from shard and returns the error at
// each position, after asserting that every position kept the id it was handed.
func deleteBatchErrors(ctx context.Context, t *testing.T, shard ShardLike, count int) []error {
	ids := make([]strfmt.UUID, count)
	for i := range ids {
		ids[i] = strfmt.UUID(uuid.NewString())
	}

	results := shard.DeleteObjectBatch(ctx, ids, time.Now(), false)

	require.Len(t, results, count, "a refused delete must report one result per id")
	errs := make([]error, count)
	for pos, result := range results {
		require.Equalf(t, ids[pos], result.UUID, "position %d must keep its id", pos)
		errs[pos] = result.Err
	}
	return errs
}

func batchOfReferences(className string, count int) objects.BatchReferences {
	out := make(objects.BatchReferences, count)
	for i := range out {
		out[i] = objects.BatchReference{
			OriginalIndex: i,
			From: &crossref.RefSource{
				Class:    schema.ClassName(className),
				Property: schema.PropertyName("toTarget"),
				TargetID: strfmt.UUID(uuid.NewString()),
			},
			To: &crossref.Ref{Class: "Target", TargetID: strfmt.UUID(uuid.NewString())},
		}
	}
	return out
}
