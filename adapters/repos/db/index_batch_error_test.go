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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// TestBatchShardErrorMarksEveryItemInGroup asserts that a failure to acquire the
// shard is reported against every item routed to that shard. The batch APIs
// return one error slot per input and an empty slot means success, so reporting
// the failure once tells the client the rest of the group was written.
func TestBatchShardErrorMarksEveryItemInGroup(t *testing.T) {
	className := "BatchShardError"
	const (
		schemaVersion = uint64(7)
		itemsPerBatch = 3
	)

	tests := []struct {
		name string
		run  func(t *testing.T, idx *Index, count int) []error
	}{
		{
			name: "putObjectBatch",
			run: func(t *testing.T, idx *Index, count int) []error {
				batch := make([]*storobj.Object, count)
				for i := range batch {
					batch[i] = testObject(className)
				}
				return idx.putObjectBatch(t.Context(), batch, nil, schemaVersion)
			},
		},
		{
			name: "AddReferencesBatch",
			run: func(t *testing.T, idx *Index, count int) []error {
				refs := make(objects.BatchReferences, count)
				for i := range refs {
					refs[i] = objects.BatchReference{
						From: &crossref.RefSource{TargetID: strfmt.UUID(uuid.NewString())},
						To:   &crossref.Ref{TargetID: strfmt.UUID(uuid.NewString())},
					}
				}
				return idx.AddReferencesBatch(t.Context(), refs, nil, schemaVersion)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, _ := refCountTestIndex(t, className)

			// the caller's own wait succeeds, the one inside the shard lookup does not
			schemaReader := idx.schemaReader.(*schemaUC.MockSchemaReader)
			schemaReader.EXPECT().WaitForUpdate(mock.Anything, schemaVersion).Return(nil).Once()
			schemaReader.EXPECT().WaitForUpdate(mock.Anything, schemaVersion).
				Return(context.Canceled).Once()

			errs := test.run(t, idx, itemsPerBatch)

			require.Len(t, errs, itemsPerBatch)
			for pos, err := range errs {
				require.Errorf(t, err, "item %d was not written and must not be reported as written", pos)
			}
		})
	}
}

// TestPutObjectBatchPanicMarksItsOwnGroup asserts that a panic while writing a
// shard group is reported against that group's own batch positions. group.pos
// holds positions, so ranging over its indices instead stamps entries belonging
// to other groups and leaves the group's own tail empty, which reads as success.
func TestPutObjectBatchPanicMarksItsOwnGroup(t *testing.T) {
	className := "BatchPanicPositions"

	idx, _ := refCountTestIndex(t, className)

	router := types.NewMockRouter(t)
	router.EXPECT().GetWriteReplicasLocation(mock.Anything, mock.Anything, mock.Anything).
		Run(func(collection, tenant, shard string) { panic("write replica lookup failed") }).
		Return(types.WriteReplicaSet{}, nil).Maybe()
	idx.router = router

	// the unroutable object holds position 0 without joining a shard group, so
	// the group that panics covers positions 1 and 2
	unroutable := testObject(className)
	unroutable.Object.ID = "not-a-uuid"
	batch := []*storobj.Object{unroutable, testObject(className), testObject(className)}

	errs := idx.putObjectBatch(t.Context(), batch, nil, 0)

	require.Len(t, errs, len(batch))
	require.ErrorContains(t, errs[0], "not-a-uuid",
		"an object that never reached a shard keeps its own error")
	require.Error(t, errs[1])
	require.Error(t, errs[2],
		"an object in the panicking group must not be reported as written")
}
