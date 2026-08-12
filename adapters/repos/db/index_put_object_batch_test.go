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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// TestPutObjectBatchReportsErrorAtEveryGroupPosition asserts that a failure of a
// whole shard group is reported at every position that group owns, and at no
// other. A position left nil is reported to the client as a written object, and
// a position belonging to another group overwrites that group's own result.
func TestPutObjectBatchReportsErrorAtEveryGroupPosition(t *testing.T) {
	className := "PutObjectBatchPositions"
	const schemaVersion = uint64(7)

	tests := []struct {
		name string
		// unresolvableFirst gives the object at position 0 an invalid id, so the
		// remaining objects form a group whose positions do not start at 0.
		unresolvableFirst bool
		schemaVersion     uint64
		setup             func(t *testing.T, idx *Index, shard *Shard)
		// wantErrAt maps a position to a substring of the error expected there;
		// every other position must carry no error.
		wantErrAt map[int]string
	}{
		{
			name:          "failed lookup",
			schemaVersion: schemaVersion,
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				// the caller's own wait succeeds, the one inside the shard lookup does not
				schemaReader := idx.schemaReader.(*schemaUC.MockSchemaReader)
				schemaReader.EXPECT().WaitForUpdate(mock.Anything, schemaVersion).Return(nil).Once()
				schemaReader.EXPECT().WaitForUpdate(mock.Anything, schemaVersion).
					Return(context.Canceled).Once()
			},
			wantErrAt: map[int]string{
				0: "wait for schema version",
				1: "wait for schema version",
				2: "wait for schema version",
			},
		},
		{
			name:              "panicking group",
			unresolvableFirst: true,
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				router := types.NewMockRouter(t)
				router.EXPECT().GetWriteReplicasLocation(className, mock.Anything, mock.Anything).
					RunAndReturn(func(string, string, string) (types.WriteReplicaSet, error) {
						panic("write replicas lookup panicked")
					})
				idx.router = router
			},
			wantErrAt: map[int]string{
				0: "parse uuid",
				1: "an unexpected error occurred",
				2: "an unexpected error occurred",
			},
		},
		{
			name: "read-only shard",
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				require.NoError(t, shard.SetStatusReadonly(statusReasonResourcePressure))
			},
			wantErrAt: map[int]string{
				0: "store is read-only",
				1: "store is read-only",
				2: "store is read-only",
			},
		},
		{
			name:              "read-only shard, group not starting at position 0",
			unresolvableFirst: true,
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				require.NoError(t, shard.SetStatusReadonly(statusReasonResourcePressure))
			},
			wantErrAt: map[int]string{
				0: "parse uuid",
				1: "store is read-only",
				2: "store is read-only",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, shard := refCountTestIndex(t, className)

			objs := []*storobj.Object{
				testObject(className), testObject(className), testObject(className),
			}
			if test.unresolvableFirst {
				objs[0].Object.ID = strfmt.UUID("not-a-uuid")
			}
			test.setup(t, idx, shard)

			out := idx.putObjectBatch(t.Context(), objs, nil, test.schemaVersion)

			require.Len(t, out, len(objs))
			for pos := range objs {
				want, ok := test.wantErrAt[pos]
				if !ok {
					require.NoErrorf(t, out[pos], "position %d belongs to no failing group", pos)
					continue
				}
				require.ErrorContainsf(t, out[pos], want,
					"position %d must carry the failure of its own group", pos)
			}
		})
	}
}
