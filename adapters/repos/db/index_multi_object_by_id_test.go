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
	"encoding/binary"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/entities/multi"
)

// TestMultiObjectByIDLocalReadError asserts that a failed read on a local shard
// surfaces to the caller. Returning the objects collected so far with a nil error
// would report a partial result as a complete one.
func TestMultiObjectByIDLocalReadError(t *testing.T) {
	className := "MultiObjectByIDLocalRead"

	tests := []struct {
		name string
		// corrupt reports whether the stored object is replaced by an
		// undecodable value before it is read back.
		corrupt   bool
		wantErr   bool
		wantFound bool
	}{
		{name: "readable object", corrupt: false, wantErr: false, wantFound: true},
		{name: "undecodable object", corrupt: true, wantErr: true, wantFound: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			shard, idx := testShard(t, ctx, className, func(i *Index) {
				i.shardResolver = resolver.NewShardResolver(className, false, i.getSchema)
			})

			obj := testObject(className)
			require.NoError(t, shard.PutObject(ctx, obj))

			if test.corrupt {
				key, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
				require.NoError(t, err)
				docID := make([]byte, 8)
				binary.LittleEndian.PutUint64(docID, obj.DocID)
				// marshaller version 2 does not exist, so decoding fails
				require.NoError(t, shard.Store().Bucket(helpers.ObjectsBucketLSM).
					Put(key, []byte{2}, lsmkv.WithSecondaryKey(
						helpers.ObjectsBucketLSMDocIDSecondaryIndex, docID)))
			}

			found, err := idx.multiObjectByID(ctx,
				[]multi.Identifier{{ID: obj.ID().String(), ClassName: className}}, "")

			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, found, 1)
			if test.wantFound {
				require.NotNil(t, found[0])
				require.Equal(t, obj.ID(), found[0].ID())
			}
		})
	}
}
