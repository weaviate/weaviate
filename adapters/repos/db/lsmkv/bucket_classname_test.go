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

package lsmkv

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// TestObjectsBucketStampsClassNameOnDecode is the bucket-level integration of
// the WS7 contract: the canonical class name attached to the bucket via
// WithClassName is what every reader sees on decoded objects, regardless of
// what the on-disk payload bytes carry in their class-name field. The test
// covers two on-disk shapes that the bucket-stamping path must override:
//
//   - the field is empty (writer had no class to stamp at marshal time, e.g.
//     wire-receive paths);
//   - the field is non-empty but doesn't match the bucket's canonical class
//     (data copied in from elsewhere, namespace-qualified writes from earlier
//     code, restored backups, etc.).
//
// In both cases the test flushes the memtable to a real on-disk segment so the
// read path is served from disk, then verifies both decoders:
//
//   - storobj.FromBinaryWithClassName(v, bucket.ClassName()) → bucket wins;
//   - storobj.FromBinary(v) (legacy single-arg) → falls back to on-disk.
func TestObjectsBucketStampsClassNameOnDecode(t *testing.T) {
	const wantClass = "Movies"

	cases := []struct {
		name           string
		marshaledClass string // value written into the on-disk class-name bytes
	}{
		{name: "empty on-disk class", marshaledClass: ""},
		{name: "different on-disk class", marshaledClass: "Books"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			noopCB := cyclemanager.NewCallbackGroupNoop()

			bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
				WithStrategy(StrategyReplace),
				WithClassName(wantClass),
			)
			require.NoError(t, err)
			defer bucket.Shutdown(ctx)

			className, err := bucket.ClassName()
			require.NoError(t, err)
			require.Equal(t, wantClass, className, "ClassName() must return the value supplied via WithClassName")

			obj := storobj.FromObject(
				&models.Object{
					Class:              tc.marshaledClass,
					CreationTimeUnix:   1,
					LastUpdateTimeUnix: 2,
					ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
					Properties: map[string]interface{}{
						"title": "The Matrix",
					},
				},
				[]float32{1, 2, 3},
				nil,
				nil,
			)
			obj.DocID = 7

			objBytes, err := obj.MarshalBinary()
			require.NoError(t, err)

			idBytes, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
			require.NoError(t, err)

			require.NoError(t, bucket.Put(idBytes, objBytes))

			// Force the data to disk so the read path serves it from an
			// on-disk segment rather than the memtable.
			require.NoError(t, bucket.FlushAndSwitch())

			cursor := bucket.Cursor()
			defer cursor.Close()

			count := 0
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				count++

				// Bucket-aware decode: the caller-supplied (bucket) className
				// must win, regardless of what's on disk.
				className, err := bucket.ClassName()
				require.NoError(t, err, "cannot get bucket class name")
				decodedWithClassName, err := storobj.FromBinaryDisk(v, className)
				require.NoError(t, err, "cannot unmarshal object via FromBinaryWithClassName")
				require.NotNil(t, decodedWithClassName)
				assert.Equal(t, wantClass, decodedWithClassName.Object.Class,
					"bucket must stamp its canonical className on the decoded object")
				assert.Equal(t, obj.ID(), decodedWithClassName.ID())
				assert.Equal(t, obj.DocID, decodedWithClassName.DocID)
				assert.Equal(t, obj.Vector, decodedWithClassName.Vector)
				assert.Equal(t, obj.Properties(), decodedWithClassName.Properties())

				// Legacy path: FromBinary (no caller className) falls back to
				// the on-disk value. This is the contract for wire-receive
				// callers that have no bucket in hand.
				decoded, err := storobj.FromBinaryNetwork(v)
				require.NoError(t, err, "cannot unmarshal object via FromBinary")
				require.NotNil(t, decoded)
				assert.Equal(t, tc.marshaledClass, decoded.Object.Class,
					"without caller-supplied className, decoded.Class must reflect the on-disk value")
				assert.Equal(t, obj.ID(), decoded.ID())
				assert.Equal(t, obj.DocID, decoded.DocID)
				assert.Equal(t, obj.Vector, decoded.Vector)
				assert.Equal(t, obj.Properties(), decoded.Properties())
			}
			require.Equal(t, 1, count, "expected exactly one object in the bucket")
		})
	}
}
