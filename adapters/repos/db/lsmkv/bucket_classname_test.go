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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// TestObjectsBucketStampsClassNameOnDecode locks the bucket-level contract:
// the class name supplied to a bucket via WithClassName is what every *Disk
// decoder stamps on the decoded object, regardless of what the on-disk
// class-name field carries. The Network decoders mirror the on-disk value
// instead.
//
// The test parameterises the on-disk class-name field over two shapes — empty
// and non-empty-but-mismatched — and exercises every decoder entry point on
// *storobj.Object after a FlushAndSwitch so the read path is served from a
// real on-disk segment rather than the memtable:
//
//   - storobj.FromBinaryDisk / FromBinaryNetwork
//   - storobj.FromBinaryOptionalDisk / FromBinaryOptionalNetwork
//   - storobj.FromBinaryUUIDOnlyDisk (no Network counterpart)
//   - (*Object).UnmarshalBinaryDisk / (*Object).UnmarshalBinaryNetwork
func TestObjectsBucketStampsClassNameOnDecode(t *testing.T) {
	cases := []struct {
		name           string
		marshaledClass string // value written into the on-disk class-name bytes
		wantClass      string // expected class name after decoding
		expectedErr    bool   // expected error substring, if any
	}{
		{name: "empty on-disk class", marshaledClass: "", wantClass: "Movies", expectedErr: false},
		{name: "different on-disk class", marshaledClass: "Books", wantClass: "Movies", expectedErr: false},
		{name: "empty on-disk class, empty bucket class", marshaledClass: "", wantClass: "", expectedErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			noopCB := cyclemanager.NewCallbackGroupNoop()

			bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
				WithStrategy(StrategyReplace),
				WithClassName(tc.wantClass),
			)
			require.NoError(t, err)
			defer bucket.Shutdown(ctx)

			className, err := bucket.ClassName()
			if tc.expectedErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantClass, className, "ClassName() must return the value supplied via WithClassName")

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

				// FromBinaryDisk: caller-supplied (bucket) className wins
				// regardless of what's on disk.
				decodedDisk, err := storobj.FromBinaryDisk(v, className)
				require.NoError(t, err, "cannot unmarshal via FromBinaryDisk")
				require.NotNil(t, decodedDisk)
				assert.Equal(t, tc.wantClass, decodedDisk.Object.Class,
					"FromBinaryDisk must stamp the bucket's canonical className")
				assert.Equal(t, obj.ID(), decodedDisk.ID())
				assert.Equal(t, obj.DocID, decodedDisk.DocID)
				assert.Equal(t, obj.Vector, decodedDisk.Vector)
				assert.Equal(t, obj.Properties(), decodedDisk.Properties())

				// FromBinaryNetwork: no caller className; reflects the
				// on-disk value, or errors if the on-disk class is empty too.
				decodedNet, err := storobj.FromBinaryNetwork(v)
				if tc.marshaledClass == "" {
					require.Error(t, err, "FromBinaryNetwork must error when caller and on-disk classes are both empty")
				} else {
					require.NoError(t, err, "cannot unmarshal via FromBinaryNetwork")
					require.NotNil(t, decodedNet)
					assert.Equal(t, tc.marshaledClass, decodedNet.Object.Class,
						"FromBinaryNetwork must reflect the on-disk class-name value")
					assert.Equal(t, obj.ID(), decodedNet.ID())
					assert.Equal(t, obj.DocID, decodedNet.DocID)
					assert.Equal(t, obj.Vector, decodedNet.Vector)
					assert.Equal(t, obj.Properties(), decodedNet.Properties())
				}

				// FromBinaryOptionalDisk: same Disk-side contract for the
				// optional/additional decoder.
				decodedOptDisk, err := storobj.FromBinaryOptionalDisk(v, className,
					additional.Properties{Vector: true}, nil)
				require.NoError(t, err, "cannot unmarshal via FromBinaryOptionalDisk")
				require.NotNil(t, decodedOptDisk)
				assert.Equal(t, tc.wantClass, decodedOptDisk.Object.Class,
					"FromBinaryOptionalDisk must stamp the bucket's canonical className")
				assert.Equal(t, obj.ID(), decodedOptDisk.ID())
				assert.Equal(t, obj.DocID, decodedOptDisk.DocID)
				assert.Equal(t, obj.Vector, decodedOptDisk.Vector)

				// FromBinaryOptionalNetwork: optional/additional decoder
				// without a caller className — falls back to on-disk, or
				// errors if the on-disk class is empty too.
				decodedOptNet, err := storobj.FromBinaryOptionalNetwork(v,
					additional.Properties{Vector: true}, nil)
				if tc.marshaledClass == "" {
					require.Error(t, err, "FromBinaryOptionalNetwork must error when caller and on-disk classes are both empty")
				} else {
					require.NoError(t, err, "cannot unmarshal via FromBinaryOptionalNetwork")
					require.NotNil(t, decodedOptNet)
					assert.Equal(t, tc.marshaledClass, decodedOptNet.Object.Class,
						"FromBinaryOptionalNetwork must reflect the on-disk class-name value")
					assert.Equal(t, obj.ID(), decodedOptNet.ID())
					assert.Equal(t, obj.DocID, decodedOptNet.DocID)
					assert.Equal(t, obj.Vector, decodedOptNet.Vector)
				}

				// FromBinaryUUIDOnlyDisk: header-only fast path. Decodes ID,
				// DocID, timestamps, and class. Vector and Properties are
				// intentionally not populated, so we don't assert on them.
				decodedUUIDOnly, err := storobj.FromBinaryUUIDOnlyDisk(v, className)
				require.NoError(t, err, "cannot unmarshal via FromBinaryUUIDOnlyDisk")
				require.NotNil(t, decodedUUIDOnly)
				assert.Equal(t, tc.wantClass, decodedUUIDOnly.Object.Class,
					"FromBinaryUUIDOnlyDisk must stamp the bucket's canonical className")
				assert.Equal(t, obj.ID(), decodedUUIDOnly.ID())
				assert.Equal(t, obj.DocID, decodedUUIDOnly.DocID)

				// (*Object).UnmarshalBinaryDisk: object-method form of the
				// Disk decoder. Same precedence contract as FromBinaryDisk.
				var unmarshalDisk storobj.Object
				require.NoError(t, unmarshalDisk.UnmarshalBinaryDisk(v, className),
					"cannot unmarshal via UnmarshalBinaryDisk")
				assert.Equal(t, tc.wantClass, unmarshalDisk.Object.Class,
					"UnmarshalBinaryDisk must stamp the bucket's canonical className")
				assert.Equal(t, obj.ID(), unmarshalDisk.ID())
				assert.Equal(t, obj.DocID, unmarshalDisk.DocID)
				assert.Equal(t, obj.Vector, unmarshalDisk.Vector)
				assert.Equal(t, obj.Properties(), unmarshalDisk.Properties())

				// (*Object).UnmarshalBinaryNetwork: object-method form of the
				// Network decoder. Falls back to on-disk class, or errors
				// if the on-disk class is empty too.
				var unmarshalNet storobj.Object
				if tc.marshaledClass == "" {
					require.Error(t, unmarshalNet.UnmarshalBinaryNetwork(v),
						"UnmarshalBinaryNetwork must error when caller and on-disk classes are both empty")
				} else {
					require.NoError(t, unmarshalNet.UnmarshalBinaryNetwork(v),
						"cannot unmarshal via UnmarshalBinaryNetwork")
					assert.Equal(t, tc.marshaledClass, unmarshalNet.Object.Class,
						"UnmarshalBinaryNetwork must reflect the on-disk class-name value")
					assert.Equal(t, obj.ID(), unmarshalNet.ID())
					assert.Equal(t, obj.DocID, unmarshalNet.DocID)
					assert.Equal(t, obj.Vector, unmarshalNet.Vector)
					assert.Equal(t, obj.Properties(), unmarshalNet.Properties())
				}
			}
			require.Equal(t, 1, count, "expected exactly one object in the bucket")

			// bucket.IterateObjects: bucket-side helper. Internally calls
			// bucket.ClassName() + FromBinaryDisk, so the canonical class
			// must be stamped on every yielded object regardless of what
			// the on-disk bytes carry.
			var iterated []*storobj.Object
			require.NoError(t, bucket.IterateObjects(ctx, func(o *storobj.Object) error {
				iterated = append(iterated, o)
				return nil
			}))
			require.Len(t, iterated, 1)
			assert.Equal(t, tc.wantClass, iterated[0].Object.Class,
				"IterateObjects must stamp the bucket's canonical className")
			assert.Equal(t, obj.ID(), iterated[0].ID())
			assert.Equal(t, obj.DocID, iterated[0].DocID)
		})
	}
}

// TestObjectsBucket_RoundTripWithSkipClassName covers the writer-side skip
// path: bytes produced via MarshalBinaryDisk(true) carry no className body
// (length prefix is 0) yet round-trip cleanly through the bucket and the
// *Disk decoders, which stamp the bucket's canonical className. The matching
// *Network decoders must error because the on-disk class is empty.
func TestObjectsBucket_RoundTripWithSkipClassName(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace),
		WithClassName("Movies"),
	)
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	className, err := bucket.ClassName()
	require.NoError(t, err)
	require.Equal(t, "Movies", className)

	obj := storobj.FromObject(
		&models.Object{
			Class:              "Movies",
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

	objBytes, err := obj.MarshalBinaryDisk(true)
	require.NoError(t, err)

	idBytes, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
	require.NoError(t, err)

	require.NoError(t, bucket.Put(idBytes, objBytes))
	require.NoError(t, bucket.FlushAndSwitch())

	stored, err := bucket.Get(idBytes)
	require.NoError(t, err)
	require.NotNil(t, stored)

	decodedDisk, err := storobj.FromBinaryDisk(stored, className)
	require.NoError(t, err)
	assert.Equal(t, "Movies", decodedDisk.Object.Class)
	assert.Equal(t, obj.ID(), decodedDisk.ID())
	assert.Equal(t, obj.DocID, decodedDisk.DocID)
	assert.Equal(t, obj.Vector, decodedDisk.Vector)
	assert.Equal(t, obj.Properties(), decodedDisk.Properties())

	_, err = storobj.FromBinaryNetwork(stored)
	require.Error(t, err, "FromBinaryNetwork must error when on-disk class is empty")
}
