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

//go:build integrationTest

package db

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const geoPropClass = "GeoPropRead"

func munichCoordinates() *models.GeoCoordinates {
	return &models.GeoCoordinates{Latitude: ptFloat32(48.13743), Longitude: ptFloat32(11.57549)}
}

func stuttgartCoordinates() *models.GeoCoordinates {
	return &models.GeoCoordinates{Latitude: ptFloat32(48.78232), Longitude: ptFloat32(9.17702)}
}

func testGeoPropShard(t *testing.T, ctx context.Context) *Shard {
	t.Helper()

	class := &models.Class{
		Class: geoPropClass,
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{Name: "sizes", DataType: schema.DataTypeNumberArray.PropString()},
			{Name: "location", DataType: []string{string(schema.DataTypeGeoCoordinates)}},
			{Name: "home", DataType: []string{string(schema.DataTypeGeoCoordinates)}},
		},
	}

	shard, _ := testShardWithSettings(t, ctx, class,
		hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, false)
	return concreteShard(t, shard)
}

func putGeoPropObject(t *testing.T, ctx context.Context, s *Shard, props map[string]interface{}) uint64 {
	t.Helper()

	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      geoPropClass,
			Properties: props,
		},
		Vector: []float32{1, 2, 3},
	}
	require.NoError(t, s.PutObject(ctx, obj))
	return obj.DocID
}

// putRawObjectPayload stores payload under a fresh uuid with docID as its
// secondary key, bypassing the write path's marshalling.
func putRawObjectPayload(t *testing.T, s *Shard, docID uint64, payload []byte) uint64 {
	t.Helper()

	idBytes, err := uuid.New().MarshalBinary()
	require.NoError(t, err)

	docIDBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBuf, docID)

	require.NoError(t, s.store.Bucket(helpers.ObjectsBucketLSM).Put(idBytes, payload,
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBuf)))
	return docID
}

// payloadWithProps builds an object payload whose property blob is exactly
// props, including blobs no marshaller would produce. It splices rather than
// marshals because json.Marshal rejects invalid json.
func payloadWithProps(t *testing.T, props []byte) []byte {
	t.Helper()

	payload, err := (&storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      geoPropClass,
			Properties: map[string]interface{}{},
		},
	}).MarshalBinaryDisk(true)
	require.NoError(t, err)

	// version, docID, kind, uuid, both timestamps, then the vector and
	// classname lengths, whose bodies are empty for the object above
	const propLengthPos = 1 + 8 + 1 + 16 + 8 + 8 + 2 + 2
	propLength := int(binary.LittleEndian.Uint32(payload[propLengthPos:]))

	out := append([]byte{}, payload[:propLengthPos]...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(props)))
	out = append(out, props...)
	return append(out, payload[propLengthPos+4+propLength:]...)
}

func TestMakeCoordinatesForID(t *testing.T) {
	ctx := context.Background()
	s := testGeoPropShard(t, ctx)

	fullProps := map[string]interface{}{
		"name":     "munich office",
		"sizes":    []float64{1.5, 2.5},
		"location": munichCoordinates(),
		"home":     stuttgartCoordinates(),
	}

	tests := []struct {
		name     string
		propName string
		docID    func(t *testing.T) uint64
		want     *models.GeoCoordinates
		// a doc that is gone, or present without a value for propName: the geo
		// index tombstones the doc ID for both
		wantNotFound bool
		// any other failure, which must not read as a missing doc
		wantErrContains string
	}{
		{
			name:     "geo prop next to other properties",
			propName: "location",
			docID:    func(t *testing.T) uint64 { return putGeoPropObject(t, ctx, s, fullProps) },
			want:     munichCoordinates(),
		},
		{
			name:     "second geo prop of the same object",
			propName: "home",
			docID:    func(t *testing.T) uint64 { return putGeoPropObject(t, ctx, s, fullProps) },
			want:     stuttgartCoordinates(),
		},
		{
			name:     "object without the requested prop",
			propName: "location",
			docID: func(t *testing.T) uint64 {
				return putGeoPropObject(t, ctx, s, map[string]interface{}{"name": "no coordinates here"})
			},
			wantNotFound: true,
		},
		{
			name:         "object with an empty property map",
			propName:     "location",
			docID:        func(t *testing.T) uint64 { return putGeoPropObject(t, ctx, s, map[string]interface{}{}) },
			wantNotFound: true,
		},
		{
			name:         "object without any properties",
			propName:     "location",
			docID:        func(t *testing.T) uint64 { return putGeoPropObject(t, ctx, s, nil) },
			wantNotFound: true,
		},
		{
			name:         "doc id that was never written",
			propName:     "location",
			docID:        func(t *testing.T) uint64 { return 1_000_000 },
			wantNotFound: true,
		},
		{
			name:     "payload of an unsupported marshaller version",
			propName: "location",
			docID: func(t *testing.T) uint64 {
				return putRawObjectPayload(t, s, 2_000_000, []byte{2, 0, 0, 0})
			},
			wantErrContains: "unsupported binary marshaller version",
		},
		{
			name:     "property blob without its opening brace",
			propName: "location",
			docID: func(t *testing.T) uint64 {
				return putRawObjectPayload(t, s, 2_000_001,
					payloadWithProps(t, []byte(`"location":"48.1"}`)))
			},
			wantErrContains: "malformed property json",
		},
		{
			name:     "prop that does not hold coordinates",
			propName: "location",
			docID: func(t *testing.T) uint64 {
				return putRawObjectPayload(t, s, 2_000_002,
					payloadWithProps(t, []byte(`{"location":"48.1"}`)))
			},
			wantErrContains: "expected property to be of type",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinates, err := s.makeCoordinatesForID(test.propName)(ctx, test.docID(t))

			if test.want != nil {
				require.NoError(t, err)
				require.Equal(t, test.want, coordinates)
				return
			}

			require.Error(t, err)
			var notFound storobj.ErrNotFound
			if test.wantNotFound {
				require.ErrorAs(t, err, &notFound)
				return
			}

			require.NotEmpty(t, test.wantErrContains, "table case declares no expectation")
			require.ErrorContains(t, err, test.wantErrContains)
			require.NotErrorAs(t, err, &notFound,
				"a read that failed for another reason must not look like a deleted doc")
		})
	}
}

// All lookups share one *storobj.PropertyExtraction.
func TestMakeCoordinatesForIDConcurrentLookups(t *testing.T) {
	ctx := context.Background()
	s := testGeoPropShard(t, ctx)

	docID := putGeoPropObject(t, ctx, s, map[string]interface{}{"location": munichCoordinates()})
	coordinatesForID := s.makeCoordinatesForID("location")

	const lookups = 8
	coordinates := make([]*models.GeoCoordinates, lookups)
	errs := make([]error, lookups)

	var wg sync.WaitGroup
	for i := range lookups {
		wg.Go(func() {
			coordinates[i], errs[i] = coordinatesForID(ctx, docID)
		})
	}
	wg.Wait()

	for i := range lookups {
		require.NoError(t, errs[i])
		require.Equal(t, munichCoordinates(), coordinates[i])
	}
}

func TestObjectByIndexIDWithPropsDecodesOnlyRequestedProps(t *testing.T) {
	ctx := context.Background()
	s := testGeoPropShard(t, ctx)

	docID := putGeoPropObject(t, ctx, s, map[string]interface{}{
		"name":     "munich office",
		"sizes":    []float64{1.5, 2.5},
		"location": munichCoordinates(),
	})

	obj, err := s.objectByIndexIDWithProps(ctx, docID, storobj.NewPropExtraction().Add("location"))
	require.NoError(t, err)

	require.Empty(t, obj.Vector, "the vector must stay undecoded")
	props, ok := obj.Properties().(map[string]interface{})
	require.True(t, ok)
	require.Len(t, props, 1, "only the requested property must be decoded")
	require.Equal(t, munichCoordinates(), props["location"])
}
