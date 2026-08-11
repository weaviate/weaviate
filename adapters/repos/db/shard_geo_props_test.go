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
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/filters"
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

// The prefill scan reads coordinates straight off the stored bytes while every
// other lookup goes by doc ID. A shard whose two readers disagree would index
// coordinates it never serves.
func TestGeoCoordinateReadersAgree(t *testing.T) {
	ctx := context.Background()
	s := testGeoPropShard(t, ctx)

	tests := []struct {
		name  string
		props map[string]interface{}
		want  *models.GeoCoordinates
	}{
		{
			name:  "geo prop next to other properties",
			props: map[string]interface{}{"name": "munich office", "location": munichCoordinates()},
			want:  munichCoordinates(),
		},
		{
			name:  "two geo props on one object",
			props: map[string]interface{}{"location": munichCoordinates(), "home": stuttgartCoordinates()},
			want:  munichCoordinates(),
		},
		{
			name:  "object without the requested prop",
			props: map[string]interface{}{"name": "no coordinates here"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			docID := putGeoPropObject(t, ctx, s, test.props)

			objectBytes, err := s.store.Bucket(helpers.ObjectsBucketLSM).
				GetBySecondary(ctx, 0, binary.LittleEndian.AppendUint64(nil, docID))
			require.NoError(t, err)

			fromObject, err := s.makeCoordinatesFromObject("location")(objectBytes)
			require.NoError(t, err)
			require.Equal(t, test.want, fromObject)

			forID, err := s.makeCoordinatesForID("location")(ctx, docID)
			if test.want == nil {
				// the by-id reader reports a missing coordinate as a gone doc, the
				// scan just skips the object
				var notFound storobj.ErrNotFound
				require.ErrorAs(t, err, &notFound)
				return
			}
			require.NoError(t, err)
			require.Equal(t, forID, fromObject)
		})
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

func geoProp(name string) *models.Property {
	return &models.Property{Name: name, DataType: []string{string(schema.DataTypeGeoCoordinates)}}
}

// geoIndexAndQueue reads the live index and queue registered for propName.
func geoIndexAndQueue(t *testing.T, s *Shard, propName string) (*geo.Index, *VectorIndexQueue) {
	t.Helper()

	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	idx, ok := s.propertyIndices[propName]
	require.True(t, ok, "no property index for %q", propName)
	return idx.GeoIndex, s.geoQueues[propName]
}

// TestInitGeoProp covers when a second initGeoProp call reuses the registered
// index and when it has to build a new one. Reusing is what keeps a re-init off
// the blocking cache prefill and stops the old index from being orphaned.
func TestInitGeoProp(t *testing.T) {
	tests := []struct {
		name string
		prop string
		// setup runs between capturing the index and calling initGeoProp again.
		setup    func(t *testing.T, ctx context.Context, s *Shard)
		wantSame bool
	}{
		{
			name:     "first geo prop of the class is reused",
			prop:     "location",
			wantSame: true,
		},
		{
			name:     "second geo prop of the class is reused",
			prop:     "home",
			wantSame: true,
		},
		{
			name: "a dropped prop is built from scratch",
			prop: "location",
			setup: func(t *testing.T, ctx context.Context, s *Shard) {
				s.propertyIndicesLock.Lock()
				defer s.propertyIndicesLock.Unlock()
				require.NoError(t, s.propertyIndices.DropAll(ctx, false))
			},
			wantSame: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s := testGeoPropShard(t, ctx)

			before, beforeQueue := geoIndexAndQueue(t, s, test.prop)
			if test.setup != nil {
				test.setup(t, ctx, s)
			}

			require.NoError(t, s.initGeoProp(geoProp(test.prop)))

			after, afterQueue := geoIndexAndQueue(t, s, test.prop)
			if !test.wantSame {
				require.NotSame(t, before, after, "the prop must be inited from scratch")
				return
			}
			require.Same(t, before, after,
				"re-init replaced the live geo index, leaving the old one running")
			require.Same(t, beforeQueue, afterQueue,
				"re-init replaced the live geo queue, leaving the old one registered")
		})
	}
}

// TestInitGeoPropKeysOnPropName pins that the guard keys on the property name
// rather than on the shard having any geo index at all.
func TestInitGeoPropKeysOnPropName(t *testing.T) {
	ctx := context.Background()
	s := testGeoPropShard(t, ctx)

	location, _ := geoIndexAndQueue(t, s, "location")
	home, _ := geoIndexAndQueue(t, s, "home")
	require.NotSame(t, location, home, "each geo prop must get its own index")

	require.NoError(t, s.initGeoProp(geoProp("office")))

	office, _ := geoIndexAndQueue(t, s, "office")
	require.NotNil(t, office, "a prop with no index yet must still be inited")

	stillLocation, _ := geoIndexAndQueue(t, s, "location")
	require.Same(t, location, stillLocation, "initing a new prop disturbed an existing one")
}

// TestInitGeoPropQueueFailureIsRetryable pins that a failed queue build leaves
// no index registered. Keeping it would make the guard skip the retry, so the
// prop would serve reads with an index nothing ever drains into.
func TestInitGeoPropQueueFailureIsRetryable(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{
		Class:      geoPropClass,
		Properties: []*models.Property{{Name: "name", DataType: schema.DataTypeText.PropString()}},
	}
	shardLike, _ := testShardWithSettings(t, ctx, class,
		hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, true)
	s := concreteShard(t, shardLike)

	// a regular file where the queue dir belongs makes the queue's MkdirAll fail
	queueDir := filepath.Join(s.path(), geoPropID("location")+".queue.d")
	require.NoError(t, os.WriteFile(queueDir, []byte("blocked"), 0o644))

	require.Error(t, s.initGeoProp(geoProp("location")))

	s.propertyIndicesLock.RLock()
	_, registered := s.propertyIndices["location"]
	s.propertyIndicesLock.RUnlock()
	require.False(t, registered, "a failed init must not leave the index registered")

	require.NoError(t, os.Remove(queueDir))
	require.NoError(t, s.initGeoProp(geoProp("location")), "the retry must be able to run")

	idx, queue := geoIndexAndQueue(t, s, "location")
	require.NotNil(t, idx)
	require.NotNil(t, queue, "the retry must produce the queue the first attempt failed on")
}

// TestInitGeoPropConcurrent pins that racing callers converge on one usable
// index. It runs under -race; it does not distinguish which of the racing
// indexes survived.
func TestInitGeoPropConcurrent(t *testing.T) {
	ctx := context.Background()
	s := testGeoPropShard(t, ctx)

	const callers = 8
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = s.initGeoProp(geoProp("office"))
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "caller %d", i)
	}

	idx, _ := geoIndexAndQueue(t, s, "office")
	require.NotNil(t, idx)

	require.NoError(t, idx.Add(ctx, 1, munichCoordinates()))
	found, err := idx.WithinRange(ctx, filters.GeoRange{
		GeoCoordinates: munichCoordinates(),
		Distance:       10000,
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, found)
}
