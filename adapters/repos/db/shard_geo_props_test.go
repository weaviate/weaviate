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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hfreshent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
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

// initGeoProp is the only caller holding the collection and shard a geo index
// belongs to, so an index built without them logs its blocking startup work
// anonymously on a node running many shards.
func TestInitGeoPropNamesItsShard(t *testing.T) {
	ctx := context.Background()
	s := testGeoPropShard(t, ctx)

	// hooking the shard's own logger rather than replacing it: the field is read
	// unsynchronized from background goroutines the shard already started
	logger, ok := s.index.logger.(*logrus.Logger)
	require.True(t, ok, "the test shard no longer carries a hookable logger")
	hook := test.NewLocal(logger)
	logger.SetLevel(logrus.DebugLevel)

	props := []string{"office", "depot"}
	for _, prop := range props {
		require.NoError(t, s.initGeoProp(geoProp(prop)))
	}

	namedPerProp := map[string]int{}
	for _, entry := range hook.AllEntries() {
		class, ok := entry.Data["class"]
		if !ok {
			// the commit logger and the vector cache do not carry these
			continue
		}
		// the id also pins the tagging: without it the shard's geo props and its
		// main index all log under the same class and shard
		id, ok := entry.Data["index_id"].(string)
		if !ok {
			continue
		}
		namedPerProp[id]++
		require.Equalf(t, geoPropClass, class, "line %q", entry.Message)
		require.Equalf(t, s.name, entry.Data["shard"], "line %q", entry.Message)
	}

	for _, prop := range props {
		require.NotZerof(t, namedPerProp[geoPropID(prop)],
			"prop %q logged no line naming its class and shard", prop)
	}
}

// testShardWithNamedVector builds a shard whose only vector index is the named
// vector "title" with the given config (async indexing on, as dynamic needs).
func testShardWithNamedVector(t *testing.T, ctx context.Context, className string,
	vic schemaConfig.VectorIndexConfig,
) (ShardLike, *Index) {
	t.Helper()
	return testShardWithSettings(t, ctx, &models.Class{Class: className}, nil, false, true, true,
		func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"title": vic}
		},
	)
}

func removeRootPath(t *testing.T, idx *Index) {
	t.Helper()
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

// TestVectorIndexLoggerCarriesIdentity pins the contract that lets storage-layer
// entities (compressors, commit loggers, queues) log without ever being told
// the logical target-vector name: the shard bakes both identities — the
// logical name for operators and the physical id for storage — into one
// logger, and every implementation, its queue, and everything they construct
// inherit it.
//
// Observed lines: the queue's own lines while it indexes the inserted vectors
// (every index type), and the construction lines of the implementations that
// emit them (hnsw "restored data from disk", hfresh's posting-size line). flat
// and dynamic construct silently on an empty index, so for them the queue lines
// are what proves the inheritance. Every line carrying index_id must also carry
// target_vector, class and shard; queue lines are additionally required to be
// present, so losing the queue's identity fails the test on its own.
func TestVectorIndexLoggerCarriesIdentity(t *testing.T) {
	hnswUC := hnsw.UserConfig{Distance: common.DefaultDistanceMetric}
	hnswUC.SetDefaults()
	// flat (and dynamic below its threshold, which is flat-backed) logs nothing
	// at construction; a cached BQ quantizer makes its startup preload log a
	// line synchronously, so there is something to observe
	flatUC := flatent.UserConfig{Distance: common.DefaultDistanceMetric}
	flatUC.SetDefaults()
	flatUC.BQ.Enabled, flatUC.BQ.Cache = true, true
	dynamicUC := dynamicent.UserConfig{Distance: common.DefaultDistanceMetric}
	dynamicUC.SetDefaults()
	dynamicUC.FlatUC.BQ.Enabled, dynamicUC.FlatUC.BQ.Cache = true, true
	hfreshUC := hfreshent.UserConfig{Distance: common.DefaultDistanceMetric}
	hfreshUC.SetDefaults()

	tests := []struct {
		name string
		vic  schemaConfig.VectorIndexConfig
	}{
		{"hnsw", hnswUC},
		{"flat", flatUC},
		{"dynamic", dynamicUC},
		{"hfresh", hfreshUC},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			className := "LoggerIdentity" + tc.name
			shd, idx := testShardWithNamedVector(t, ctx, className, tc.vic)
			s := shd.(*Shard)
			defer removeRootPath(t, idx)
			defer func() { require.NoError(t, idx.drop()) }()

			// hook the shard's own logger rather than replacing it: the field is
			// read unsynchronized from background goroutines already running
			logger, ok := s.index.logger.(*logrus.Logger)
			require.True(t, ok, "the test shard no longer carries a hookable logger")
			hook := test.NewLocal(logger)
			logger.SetLevel(logrus.DebugLevel)

			// a few vectors, so the queue produces lines under this index while
			// indexing them; wait for it to drain before recreating the index
			var objs []*storobj.Object
			for i := 0; i < 8; i++ {
				objs = append(objs, &storobj.Object{
					MarshallerVersion: 1,
					Object:            models.Object{ID: strfmt.UUID(uuid.NewString()), Class: className},
					Vectors:           map[string][]float32{"title": {float32(i), 1, 0, 1}},
				})
			}
			for _, err := range shd.PutObjectBatch(ctx, objs) {
				require.NoError(t, err)
			}
			q, ok := shd.GetVectorIndexQueue("title")
			require.True(t, ok)
			require.Eventually(t, func() bool { return q.Size() == 0 }, 30*time.Second, 50*time.Millisecond)

			// recreate the named vector's index and queue while the hook is
			// attached, so their construction and preload lines are captured
			require.NoError(t, s.DropVectorIndex(ctx, "title"))
			require.NoError(t, s.initTargetVector(ctx, "title", tc.vic, false))

			var indexLines, queueLines int
			for _, entry := range hook.AllEntries() {
				indexID, ok := entry.Data["index_id"].(string)
				if entry.Data["component"] == "vector_index_queue" {
					require.Truef(t, ok, "queue line %q lost its index_id", entry.Message)
					queueLines++
				}
				if !ok {
					continue // not a line under a vector index
				}
				indexLines++
				// hfresh's centroid graph logs its own id under the parent's name
				require.Truef(t, strings.HasPrefix(indexID, "vectors_title"), "line %q: index_id=%q", entry.Message, indexID)
				require.Equalf(t, "title", entry.Data["target_vector"], "line %q", entry.Message)
				require.Equalf(t, className, entry.Data["class"], "line %q", entry.Message)
				require.Equalf(t, s.name, entry.Data["shard"], "line %q", entry.Message)
			}
			require.NotZero(t, indexLines, "no log line under the recreated index carried index_id")
			require.NotZero(t, queueLines, "the vector index queue logged no identified line")
		})
	}
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
		hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, true, true)
	s := concreteShard(t, shardLike)

	blockGeoQueueDir(t, s, "location")

	require.Error(t, s.initGeoProp(geoProp("location")))

	s.propertyIndicesLock.RLock()
	_, registered := s.propertyIndices["location"]
	s.propertyIndicesLock.RUnlock()
	require.False(t, registered, "a failed init must not leave the index registered")

	require.NoError(t, os.Remove(filepath.Join(s.path(), geoPropID("location")+".queue.d")))
	require.NoError(t, s.initGeoProp(geoProp("location")), "the retry must be able to run")

	idx, queue := geoIndexAndQueue(t, s, "location")
	require.NotNil(t, idx)
	require.NotNil(t, queue, "the retry must produce the queue the first attempt failed on")
}

// TestInitGeoPropConcurrent pins that racing callers build one index between
// them. A second index on the same commit log directory is not just wasted
// work: it prunes the empty raw file the first one holds open, and its startup
// scan can fail on a file another caller pruned mid-scan.
func TestInitGeoPropConcurrent(t *testing.T) {
	ctx := context.Background()
	s := testGeoPropShard(t, ctx)

	// hooking the shard's own logger rather than replacing it: the field is read
	// unsynchronized from background goroutines the shard already started
	logger, ok := s.index.logger.(*logrus.Logger)
	require.True(t, ok, "the test shard no longer carries a hookable logger")
	hook := test.NewLocal(logger)

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

	// each index restores from the prop's commit log directory as it is built,
	// so one restore means the racing callers built one index between them
	var restores int
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == "restore_from_disk" {
			restores++
		}
	}
	require.Equal(t, 1, restores, "only one caller may build an index")

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

// blockGeoQueueDir puts a regular file where propName's queue directory belongs,
// so the queue's MkdirAll fails and initGeoProp errors.
func blockGeoQueueDir(t *testing.T, s *Shard, propName string) {
	t.Helper()

	require.NoError(t, os.WriteFile(
		filepath.Join(s.path(), geoPropID(propName)+".queue.d"), []byte("blocked"), 0o644))
}

// geoInitJobs counts the errgroup jobs initPropertyBuckets submits for props.
// The wrapper reports the count when Wait runs, which is the only place the
// submission shape is observable from outside.
func geoInitJobs(t *testing.T, s *Shard, props ...*models.Property) int {
	t.Helper()

	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	eg := enterrors.NewErrorGroupWrapper(logger)
	s.initPropertyBuckets(context.Background(), eg, false, props...)
	require.NoError(t, eg.Wait())

	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == "error_group_wait_initiated" {
			count, ok := entry.Data["jobs_count"].(int64)
			require.True(t, ok, "jobs_count field missing or not int64")
			return int(count)
		}
	}
	t.Fatal("error group never logged its job count")
	return 0
}

// TestInitPropertyBucketsBatchesGeoProps pins that geo props share one errgroup
// job. Each of them prefills its cache by scanning the whole objects bucket, so
// a job apiece is a full scan apiece running at once.
func TestInitPropertyBucketsBatchesGeoProps(t *testing.T) {
	textProp := &models.Property{
		Name:         "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	}

	tests := []struct {
		name     string
		props    []*models.Property
		wantJobs int
	}{
		{
			name:     "no props",
			wantJobs: 0,
		},
		{
			name:     "one text prop",
			props:    []*models.Property{textProp},
			wantJobs: 1,
		},
		{
			name:     "one geo prop",
			props:    []*models.Property{geoProp("location")},
			wantJobs: 1,
		},
		{
			name:     "three geo props share one job",
			props:    []*models.Property{geoProp("location"), geoProp("home"), geoProp("office")},
			wantJobs: 1,
		},
		{
			name:     "geo props batch while other props still fan out",
			props:    []*models.Property{textProp, geoProp("location"), geoProp("home")},
			wantJobs: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			shardLike, _ := testShardWithSettings(t, ctx, &models.Class{Class: geoPropClass},
				hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, false)

			require.Equal(t, test.wantJobs,
				geoInitJobs(t, concreteShard(t, shardLike), test.props...))
		})
	}
}

// TestInitPropertyBucketsGeoBatchContinuesAfterError pins that one geo prop
// failing still leaves the others initialized. Skipping them would bring the
// shard up accepting writes it never geo-indexes.
func TestInitPropertyBucketsGeoBatchContinuesAfterError(t *testing.T) {
	ctx := context.Background()
	shardLike, _ := testShardWithSettings(t, ctx, &models.Class{Class: geoPropClass},
		hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, true, true)
	s := concreteShard(t, shardLike)

	blockGeoQueueDir(t, s, "alpha")

	eg := enterrors.NewErrorGroupWrapper(s.index.logger)
	s.initPropertyBuckets(ctx, eg, false, geoProp("alpha"), geoProp("beta"))

	err := eg.Wait()
	require.ErrorContains(t, err, `init prop "alpha": value index:`)

	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	require.NotContains(t, s.propertyIndices, "alpha", "the failed prop must not stay registered")
	require.Contains(t, s.propertyIndices, "beta", "a later prop must still be initialized")
}
