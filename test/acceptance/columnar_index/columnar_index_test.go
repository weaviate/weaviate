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

// Package columnar_index_test verifies the user-visible behavior of the
// `indexColumnar` schema flag end to end:
//
//   - schema validation (flag only valid for int/number/date),
//   - filtered GraphQL aggregations served from columnar buckets, on both
//     the point-lookup path (< 2048 matched docIDs) and the scan path
//     (>= 2048 matched docIDs), compared against an identical twin class
//     without the flag AND against expected values tracked in the test,
//   - durability across graceful restart (flush) and crash (WAL replay).
//
// NOTE: unfiltered aggregates do NOT touch the columnar buckets, so every
// aggregate assertion in this package carries a `where` filter.
//
// CI wiring: this package needs no explicit registration. test/run.sh's
// get_fast_acceptance_packages globs `go list ./... | grep test/acceptance`
// minus an explicit exclusion list (replication, graphql_resolvers, authz,
// compaction, ...); columnar_index is neither excluded nor assigned to fast
// groups 1-4 (get_aof_group), so it lands automatically in the catch-all
// fast group 5 (get_other_packages), which runs as the "fast-group-5" job
// in .github/workflows/pull_requests.yaml.
package columnar_index_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const (
	columnarClassName = "ColumnarSongs"
	plainClassName    = "PlainSongs"
	numObjects        = 5000
	batchChunkSize    = 500
)

func boolPtr(b bool) *bool { return &b }

// songModel is the in-test source of truth for every imported object. All
// expected aggregate values are computed from this model so the test never
// trusts one server path to validate another.
type songModel struct {
	idx       int
	category  string
	hasProps  bool // ~5% of objects omit the three columnar props entirely
	likes     int64
	rating    float64
	published time.Time
	deleted   bool
}

func uuidFor(i int) strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i))
}

// buildModel creates the deterministic data set, identical for both classes.
//
// Design constraints (deviations from naive choices are deliberate):
//   - likes = (i*7) % 5000: gcd(7,5000)=1, so likes values are UNIQUE per
//     object, which keeps min/max/sum assertions free of accidental ties.
//   - rating = 0.25*i - 100: unique, and every value is a multiple of 0.25
//     with bounded magnitude, so every partial float64 sum is exact and the
//     aggregate sum is independent of visit order (the columnar scan path
//     visits docIDs in a different order than the object-scan path).
//   - one object carries rating = 100 + 2^-24, which collapses to 100.0 in
//     float32: it proves the columnar store holds full float64 precision.
//     It is still a multiple of 2^-24, so sums stay exact.
//   - published = base + i*210min: unique dates spread over ~2 years.
func buildModel() []*songModel {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	model := make([]*songModel, numObjects)
	for i := 0; i < numObjects; i++ {
		category := "common"
		if i%50 == 0 {
			category = "rare" // 100 objects -> point-lookup aggregate path
		}
		model[i] = &songModel{
			idx:      i,
			category: category,
			// ~5.3% of objects omit the columnar props (null handling).
			// 19 and 50 overlap (i ≡ 350 mod 950), so the rare/point-lookup
			// path sees nulls too.
			hasProps:  i%19 != 7,
			likes:     int64((i * 7) % numObjects),
			rating:    0.25*float64(i) - 100,
			published: base.Add(time.Duration(i) * 210 * time.Minute),
		}
	}
	model[4200].rating = 100 + math.Pow(2, -24) // float64-precision sentinel (rare category)
	return model
}

func propsFor(o *songModel) map[string]interface{} {
	props := map[string]interface{}{
		"name":     fmt.Sprintf("song %s %05d", o.category, o.idx),
		"category": o.category,
	}
	if o.hasProps {
		props["likes"] = o.likes
		props["rating"] = o.rating
		props["published"] = o.published.Format(time.RFC3339Nano)
	}
	return props
}

func makeClass(name string, columnar bool) *models.Class {
	return &models.Class{
		Class:      name,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord, IndexSearchable: boolPtr(true)},
			{Name: "category", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationField, IndexFilterable: boolPtr(true)},
			{Name: "likes", DataType: schema.DataTypeInt.PropString(), IndexColumnar: boolPtr(columnar)},
			{Name: "rating", DataType: schema.DataTypeNumber.PropString(), IndexColumnar: boolPtr(columnar)},
			{Name: "published", DataType: schema.DataTypeDate.PropString(), IndexColumnar: boolPtr(columnar)},
		},
	}
}

// catAgg holds every aggregate value the test asserts on for one
// (class, category) pair. Plain float64/time.Time fields so twin results
// and expected values can be compared with require.Equal.
type catAgg struct {
	metaCount float64

	likesCount, likesMean, likesSum, likesMin, likesMax      float64
	ratingCount, ratingMean, ratingSum, ratingMin, ratingMax float64

	publishedCount             float64
	publishedMin, publishedMax time.Time
}

// expectedFor computes the expected aggregates for a category from the model.
func expectedFor(model []*songModel, category string) catAgg {
	e := catAgg{
		likesMin: math.MaxFloat64, likesMax: -math.MaxFloat64,
		ratingMin: math.MaxFloat64, ratingMax: -math.MaxFloat64,
	}
	for _, o := range model {
		if o.deleted || o.category != category {
			continue
		}
		e.metaCount++
		if !o.hasProps {
			continue
		}
		e.likesCount++
		e.likesSum += float64(o.likes)
		e.likesMin = math.Min(e.likesMin, float64(o.likes))
		e.likesMax = math.Max(e.likesMax, float64(o.likes))

		e.ratingCount++
		e.ratingSum += o.rating
		e.ratingMin = math.Min(e.ratingMin, o.rating)
		e.ratingMax = math.Max(e.ratingMax, o.rating)

		e.publishedCount++
		if e.publishedMin.IsZero() || o.published.Before(e.publishedMin) {
			e.publishedMin = o.published
		}
		if o.published.After(e.publishedMax) {
			e.publishedMax = o.published
		}
	}
	// mean exactly as the server computes it: sum/count (numerical.go Mean())
	e.likesMean = e.likesSum / e.likesCount
	e.ratingMean = e.ratingSum / e.ratingCount
	return e
}

func numField(t *testing.T, row map[string]interface{}, prop, field string) float64 {
	t.Helper()
	pm, ok := row[prop].(map[string]interface{})
	require.True(t, ok, "aggregate response misses property %q: %#v", prop, row)
	n, ok := pm[field].(json.Number)
	require.True(t, ok, "aggregate field %s.%s is not a number: %T %v", prop, field, pm[field], pm[field])
	v, err := n.Float64()
	require.NoError(t, err)
	return v
}

func dateField(t *testing.T, row map[string]interface{}, prop, field string) time.Time {
	t.Helper()
	pm, ok := row[prop].(map[string]interface{})
	require.True(t, ok, "aggregate response misses property %q: %#v", prop, row)
	s, ok := pm[field].(string)
	require.True(t, ok, "aggregate field %s.%s is not a string: %T %v", prop, field, pm[field], pm[field])
	ts, err := time.Parse(time.RFC3339Nano, s)
	require.NoError(t, err, "aggregate field %s.%s: cannot parse %q", prop, field, s)
	return ts.UTC()
}

func TestColumnarIndex(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	model := buildModel()

	// ── shared helpers ──────────────────────────────────────────

	importObjects := func(t *testing.T, className string, objs []*songModel) {
		t.Helper()
		for start := 0; start < len(objs); start += batchChunkSize {
			end := start + batchChunkSize
			if end > len(objs) {
				end = len(objs)
			}
			batch := make([]*models.Object, 0, end-start)
			for _, o := range objs[start:end] {
				batch = append(batch, &models.Object{
					Class:      className,
					ID:         uuidFor(o.idx),
					Properties: propsFor(o),
				})
			}
			helper.CreateObjectsBatch(t, batch)
		}
	}

	// fetchCategoryAgg runs a FILTERED GraphQL aggregate (the columnar fast
	// path only serves filtered aggregations) and parses the response.
	fetchCategoryAgg := func(t *testing.T, className, category string) catAgg {
		t.Helper()
		query := fmt.Sprintf(`{
			Aggregate {
				%s(where: {operator: Equal, path: ["category"], valueText: %q}) {
					meta { count }
					likes { count mean sum minimum maximum }
					rating { count mean sum minimum maximum }
					published { count minimum maximum }
				}
			}
		}`, className, category)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		rows := result.Get("Aggregate", className).AsSlice()
		require.Len(t, rows, 1)
		row, ok := rows[0].(map[string]interface{})
		require.True(t, ok)

		meta, ok := row["meta"].(map[string]interface{})
		require.True(t, ok)
		metaCount, err := meta["count"].(json.Number).Float64()
		require.NoError(t, err)

		return catAgg{
			metaCount: metaCount,

			likesCount: numField(t, row, "likes", "count"),
			likesMean:  numField(t, row, "likes", "mean"),
			likesSum:   numField(t, row, "likes", "sum"),
			likesMin:   numField(t, row, "likes", "minimum"),
			likesMax:   numField(t, row, "likes", "maximum"),

			ratingCount: numField(t, row, "rating", "count"),
			ratingMean:  numField(t, row, "rating", "mean"),
			ratingSum:   numField(t, row, "rating", "sum"),
			ratingMin:   numField(t, row, "rating", "minimum"),
			ratingMax:   numField(t, row, "rating", "maximum"),

			publishedCount: numField(t, row, "published", "count"),
			publishedMin:   dateField(t, row, "published", "minimum"),
			publishedMax:   dateField(t, row, "published", "maximum"),
		}
	}

	patchLikes := func(t *testing.T, className string, id strfmt.UUID, likes int64) {
		t.Helper()
		obj := &models.Object{
			Class:      className,
			ID:         id,
			Properties: map[string]interface{}{"likes": likes},
		}
		params := clobjects.NewObjectsClassPatchParams().
			WithClassName(className).WithID(id).WithBody(obj)
		resp, err := helper.Client(t).Objects.ObjectsClassPatch(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)
	}

	assertAggregatesMatch := func(t *testing.T, categories ...string) {
		for _, category := range categories {
			t.Run("category "+category, func(t *testing.T) {
				expected := expectedFor(model, category)
				colAgg := fetchCategoryAgg(t, columnarClassName, category)
				plainAgg := fetchCategoryAgg(t, plainClassName, category)
				require.Equal(t, expected, plainAgg, "plain twin must match the tracked expected values")
				require.Equal(t, expected, colAgg, "columnar class must match the tracked expected values")
			})
		}
	}

	// ── 1. schema flag validation ───────────────────────────────

	ok := t.Run("schema flag validation", func(t *testing.T) {
		t.Run("text prop with indexColumnar true is rejected", func(t *testing.T) {
			bad := &models.Class{
				Class:      "ColumnarBadTextClass",
				Vectorizer: "none",
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString(), IndexColumnar: boolPtr(true)},
				},
			}
			params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(bad)
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			require.Error(t, err, "indexColumnar on a text prop must be rejected")
			var createErr *clschema.SchemaObjectsCreateUnprocessableEntity
			require.ErrorAs(t, err, &createErr)
			require.NotEmpty(t, createErr.Payload.Error)
			assert.Contains(t, createErr.Payload.Error[0].Message, "number/int/date")
		})

		t.Run("create twin classes", func(t *testing.T) {
			helper.CreateClass(t, makeClass(columnarClassName, true))
			helper.CreateClass(t, makeClass(plainClassName, false))
		})

		t.Run("GET schema echoes flags, unflagged props default false", func(t *testing.T) {
			for _, tc := range []struct {
				class    string
				columnar bool
			}{
				{columnarClassName, true},
				{plainClassName, false},
			} {
				cls := helper.GetClass(t, tc.class)
				require.NotNil(t, cls)
				flags := map[string]bool{}
				for _, p := range cls.Properties {
					require.NotNil(t, p.IndexColumnar,
						"class %s prop %s: indexColumnar must be defaulted to non-nil", tc.class, p.Name)
					flags[p.Name] = *p.IndexColumnar
				}
				assert.Equal(t, map[string]bool{
					"name":      false,
					"category":  false,
					"likes":     tc.columnar,
					"rating":    tc.columnar,
					"published": tc.columnar,
				}, flags, "class %s", tc.class)
			}
		})
	})
	require.True(t, ok, "schema setup failed, aborting")

	// ── 2. import deterministic data ────────────────────────────

	ok = t.Run("import deterministic data", func(t *testing.T) {
		importObjects(t, columnarClassName, model)
		importObjects(t, plainClassName, model)
	})
	require.True(t, ok, "import failed, aborting")

	// ── 3. filtered aggregate equals plain twin and expected ────

	t.Run("filtered aggregate equals plain twin and expected values", func(t *testing.T) {
		// "rare" matches 100 docIDs -> point-lookup path (< 2048);
		// "common" matches 4900 docIDs -> scan path (>= 2048).
		assertAggregatesMatch(t, "rare", "common")
	})

	// ── 4. update and delete reflected ──────────────────────────

	t.Run("update and delete reflected", func(t *testing.T) {
		const (
			patchIdx     = 1000 // rare, has props, likes=(1000*7)%5000=2000
			deleteIdx    = 1500 // rare, has props
			patchedLikes = 999999
		)
		require.Equal(t, "rare", model[patchIdx].category)
		require.True(t, model[patchIdx].hasProps)
		require.Equal(t, "rare", model[deleteIdx].category)
		require.True(t, model[deleteIdx].hasProps)
		originalLikes := model[patchIdx].likes

		for _, className := range []string{columnarClassName, plainClassName} {
			patchLikes(t, className, uuidFor(patchIdx), patchedLikes)
			helper.DeleteObject(t, &models.Object{Class: className, ID: uuidFor(deleteIdx)})
		}
		model[patchIdx].likes = patchedLikes
		model[deleteIdx].deleted = true

		expected := expectedFor(model, "rare")

		plainAgg := fetchCategoryAgg(t, plainClassName, "rare")
		require.Equal(t, expected, plainAgg, "plain twin must reflect patch + delete")

		colAgg := fetchCategoryAgg(t, columnarClassName, "rare")
		// Everything except the patched likes value must be reflected:
		// the delete is removed from the columns, and rating/published were
		// not touched by the patch.
		require.Equal(t, expected.metaCount, colAgg.metaCount)
		require.Equal(t, expected.likesCount, colAgg.likesCount)
		require.Equal(t, expected.likesMin, colAgg.likesMin)
		require.Equal(t, expected.ratingCount, colAgg.ratingCount)
		require.Equal(t, expected.ratingMean, colAgg.ratingMean)
		require.Equal(t, expected.ratingSum, colAgg.ratingSum)
		require.Equal(t, expected.ratingMin, colAgg.ratingMin)
		require.Equal(t, expected.ratingMax, colAgg.ratingMax)
		require.Equal(t, expected.publishedCount, colAgg.publishedCount)
		require.Equal(t, expected.publishedMin, colAgg.publishedMin)
		require.Equal(t, expected.publishedMax, colAgg.publishedMax)

		// REGRESSION GUARD: a PATCH that changes only scalar props preserves
		// the docID (compareObjsForInsertStatus in
		// adapters/repos/db/shard_write_put.go), which routes the
		// inverted-index update through inverted.DeltaSkipSearchable
		// (adapters/repos/db/inverted/delta_analyzer.go). An earlier version
		// of that function rebuilt the ToAdd/ToDelete Property structs
		// without copying HasColumnarIndex, so the columnar bucket kept
		// serving the PRE-patch value (see the "value updates with preserved
		// docIDs" phase in
		// adapters/repos/db/aggregations_columnar_integration_test.go).
		// These three assertions fail loudly if that ever regresses.
		require.Equal(t, expected.likesSum, colAgg.likesSum,
			"columnar bucket must serve the post-PATCH likes value (docID-preserving update, DeltaSkipSearchable must carry HasColumnarIndex)")
		require.Equal(t, expected.likesMean, colAgg.likesMean,
			"columnar bucket must serve the post-PATCH likes value (docID-preserving update, DeltaSkipSearchable must carry HasColumnarIndex)")
		require.Equal(t, expected.likesMax, colAgg.likesMax,
			"columnar bucket must serve the post-PATCH likes value (docID-preserving update, DeltaSkipSearchable must carry HasColumnarIndex)")

		// "common" was not touched by the mutations at all.
		assertAggregatesMatch(t, "common")

		// Restore the patched value (a second docID-preserving PATCH, which
		// exercises the same delta path once more); the delete stays and is
		// tracked in the model. Later journeys re-assert on this state.
		for _, className := range []string{columnarClassName, plainClassName} {
			patchLikes(t, className, uuidFor(patchIdx), originalLikes)
		}
		model[patchIdx].likes = originalLikes
	})

	// ── 5. graceful restart preserves columnar data ─────────────

	t.Run("graceful restart preserves columnar data", func(t *testing.T) {
		require.NoError(t, compose.StopAt(ctx, 0, nil)) // SIGTERM, graceful flush
		require.NoError(t, compose.StartAt(ctx, 0))
		// ports remap on restart: re-setup REST client
		helper.SetupClient(compose.GetWeaviate().URI())

		assertAggregatesMatch(t, "rare", "common")
	})

	// ── 6. crash recovery replays columnar WAL ──────────────────

	t.Run("crash recovery replays columnar WAL", func(t *testing.T) {
		const numPostRestart = 50
		postModel := make([]*songModel, numPostRestart)
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		for j := 0; j < numPostRestart; j++ {
			idx := numObjects + j
			postModel[j] = &songModel{
				idx:       idx,
				category:  "postrestart",
				hasProps:  true,
				likes:     int64(idx),
				rating:    0.25*float64(idx) - 100,
				published: base.Add(time.Duration(j) * time.Hour),
			}
		}
		importObjects(t, columnarClassName, postModel)
		model = append(model, postModel...)
		expected := expectedFor(model, "postrestart")

		// sanity before the crash, so a failure below is attributable to
		// recovery rather than to the import
		preCrash := fetchCategoryAgg(t, columnarClassName, "postrestart")
		require.Equal(t, expected, preCrash)
		require.Equal(t, float64(numPostRestart), preCrash.likesCount)

		// zero timeout forces SIGKILL: nothing gets flushed, recovery must
		// come from the columnar WAL
		killTimeout := time.Duration(0)
		require.NoError(t, compose.StopAt(ctx, 0, &killTimeout))
		require.NoError(t, compose.StartAt(ctx, 0))
		helper.SetupClient(compose.GetWeaviate().URI())

		postCrash := fetchCategoryAgg(t, columnarClassName, "postrestart")
		require.Equal(t, float64(numPostRestart), postCrash.likesCount,
			"all 50 post-restart objects must survive the crash")
		require.Equal(t, expected, postCrash,
			"columnar WAL replay must restore exact values, not just counts")
	})
}
