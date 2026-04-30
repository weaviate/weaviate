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

package inverted

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
)

// TestRecExecutorFilterExamples mirrors the eight F-scenarios from
// TestCorrelatedAndFilterExamplesIndexed but exercises the new recursive plan
// builder and executor in isolation. Each sub-test constructs the meta bucket
// _idx entries, the per-leaf raw position bitmaps, builds the recursive plan
// from the same propValuePair tree, and verifies the result equals docMatch.
//
// The value bucket and the production resolver path are not involved — this
// keeps the tests focused on the planner+executor pair.
func TestRecExecutorFilterExamples(t *testing.T) {
	class := filterExamplesClass()
	props := rootNestedProps(t, class, "countries")

	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }

	// runRec resolves the correlated pvp through the recursive plan + executor
	// and asserts the docID set equals want. rawsByCond maps each leaf
	// propValuePair to its raw position bitmap.
	runRec := func(t *testing.T, pv *propValuePair, mb *lsmkv.Bucket, rawsByCond map[*propValuePair]*sroar.Bitmap, want []uint64) {
		t.Helper()
		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build(pv.children)
		exec := newRecExecutor(rawsByCond, mb, ops, concurrency.SROAR_MERGE)
		result, release, err := exec.execute(context.Background(), plan)
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, want, result.ToArray())
	}

	// -----------------------------------------------------------------------
	// F1: garages[0].city AND garages[1].postcode AND garages[1].cars.{make,model}
	//
	// docMatch: city in g[0]; postcode in g[1]; make+model both in g[1].cars[0].
	// docNoMatch: same but make in g[1].cars[0] and model in g[1].cars[1].
	// -----------------------------------------------------------------------
	t.Run("F1_garages[0].city_AND_garages[1].postcode_AND_garages[1].cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Direct = uint16(1)
			leafG1Direct = uint16(2)
			leafG1Cars0  = uint16(3)
			leafG1Cars1  = uint16(4)
		)
		mb := newIdxBucket(t)

		writeIdx(t, mb, "garages", 0,
			[]uint64{enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)})
		writeIdx(t, mb, "garages", 1, []uint64{
			enc(1, leafG1Direct, docMatch), enc(1, leafG1Cars0, docMatch),
			enc(1, leafG1Direct, docNoMatch), enc(1, leafG1Cars0, docNoMatch), enc(1, leafG1Cars1, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 0, []uint64{enc(1, leafG1Cars0, docMatch), enc(1, leafG1Cars0, docNoMatch)})
		writeIdx(t, mb, "garages.cars", 1, []uint64{enc(1, leafG1Cars1, docNoMatch)})

		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0g)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1g)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1g)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx1g)

		raws := map[*propValuePair]*sroar.Bitmap{
			city:      roaringset.NewBitmap(enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)),
			postcode:  roaringset.NewBitmap(enc(1, leafG1Direct, docMatch), enc(1, leafG1Direct, docNoMatch)),
			carsMake:  roaringset.NewBitmap(enc(1, leafG1Cars0, docMatch), enc(1, leafG1Cars0, docNoMatch)),
			carsModel: roaringset.NewBitmap(enc(1, leafG1Cars0, docMatch), enc(1, leafG1Cars1, docNoMatch)),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F2: garages[0].city AND garages[1].postcode AND garages[2].cars.{make,model}
	// -----------------------------------------------------------------------
	t.Run("F2_garages[0].city_AND_garages[1].postcode_AND_garages[2].cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Direct = uint16(1)
			leafG1Direct = uint16(2)
			leafG2Cars0  = uint16(3)
			leafG2Cars1  = uint16(4)
		)
		mb := newIdxBucket(t)

		writeIdx(t, mb, "garages", 0, []uint64{enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)})
		writeIdx(t, mb, "garages", 1, []uint64{enc(1, leafG1Direct, docMatch), enc(1, leafG1Direct, docNoMatch)})
		writeIdx(t, mb, "garages", 2, []uint64{
			enc(1, leafG2Cars0, docMatch),
			enc(1, leafG2Cars0, docNoMatch), enc(1, leafG2Cars1, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 0, []uint64{enc(1, leafG2Cars0, docMatch), enc(1, leafG2Cars0, docNoMatch)})
		writeIdx(t, mb, "garages.cars", 1, []uint64{enc(1, leafG2Cars1, docNoMatch)})

		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		idx2g := filnested.ArrayIndex{RelPath: "garages", Index: 2}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0g)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1g)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx2g)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx2g)

		raws := map[*propValuePair]*sroar.Bitmap{
			city:      roaringset.NewBitmap(enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)),
			postcode:  roaringset.NewBitmap(enc(1, leafG1Direct, docMatch), enc(1, leafG1Direct, docNoMatch)),
			carsMake:  roaringset.NewBitmap(enc(1, leafG2Cars0, docMatch), enc(1, leafG2Cars0, docNoMatch)),
			carsModel: roaringset.NewBitmap(enc(1, leafG2Cars0, docMatch), enc(1, leafG2Cars1, docNoMatch)),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F3: countries[0].garages.city AND countries[1].garages.postcode AND
	//     countries[1].garages.cars.{make,model}
	// -----------------------------------------------------------------------
	t.Run("F3_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[1].garages.cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafC0G0Direct = uint16(1)
			leafC1G0Direct = uint16(1)
			leafC1G0Cars0  = uint16(2)
			leafC1G0Cars1  = uint16(3)
		)
		mb := newIdxBucket(t)

		writeIdx(t, mb, "", 0, []uint64{enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch)})
		writeIdx(t, mb, "", 1, []uint64{
			enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Cars0, docMatch),
			enc(2, leafC1G0Direct, docNoMatch), enc(2, leafC1G0Cars0, docNoMatch), enc(2, leafC1G0Cars1, docNoMatch),
		})
		writeIdx(t, mb, "garages", 0, []uint64{
			enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch),
			enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Cars0, docMatch),
			enc(2, leafC1G0Direct, docNoMatch), enc(2, leafC1G0Cars0, docNoMatch), enc(2, leafC1G0Cars1, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 0, []uint64{enc(2, leafC1G0Cars0, docMatch), enc(2, leafC1G0Cars0, docNoMatch)})
		writeIdx(t, mb, "garages.cars", 1, []uint64{enc(2, leafC1G0Cars1, docNoMatch)})

		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1c)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx1c)

		raws := map[*propValuePair]*sroar.Bitmap{
			city:      roaringset.NewBitmap(enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch)),
			postcode:  roaringset.NewBitmap(enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch)),
			carsMake:  roaringset.NewBitmap(enc(2, leafC1G0Cars0, docMatch), enc(2, leafC1G0Cars0, docNoMatch)),
			carsModel: roaringset.NewBitmap(enc(2, leafC1G0Cars0, docMatch), enc(2, leafC1G0Cars1, docNoMatch)),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F4: countries[0].garages.city AND countries[1].garages.postcode AND
	//     countries[2].garages.cars.{make,model}
	// -----------------------------------------------------------------------
	t.Run("F4_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[2].garages.cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafC0G0Direct = uint16(1)
			leafC1G0Direct = uint16(1)
			leafC2G0Cars0  = uint16(1)
			leafC2G0Cars1  = uint16(2)
		)
		mb := newIdxBucket(t)

		writeIdx(t, mb, "", 0, []uint64{enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch)})
		writeIdx(t, mb, "", 1, []uint64{enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch)})
		writeIdx(t, mb, "", 2, []uint64{
			enc(3, leafC2G0Cars0, docMatch),
			enc(3, leafC2G0Cars0, docNoMatch), enc(3, leafC2G0Cars1, docNoMatch),
		})
		writeIdx(t, mb, "garages", 0, []uint64{
			enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch),
			enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch),
			enc(3, leafC2G0Cars0, docMatch),
			enc(3, leafC2G0Cars0, docNoMatch), enc(3, leafC2G0Cars1, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 0, []uint64{enc(3, leafC2G0Cars0, docMatch), enc(3, leafC2G0Cars0, docNoMatch)})
		writeIdx(t, mb, "garages.cars", 1, []uint64{enc(3, leafC2G0Cars1, docNoMatch)})

		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}
		idx2c := filnested.ArrayIndex{RelPath: "", Index: 2}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx2c)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx2c)

		raws := map[*propValuePair]*sroar.Bitmap{
			city:      roaringset.NewBitmap(enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch)),
			postcode:  roaringset.NewBitmap(enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch)),
			carsMake:  roaringset.NewBitmap(enc(3, leafC2G0Cars0, docMatch), enc(3, leafC2G0Cars0, docNoMatch)),
			carsModel: roaringset.NewBitmap(enc(3, leafC2G0Cars0, docMatch), enc(3, leafC2G0Cars1, docNoMatch)),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F5: countries[0].garages.city AND countries[1].garages.postcode AND
	//     countries[2].garages[3].cars.{make,model}
	// -----------------------------------------------------------------------
	t.Run("F5_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[2].garages[3].cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafC0G0Direct = uint16(1)
			leafC1G0Direct = uint16(1)
			leafC2G3Cars0  = uint16(1)
			leafC2G3Cars1  = uint16(2)
		)
		mb := newIdxBucket(t)

		writeIdx(t, mb, "", 0, []uint64{enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch)})
		writeIdx(t, mb, "", 1, []uint64{enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch)})
		writeIdx(t, mb, "", 2, []uint64{
			enc(3, leafC2G3Cars0, docMatch),
			enc(3, leafC2G3Cars0, docNoMatch), enc(3, leafC2G3Cars1, docNoMatch),
		})
		writeIdx(t, mb, "garages", 0, []uint64{
			enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch),
			enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch),
		})
		writeIdx(t, mb, "garages", 3, []uint64{
			enc(3, leafC2G3Cars0, docMatch),
			enc(3, leafC2G3Cars0, docNoMatch), enc(3, leafC2G3Cars1, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 0, []uint64{enc(3, leafC2G3Cars0, docMatch), enc(3, leafC2G3Cars0, docNoMatch)})
		writeIdx(t, mb, "garages.cars", 1, []uint64{enc(3, leafC2G3Cars1, docNoMatch)})

		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}
		idx2c := filnested.ArrayIndex{RelPath: "", Index: 2}
		idx3g := filnested.ArrayIndex{RelPath: "garages", Index: 3}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx2c, idx3g)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx2c, idx3g)

		raws := map[*propValuePair]*sroar.Bitmap{
			city:      roaringset.NewBitmap(enc(1, leafC0G0Direct, docMatch), enc(1, leafC0G0Direct, docNoMatch)),
			postcode:  roaringset.NewBitmap(enc(2, leafC1G0Direct, docMatch), enc(2, leafC1G0Direct, docNoMatch)),
			carsMake:  roaringset.NewBitmap(enc(3, leafC2G3Cars0, docMatch), enc(3, leafC2G3Cars0, docNoMatch)),
			carsModel: roaringset.NewBitmap(enc(3, leafC2G3Cars0, docMatch), enc(3, leafC2G3Cars1, docNoMatch)),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F6: countries.garages.city AND postcode AND cars.{make,model} (no indices)
	// -----------------------------------------------------------------------
	t.Run("F6_countries.garages.city_AND_postcode_AND_cars.{make,model}", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Direct = uint16(1)
			leafG0Cars0  = uint16(2)
			leafG0Cars1  = uint16(3)
		)
		mb := newIdxBucket(t)

		writeIdx(t, mb, "garages", 0, []uint64{
			enc(1, leafG0Direct, docMatch), enc(1, leafG0Cars0, docMatch),
			enc(1, leafG0Direct, docNoMatch), enc(1, leafG0Cars0, docNoMatch), enc(1, leafG0Cars1, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 0, []uint64{enc(1, leafG0Cars0, docMatch), enc(1, leafG0Cars0, docNoMatch)})
		writeIdx(t, mb, "garages.cars", 1, []uint64{enc(1, leafG0Cars1, docNoMatch)})

		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		carsMake := makeLeafPvp(class, "countries", "garages.cars.make", "honda")
		carsModel := makeLeafPvp(class, "countries", "garages.cars.model", "civic")

		raws := map[*propValuePair]*sroar.Bitmap{
			city:      roaringset.NewBitmap(enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)),
			postcode:  roaringset.NewBitmap(enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)),
			carsMake:  roaringset.NewBitmap(enc(1, leafG0Cars0, docMatch), enc(1, leafG0Cars0, docNoMatch)),
			carsModel: roaringset.NewBitmap(enc(1, leafG0Cars0, docMatch), enc(1, leafG0Cars1, docNoMatch)),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F7: countries.garages.city AND postcode AND cars.accessories.type AND cars.tags
	// -----------------------------------------------------------------------
	t.Run("F7_countries.garages.city_AND_postcode_AND_cars.accessories.type_AND_cars.tags", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Direct      = uint16(1)
			leafG0Cars0Tag    = uint16(2)
			leafG0Cars0Access = uint16(3)
			leafG0Cars1Tag    = uint16(4)
		)
		mb := newIdxBucket(t)

		writeIdx(t, mb, "garages", 0, []uint64{
			enc(1, leafG0Direct, docMatch), enc(1, leafG0Cars0Tag, docMatch), enc(1, leafG0Cars0Access, docMatch),
			enc(1, leafG0Direct, docNoMatch), enc(1, leafG0Cars0Access, docNoMatch), enc(1, leafG0Cars1Tag, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 0, []uint64{
			enc(1, leafG0Cars0Tag, docMatch), enc(1, leafG0Cars0Access, docMatch),
			enc(1, leafG0Cars0Access, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 1, []uint64{enc(1, leafG0Cars1Tag, docNoMatch)})
		writeIdx(t, mb, "garages.cars.accessories", 0, []uint64{
			enc(1, leafG0Cars0Access, docMatch),
			enc(1, leafG0Cars0Access, docNoMatch),
		})

		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		accType := makeLeafPvp(class, "countries", "garages.cars.accessories.type", "spolier")
		tags := makeLeafPvp(class, "countries", "garages.cars.tags", "electric")

		raws := map[*propValuePair]*sroar.Bitmap{
			city:     roaringset.NewBitmap(enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)),
			postcode: roaringset.NewBitmap(enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)),
			accType:  roaringset.NewBitmap(enc(1, leafG0Cars0Access, docMatch), enc(1, leafG0Cars0Access, docNoMatch)),
			tags:     roaringset.NewBitmap(enc(1, leafG0Cars0Tag, docMatch), enc(1, leafG0Cars1Tag, docNoMatch)),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", city, postcode, accType, tags), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", tags, accType, postcode, city), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F8: countries.garages.city AND postcode AND cars.accessories.type AND cars.tires.width
	// -----------------------------------------------------------------------
	t.Run("F8_countries.garages.city_AND_postcode_AND_cars.accessories.type_AND_cars.tires.width", func(t *testing.T) {
		const (
			docMatch   = uint64(1)
			docNoMatch = uint64(2)

			leafG0Direct      = uint16(1)
			leafG0Cars0Access = uint16(2)
			leafG0Cars0Tire   = uint16(3)
			leafG0Cars1Tire   = uint16(4)
		)
		mb := newIdxBucket(t)

		writeIdx(t, mb, "garages", 0, []uint64{
			enc(1, leafG0Direct, docMatch), enc(1, leafG0Cars0Access, docMatch), enc(1, leafG0Cars0Tire, docMatch),
			enc(1, leafG0Direct, docNoMatch), enc(1, leafG0Cars0Access, docNoMatch), enc(1, leafG0Cars1Tire, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 0, []uint64{
			enc(1, leafG0Cars0Access, docMatch), enc(1, leafG0Cars0Tire, docMatch),
			enc(1, leafG0Cars0Access, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars", 1, []uint64{enc(1, leafG0Cars1Tire, docNoMatch)})
		writeIdx(t, mb, "garages.cars.accessories", 0, []uint64{
			enc(1, leafG0Cars0Access, docMatch),
			enc(1, leafG0Cars0Access, docNoMatch),
		})
		writeIdx(t, mb, "garages.cars.tires", 0, []uint64{
			enc(1, leafG0Cars0Tire, docMatch),
			enc(1, leafG0Cars1Tire, docNoMatch),
		})

		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		accType := makeLeafPvp(class, "countries", "garages.cars.accessories.type", "spolier")
		tireWidth := makeLeafPvp(class, "countries", "garages.cars.tires.width", "225")

		raws := map[*propValuePair]*sroar.Bitmap{
			city:      roaringset.NewBitmap(enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)),
			postcode:  roaringset.NewBitmap(enc(1, leafG0Direct, docMatch), enc(1, leafG0Direct, docNoMatch)),
			accType:   roaringset.NewBitmap(enc(1, leafG0Cars0Access, docMatch), enc(1, leafG0Cars0Access, docNoMatch)),
			tireWidth: roaringset.NewBitmap(enc(1, leafG0Cars0Tire, docMatch), enc(1, leafG0Cars1Tire, docNoMatch)),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", city, postcode, accType, tireWidth), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", tireWidth, accType, postcode, city), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F13: garages.cars[0].make AND garages.cars[1].model — pure split with
	// ≥2 constrained buckets at intermediate scope. groupChildrenByArrayIndicesKey
	// would split these into separate compatibility groups (conflicting indices
	// at "garages.cars"), so the executor's len(branches)>1 + lcaPath != ""
	// path is unreachable via production dispatch. This test drives the planner
	// directly to lock in evalSplit's rootDoc-level AndAll across branches.
	//
	// Plan: SPLIT@"garages.cars" with branches:
	//   idx=0 → GROUP@"garages.cars" here=[make]   (canUseRawAndAll)
	//   idx=1 → GROUP@"garages.cars" here=[model]  (canUseRawAndAll)
	// Branch results are MaskLeaf'd raws (rootDoc shape); evalSplit AndAlls at
	// rootDoc level (no MaskRootLeaf) — same-root semantics enforced by root_idx.
	//
	// docMatch:    countries[1].garages[0].cars[0].make=honda AND
	//              countries[1].garages[0].cars[1].model=civic → match
	// docNoMatch1: cars[0].make only — cars[1] branch empty for the doc
	// docNoMatch2: make in countries[1].cars[0]; model in countries[2].cars[1]
	//              — different root_idx → AndAll at rootDoc empty
	// docNoMatch3: make in cars[1] (wrong index) AND model in cars[0] (wrong
	//              index) — both branches empty (raw leaf misses branchScope)
	// -----------------------------------------------------------------------
	t.Run("F13_garages.cars[0].make_AND_garages.cars[1].model_pure_split_intermediate", func(t *testing.T) {
		const (
			docMatch    = uint64(1)
			docNoMatch1 = uint64(2)
			docNoMatch2 = uint64(3)
			docNoMatch3 = uint64(4)

			leafC1G0Cars0 = uint16(1)
			leafC1G0Cars1 = uint16(2)
			leafC2G0Cars1 = uint16(3)
		)
		mb := newIdxBucket(t)

		// _idx.garages.cars[0]: cars[0] positions across all garages/countries.
		writeIdx(t, mb, "garages.cars", 0, []uint64{
			enc(1, leafC1G0Cars0, docMatch),
			enc(1, leafC1G0Cars0, docNoMatch1),
			enc(1, leafC1G0Cars0, docNoMatch2),
			enc(1, leafC1G0Cars0, docNoMatch3), // model leaf for docNoMatch3 (wrong car)
		})
		// _idx.garages.cars[1]: cars[1] positions across all garages/countries.
		writeIdx(t, mb, "garages.cars", 1, []uint64{
			enc(1, leafC1G0Cars1, docMatch),
			enc(2, leafC2G0Cars1, docNoMatch2),
			enc(1, leafC1G0Cars1, docNoMatch3), // make leaf for docNoMatch3 (wrong car)
		})

		idx0cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 0}
		idx1cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 1}

		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx0cars)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx1cars)

		raws := map[*propValuePair]*sroar.Bitmap{
			carsMake: roaringset.NewBitmap(
				enc(1, leafC1G0Cars0, docMatch),
				enc(1, leafC1G0Cars0, docNoMatch1),
				enc(1, leafC1G0Cars0, docNoMatch2),
				enc(1, leafC1G0Cars1, docNoMatch3),
			),
			carsModel: roaringset.NewBitmap(
				enc(1, leafC1G0Cars1, docMatch),
				enc(2, leafC2G0Cars1, docNoMatch2),
				enc(1, leafC1G0Cars0, docNoMatch3),
			),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", carsMake, carsModel), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", carsModel, carsMake), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F15: countries[0].garages.city AND countries[1].garages.postcode — pure
	// split with ≥2 constrained buckets at root scope (lcaPath=""). Like F13,
	// groupChildrenByArrayIndicesKey would split these into separate
	// compatibility groups (conflicting indices at RelPath=""), so the
	// executor's len(branches)>1 + lcaPath=="" path is unreachable via
	// production dispatch. This test drives the planner directly to lock in
	// evalSplit's andBranchesAtDocID combiner — the docID-level AND that the
	// intermediate-scope F13 path does not exercise.
	//
	// Plan: SPLIT@"" with branches:
	//   idx=0 → GROUP@"garages" here=[city]      (canUseRawAndAll)
	//   idx=1 → GROUP@"garages" here=[postcode]  (canUseRawAndAll)
	// Each branch returns a rootDoc bitmap pinned to a different root_idx, so
	// AndAll at rootDoc level would yield empty. evalSplit detects lcaPath=""
	// + len(branches)>1, MaskRootLeafs each branch, then ANDs at docID level.
	//
	// docMatch:    c[0].garages[0].city=berlin AND c[1].garages[0].postcode=12345
	// docNoMatch1: only c[0].city — branch[1] empty for this doc (no c[1].postcode)
	// docNoMatch2: only c[1].postcode — branch[0] empty for this doc (no c[0].city)
	// docNoMatch3: city in c[1] (wrong country) AND postcode in c[0] (wrong
	//              country) — both raws ∩ idx[""][k] miss this doc, so neither
	//              branch carries it
	// -----------------------------------------------------------------------
	t.Run("F15_countries[0].garages.city_AND_countries[1].garages.postcode_pure_split_root", func(t *testing.T) {
		const (
			docMatch    = uint64(1)
			docNoMatch1 = uint64(2)
			docNoMatch2 = uint64(3)
			docNoMatch3 = uint64(4)

			leafC0 = uint16(1)
			leafC1 = uint16(2)
		)
		mb := newIdxBucket(t)

		// _idx[""][0]: positions in country[0] (root_idx=1) across all docs.
		writeIdx(t, mb, "", 0, []uint64{
			enc(1, leafC0, docMatch),
			enc(1, leafC0, docNoMatch1),
			enc(1, leafC0, docNoMatch3), // postcode (wrong country for postcode)
		})
		// _idx[""][1]: positions in country[1] (root_idx=2) across all docs.
		writeIdx(t, mb, "", 1, []uint64{
			enc(2, leafC1, docMatch),
			enc(2, leafC1, docNoMatch2),
			enc(2, leafC1, docNoMatch3), // city (wrong country for city)
		})

		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)

		raws := map[*propValuePair]*sroar.Bitmap{
			city: roaringset.NewBitmap(
				enc(1, leafC0, docMatch),
				enc(1, leafC0, docNoMatch1),
				enc(2, leafC1, docNoMatch3), // city in c[1] — outside idx[""][0]
			),
			postcode: roaringset.NewBitmap(
				enc(2, leafC1, docMatch),
				enc(2, leafC1, docNoMatch2),
				enc(1, leafC0, docNoMatch3), // postcode in c[0] — outside idx[""][1]
			),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", city, postcode), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", postcode, city), mb, raws, []uint64{docMatch})
	})

	// -----------------------------------------------------------------------
	// F16: garages[0].cars.{make, tires.width} AND garages[1].cars.{make,
	// tires.width} — multi-branch SPLIT@"garages" where each branch carries a
	// non-flat GROUP (here + sub) instead of the flat 1-leaf branches of F13.
	// This exercises evalSplitBranch dispatching to evalNode → evalGroup →
	// runIdxLoopRecursive (since the branch's GROUP has subs, canUseRawAndAll
	// is rejected) and parentScope propagation through nested cars/tires
	// scopes — a path the flat branches in F13/F15 do not reach.
	//
	// Plan (driven through the planner — conflicting "garages" indices would
	// be split into separate compatibility groups by dispatch):
	//   SPLIT@"garages"
	//     branches:
	//       idx=0 → GROUP@"garages.cars" here=[make] subs=[GROUP@"garages.cars.tires" here=[width]]
	//       idx=1 → GROUP@"garages.cars" here=[make] subs=[GROUP@"garages.cars.tires" here=[width]]
	//
	// docMatch:    garages[0].cars[0]={make=honda, tires[0].width=205} AND
	//              garages[1].cars[0]={make=ferrari, tires[0].width=225}
	//              → both branches match for the same root, AndAll succeeds
	// docNoMatch1: garages[0] correct; garages[1].cars[0]={make=ferrari} but
	//              tires.width=225 sits in cars[1] → branch[1]'s sub fails the
	//              AND inside cars[0]'s scope (no width in cars[0].tires); the
	//              cars[1] iteration fails the make leaf; branch[1] empty
	// docNoMatch2: only garages[0] populated → branch[1] empty for this doc
	// -----------------------------------------------------------------------
	t.Run("F16_garages[0]_AND_garages[1]_each_with_make_AND_tires.width_split_with_subs", func(t *testing.T) {
		const (
			docMatch    = uint64(1)
			docNoMatch1 = uint64(2)
			docNoMatch2 = uint64(3)

			// docMatch leaves
			leafM_G0_C0_make  = uint16(1)
			leafM_G0_C0_width = uint16(2)
			leafM_G1_C0_make  = uint16(3)
			leafM_G1_C0_width = uint16(4)

			// docNoMatch1 leaves — garages[1].cars[0].make is right but
			// tires.width=225 sits in garages[1].cars[1] (different car)
			leafN1_G0_C0_make  = uint16(11)
			leafN1_G0_C0_width = uint16(12)
			leafN1_G1_C0_make  = uint16(13)
			leafN1_G1_C1_width = uint16(14)

			// docNoMatch2 leaves — only garages[0]
			leafN2_G0_C0_make  = uint16(21)
			leafN2_G0_C0_width = uint16(22)
		)
		mb := newIdxBucket(t)

		// All docs encode at root_idx=1 (countries[0]) — F16 has no
		// countries-level constraint, so the root position is uniform.
		// _idx.garages[0]: positions under garages[0]
		writeIdx(t, mb, "garages", 0, []uint64{
			enc(1, leafM_G0_C0_make, docMatch),
			enc(1, leafM_G0_C0_width, docMatch),
			enc(1, leafN1_G0_C0_make, docNoMatch1),
			enc(1, leafN1_G0_C0_width, docNoMatch1),
			enc(1, leafN2_G0_C0_make, docNoMatch2),
			enc(1, leafN2_G0_C0_width, docNoMatch2),
		})
		// _idx.garages[1]: positions under garages[1]
		writeIdx(t, mb, "garages", 1, []uint64{
			enc(1, leafM_G1_C0_make, docMatch),
			enc(1, leafM_G1_C0_width, docMatch),
			enc(1, leafN1_G1_C0_make, docNoMatch1),
			enc(1, leafN1_G1_C1_width, docNoMatch1),
		})
		// _idx.garages.cars[0]: positions under cars[0] across all garages
		writeIdx(t, mb, "garages.cars", 0, []uint64{
			enc(1, leafM_G0_C0_make, docMatch),
			enc(1, leafM_G0_C0_width, docMatch),
			enc(1, leafM_G1_C0_make, docMatch),
			enc(1, leafM_G1_C0_width, docMatch),
			enc(1, leafN1_G0_C0_make, docNoMatch1),
			enc(1, leafN1_G0_C0_width, docNoMatch1),
			enc(1, leafN1_G1_C0_make, docNoMatch1),
			enc(1, leafN2_G0_C0_make, docNoMatch2),
			enc(1, leafN2_G0_C0_width, docNoMatch2),
		})
		// _idx.garages.cars[1]: cars[1] — only docNoMatch1.garages[1].cars[1]
		writeIdx(t, mb, "garages.cars", 1, []uint64{
			enc(1, leafN1_G1_C1_width, docNoMatch1),
		})
		// _idx.garages.cars.tires[0]: positions under tires[0] across all cars
		writeIdx(t, mb, "garages.cars.tires", 0, []uint64{
			enc(1, leafM_G0_C0_width, docMatch),
			enc(1, leafM_G1_C0_width, docMatch),
			enc(1, leafN1_G0_C0_width, docNoMatch1),
			enc(1, leafN1_G1_C1_width, docNoMatch1),
			enc(1, leafN2_G0_C0_width, docNoMatch2),
		})

		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}

		makeHonda := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx0g)
		width205 := makeLeafPvpWithIdx(class, "countries", "garages.cars.tires.width", "205", idx0g)
		makeFerrari := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "ferrari", idx1g)
		width225 := makeLeafPvpWithIdx(class, "countries", "garages.cars.tires.width", "225", idx1g)

		raws := map[*propValuePair]*sroar.Bitmap{
			makeHonda: roaringset.NewBitmap(
				enc(1, leafM_G0_C0_make, docMatch),
				enc(1, leafN1_G0_C0_make, docNoMatch1),
				enc(1, leafN2_G0_C0_make, docNoMatch2),
			),
			width205: roaringset.NewBitmap(
				enc(1, leafM_G0_C0_width, docMatch),
				enc(1, leafN1_G0_C0_width, docNoMatch1),
				enc(1, leafN2_G0_C0_width, docNoMatch2),
			),
			makeFerrari: roaringset.NewBitmap(
				enc(1, leafM_G1_C0_make, docMatch),
				enc(1, leafN1_G1_C0_make, docNoMatch1),
			),
			width225: roaringset.NewBitmap(
				enc(1, leafM_G1_C0_width, docMatch),
				enc(1, leafN1_G1_C1_width, docNoMatch1),
			),
		}

		runRec(t, makeCorrelatedPvp(class, "countries", makeHonda, width205, makeFerrari, width225), mb, raws, []uint64{docMatch})
		runRec(t, makeCorrelatedPvp(class, "countries", width225, makeFerrari, width205, makeHonda), mb, raws, []uint64{docMatch})
	})

	// runRecEmpty mirrors runRec but expects an empty result. The leak-tracking
	// pool's t.Cleanup catches any unreleased buffer; the empty assertion
	// verifies the executor produced no docIDs. requireBitmapValid is skipped
	// because a legitimately empty bitmap has NumContainers()==0 — the same
	// signal it uses to detect a zeroed (released) buffer.
	runRecEmpty := func(t *testing.T, pv *propValuePair, mb *lsmkv.Bucket, rawsByCond map[*propValuePair]*sroar.Bitmap) {
		t.Helper()
		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build(pv.children)
		exec := newRecExecutor(rawsByCond, mb, ops, concurrency.SROAR_MERGE)
		result, release, err := exec.execute(context.Background(), plan)
		require.NoError(t, err)
		defer release()
		assert.Empty(t, result.ToArray())
	}

	// -----------------------------------------------------------------------
	// E1: SPLIT branch references a non-existent _idx[K]. evalSplitBranch reads
	// _idx.garages[99] via RoaringSetGet, finds it empty, and returns a fresh
	// NewEmpty bitmap. The original (empty) idxK still gets released via its
	// deferred idxRel — both lifecycle paths must fire cleanly so the tracking
	// pool reports zero outstanding buffers at teardown.
	// -----------------------------------------------------------------------
	t.Run("E1_split_branch_nonexistent_idx_returns_empty", func(t *testing.T) {
		mb := newIdxBucket(t)
		// _idx.garages[1] exists with one entry; _idx.garages[99] is absent.
		writeIdx(t, mb, "garages", 1, []uint64{enc(1, 1, 1)})

		idx99g := filnested.ArrayIndex{RelPath: "garages", Index: 99}
		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx99g)

		raws := map[*propValuePair]*sroar.Bitmap{
			city: roaringset.NewBitmap(enc(1, 1, 1)),
		}

		runRecEmpty(t, makeCorrelatedPvp(class, "countries", city), mb, raws)
	})

	// -----------------------------------------------------------------------
	// E2: empty raw bitmap at deepest leaf. evalGroupRoot's canUseRawAndAll
	// path runs AndAll over [raw_make, branchScope]; raw_make is empty so the
	// AndAll result is empty. The intermediate AndAll buffer and final MaskLeaf
	// buffer both come from the pool — both must be released through the
	// success path even when the final docID set is empty.
	// -----------------------------------------------------------------------
	t.Run("E2_empty_raw_at_deepest_leaf_returns_empty", func(t *testing.T) {
		mb := newIdxBucket(t)
		// _idx.garages[1] is non-empty so evalSplitBranch produces a real
		// branchScope rather than short-circuiting (we want the AndAll to run).
		writeIdx(t, mb, "garages", 1, []uint64{enc(1, 1, 1)})

		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		make := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "ferrari", idx1g)

		raws := map[*propValuePair]*sroar.Bitmap{
			make: roaringset.NewBitmap(), // no doc carries make=ferrari
		}

		runRecEmpty(t, makeCorrelatedPvp(class, "countries", make), mb, raws)
	})

	// -----------------------------------------------------------------------
	// E3: evalSplitBranch branchScope.IsEmpty short-circuit. A nested SPLIT
	// where the inner branch's _idx[K] is non-empty but disjoint from the
	// outer branch's parentScope. intersectScope ANDs them and yields empty,
	// triggering the second short-circuit (distinct from E1's idxK.IsEmpty).
	//
	// Filter: countries[0].garages.cars[5].make=honda. The plan is
	//   SPLIT@""[0] → SPLIT@"garages.cars"[5] → GROUP@"garages.cars" here=[make]
	// _idx[""][0] only carries country[0] positions (root_idx=1) and
	// _idx.garages.cars[5] only carries cars[5] positions in country[2]
	// (root_idx=3). Their AndAll is empty → branchScope.IsEmpty() fires inside
	// the inner evalSplitBranch, which returns NewEmpty(64). The pool buffer
	// allocated by intersectScope's AndAll must still be released through the
	// deferred branchScopeRel.
	// -----------------------------------------------------------------------
	t.Run("E3_split_branch_branchScope_empty_short_circuit", func(t *testing.T) {
		mb := newIdxBucket(t)
		// Outer: country[0] positions (root_idx=1).
		writeIdx(t, mb, "", 0, []uint64{enc(1, 1, 1)})
		// Inner: cars[5] positions exist, but only inside country[2] (root_idx=3)
		// — disjoint from the outer parentScope, so the AndAll yields empty.
		writeIdx(t, mb, "garages.cars", 5, []uint64{enc(3, 5, 2)})

		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx5cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 5}
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx0c, idx5cars)

		raws := map[*propValuePair]*sroar.Bitmap{
			// raw is intentionally non-empty so the empty result must come from
			// the branchScope.IsEmpty short-circuit, not from an AndAll over an
			// empty raw bitmap (E2's path).
			carsMake: roaringset.NewBitmap(enc(3, 5, 2)),
		}

		runRecEmpty(t, makeCorrelatedPvp(class, "countries", carsMake), mb, raws)
	})
}

// TestRecExecutorReturnMasked exercises withReturnMasked across both execute
// paths. It runs the same scenario twice — once unmasked (final docIDs), once
// masked (root+docID positions) — and asserts that MaskRootLeaf'ing the masked
// output produces the same docID set as the unmasked output. This is the
// invariant resolveMultiGroupRootDocIDAnd relies on when ANDing groups at
// root+docID level before stripping root bits.
func TestRecExecutorReturnMasked(t *testing.T) {
	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }

	t.Run("normal_path_returnMasked_then_MaskRootLeaf_equals_unmasked", func(t *testing.T) {
		const (
			docMatch   = uint64(101)
			docNoMatch = uint64(102)
		)
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		s, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := correlationTestClass()

		vb := store.Bucket(valueBucket)
		writeNestedValue(t, vb, "city", "berlin", []uint64{enc(1, 1, docMatch), enc(1, 1, docNoMatch)})
		writeNestedValue(t, vb, "postcode", "10115", []uint64{enc(1, 1, docMatch), enc(1, 2, docNoMatch)})

		pv := makeCorrelatedPvp(class, "addresses",
			makeLeafPvp(class, "addresses", "city", "berlin"),
			makeLeafPvp(class, "addresses", "postcode", "10115"),
		)

		// Unmasked: plain docIDs.
		planU, execU, relsU, err := pv.buildRecGroupExecutor(context.Background(), s, pv.children)
		require.NoError(t, err)
		docs, docsRel, err := execU.execute(context.Background(), planU)
		require.NoError(t, err)
		unmaskedDocs := docs.ToArray()
		docsRel()
		for _, rel := range relsU {
			rel()
		}

		// Masked: root+docID positions. MaskRootLeaf'ing must give the same docs.
		planM, execM, relsM, err := pv.buildRecGroupExecutor(context.Background(), s, pv.children)
		require.NoError(t, err)
		execM.withReturnMasked(true)
		masked, maskedRel, err := execM.execute(context.Background(), planM)
		require.NoError(t, err)
		ops := newLifecycleOps(t)
		fromMasked, fromMaskedRel := ops.MaskRootLeaf(masked)
		assert.Equal(t, unmaskedDocs, fromMasked.ToArray(),
			"MaskRootLeaf(returnMasked output) must equal unmasked output")
		fromMaskedRel()
		maskedRel()
		for _, rel := range relsM {
			rel()
		}
	})

	t.Run("root_anchor_path_returnMasked_then_MaskRootLeaf_equals_unmasked", func(t *testing.T) {
		// addresses.city IS NULL — no positives, only excludes; execute takes the
		// rootAnchor branch in both modes. Same MaskRootLeaf invariant must hold.
		const (
			docMatch   = uint64(111)
			docNoMatch = uint64(112)
		)
		valueBucket := helpers.BucketNestedFromPropNameLSM("addresses")
		metaBucket := helpers.BucketNestedMetaFromPropNameLSM("addresses")
		s, store := newNestedTestSearcher(t, valueBucket, metaBucket)
		class := correlationTestClass()

		mb := store.Bucket(metaBucket)
		require.NoError(t, mb.RoaringSetAddList(invnested.ExistsKey(""), []uint64{
			enc(1, 1, docMatch), enc(1, 1, docNoMatch),
		}))
		require.NoError(t, mb.RoaringSetAddList(invnested.ExistsKey("city"), []uint64{
			enc(1, 1, docNoMatch),
		}))

		pv := makeCorrelatedPvp(class, "addresses",
			makeIsNullPvp(class, "addresses", "city", true),
		)

		planU, execU, relsU, err := pv.buildRecGroupExecutor(context.Background(), s, pv.children)
		require.NoError(t, err)
		docs, docsRel, err := execU.execute(context.Background(), planU)
		require.NoError(t, err)
		unmaskedDocs := docs.ToArray()
		docsRel()
		for _, rel := range relsU {
			rel()
		}

		planM, execM, relsM, err := pv.buildRecGroupExecutor(context.Background(), s, pv.children)
		require.NoError(t, err)
		execM.withReturnMasked(true)
		masked, maskedRel, err := execM.execute(context.Background(), planM)
		require.NoError(t, err)
		assert.Nil(t, planM, "rootAnchor scenario yields nil plan")
		ops := newLifecycleOps(t)
		fromMasked, fromMaskedRel := ops.MaskRootLeaf(masked)
		assert.Equal(t, unmaskedDocs, fromMasked.ToArray(),
			"MaskRootLeaf(returnMasked rootAnchor output) must equal unmasked output")
		fromMaskedRel()
		maskedRel()
		for _, rel := range relsM {
			rel()
		}
	})
}

// TestRecExecutorRootAnchor exercises the executeRootAnchor path of execute()
// — the branch taken when plan==nil. The path applies for correlated AND
// filters that contain only IsNull conditions: there is no positive anchor, so
// the executor uses _exists.{scope} as the element universe. Each exclude (a
// raw _exists.{path} position bitmap) is AndNot'd at raw level (preserving
// leaf alignment with the anchor), then MaskLeaf collapses to rootDoc, then
// MaskRootLeaf to docIDs.
//
// TestRecExecutorReturnMasked already proves the masked-vs-unmasked invariant
// for this path. This test exercises absolute-result and lifecycle invariants:
// single vs. multiple excludes, partial-element subtraction (one element of a
// multi-element doc excluded while another survives), and the nil-anchor
// error.
func TestRecExecutorRootAnchor(t *testing.T) {
	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }

	t.Run("anchor_minus_single_exclude_drops_doc", func(t *testing.T) {
		// doc100 has one element at (root=1, leaf=1); doc101 has one element
		// at (root=2, leaf=1). The exclude removes doc100's only element.
		anchor := roaringset.NewBitmap(enc(1, 1, 100), enc(2, 1, 101))
		exclude := roaringset.NewBitmap(enc(1, 1, 100))

		ops := newLifecycleOps(t)
		exec := newRecExecutor(nil, nil, ops, concurrency.SROAR_MERGE).
			withRootAnchor(anchor).
			withExcludes([]recExclude{{bitmap: exclude}})

		result, release, err := exec.execute(context.Background(), nil)
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{101}, result.ToArray())
	})

	t.Run("anchor_minus_multiple_excludes_loops", func(t *testing.T) {
		// The AndNot loop must run for every exclude and accumulate
		// subtractions. Two excludes drop two distinct docs from the universe.
		anchor := roaringset.NewBitmap(
			enc(1, 1, 200), enc(1, 1, 201), enc(1, 1, 202),
			enc(1, 1, 203), enc(1, 1, 204),
		)
		excl1 := roaringset.NewBitmap(enc(1, 1, 201))
		excl2 := roaringset.NewBitmap(enc(1, 1, 203))

		ops := newLifecycleOps(t)
		exec := newRecExecutor(nil, nil, ops, concurrency.SROAR_MERGE).
			withRootAnchor(anchor).
			withExcludes([]recExclude{{bitmap: excl1}, {bitmap: excl2}})

		result, release, err := exec.execute(context.Background(), nil)
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{200, 202, 204}, result.ToArray())
	})

	t.Run("partial_element_exclude_keeps_doc_via_other_leaf", func(t *testing.T) {
		// Leaf-precise subtraction: doc300 has two elements (leaf=1 and
		// leaf=2). The exclude removes only leaf=1's position — leaf=2
		// survives the AndNot, so MaskLeaf+MaskRootLeaf still emits doc300.
		// doc301's only element is excluded so doc301 drops out entirely.
		anchor := roaringset.NewBitmap(
			enc(1, 1, 300), enc(1, 2, 300),
			enc(1, 1, 301),
		)
		exclude := roaringset.NewBitmap(
			enc(1, 1, 300),
			enc(1, 1, 301),
		)

		ops := newLifecycleOps(t)
		exec := newRecExecutor(nil, nil, ops, concurrency.SROAR_MERGE).
			withRootAnchor(anchor).
			withExcludes([]recExclude{{bitmap: exclude}})

		result, release, err := exec.execute(context.Background(), nil)
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{300}, result.ToArray())
	})

	t.Run("nil_rootAnchor_returns_error", func(t *testing.T) {
		ops := newLifecycleOps(t)
		exec := newRecExecutor(nil, nil, ops, concurrency.SROAR_MERGE)
		_, _, err := exec.execute(context.Background(), nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rootAnchor")
	})
}

// TestRecExecutorExcludePositions exercises the exclude-subtraction loop in
// execute() — the non-rootAnchor path. evalNode produces a rootDoc bitmap; for
// each exclude (a raw _exists.{path} bitmap) execute MaskLeafs the exclude and
// AndNots it from bm. Subtraction is at root+docID level — distinct from the
// raw-level (leaf-precise) subtraction in executeRootAnchor.
func TestRecExecutorExcludePositions(t *testing.T) {
	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }
	class := filterExamplesClass()
	props := rootNestedProps(t, class, "countries")

	t.Run("multi_exclude_loop_subtracts_at_rootDoc_level", func(t *testing.T) {
		// Plan-driven path: single positive condition `garages.city=berlin`
		// produces GROUP@"garages" here=[garages.city]. canUseRawAndAll runs
		// AndAll([raw_city]) → MaskLeaf → bm at rootDoc shape. The exclude
		// loop in execute() then MaskLeafs each exclude (collapsing leaf bits)
		// and AndNots from bm. Two excludes, one applying to two docs each
		// (with overlap on doc4), verify the loop iterates and accumulates.
		//
		// doc1: city only — kept.
		// doc2: city + zip exists in a different leaf — excludeZip drops it.
		// doc3: city + age exists — excludeAge drops it.
		// doc4: city + zip + age — both excludes apply (idempotent AndNot).
		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		rawCity := roaringset.NewBitmap(
			enc(1, 1, 1),
			enc(2, 1, 2),
			enc(3, 1, 3),
			enc(4, 1, 4),
		)
		excludeZip := roaringset.NewBitmap(
			enc(2, 5, 2),
			enc(4, 5, 4),
		)
		excludeAge := roaringset.NewBitmap(
			enc(3, 7, 3),
			enc(4, 7, 4),
		)

		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build([]*propValuePair{city})
		exec := newRecExecutor(map[*propValuePair]*sroar.Bitmap{city: rawCity}, nil, ops, concurrency.SROAR_MERGE).
			withExcludes([]recExclude{{bitmap: excludeZip}, {bitmap: excludeAge}})

		result, release, err := exec.execute(context.Background(), plan)
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{1}, result.ToArray())
	})

	t.Run("exclude_drops_only_matching_root_element_other_root_survives", func(t *testing.T) {
		// Subtraction granularity is the (root_idx, docID) pair, not the doc
		// alone. doc500 has city=berlin in two different root elements
		// (root_idx=1 and root_idx=2). zip exists only in root_idx=1. The
		// excluded MaskLeaf'd entry (1, 0, 500) is AndNot'd; (2, 0, 500)
		// survives, so doc500 still appears after MaskRootLeaf.
		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		rawCity := roaringset.NewBitmap(enc(1, 1, 500), enc(2, 1, 500))
		excludeZip := roaringset.NewBitmap(enc(1, 5, 500))

		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build([]*propValuePair{city})
		exec := newRecExecutor(map[*propValuePair]*sroar.Bitmap{city: rawCity}, nil, ops, concurrency.SROAR_MERGE).
			withExcludes([]recExclude{{bitmap: excludeZip}})

		result, release, err := exec.execute(context.Background(), plan)
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{500}, result.ToArray())
	})

	t.Run("matching_lcaPath_subtracts_raw_inside_canUseRawAndAll_group", func(t *testing.T) {
		// §8.5: when the exclude's lcaPath equals the group's lcaPath, the
		// exclude is AndNot'd at raw level on the group's AndAll'd raw bitmap
		// (before MaskLeaf). This preserves leaf-precise per-element semantics.
		// Plan: GROUP@"garages" here=[garages.city] (single contributor →
		// canUseRawAndAll). Exclude: garages.zip _exists at lcaPath="garages"
		// matches. doc1 has city in garages[1] (leaf=3) and garages[2] (leaf=5),
		// zip exists in garages[1] only (leaf=3). Raw AndNot drops only the
		// leaf=3 position; leaf=5 survives MaskLeaf → doc1 is kept.
		// doc2 has city only in garages[1] where zip also exists → dropped.
		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		rawCity := roaringset.NewBitmap(
			enc(1, 3, 1), enc(1, 5, 1),
			enc(1, 3, 2),
		)
		excludeZip := roaringset.NewBitmap(
			enc(1, 3, 1),
			enc(1, 3, 2),
		)

		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build([]*propValuePair{city})
		exec := newRecExecutor(map[*propValuePair]*sroar.Bitmap{city: rawCity}, nil, ops, concurrency.SROAR_MERGE).
			withExcludes([]recExclude{{bitmap: excludeZip, lcaPath: "garages"}}).
			withPlanLCAs(collectPlanLCAs(plan))

		result, release, err := exec.execute(context.Background(), plan)
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{1}, result.ToArray())
	})

	t.Run("non_matching_lcaPath_falls_through_to_rootDoc_subtract", func(t *testing.T) {
		// When the exclude's lcaPath is NOT in planLCAs, execute() applies
		// rootDoc-level subtraction (MaskLeaf+AndNot). Same data layout as the
		// §8.5 raw test, but lcaPath="garages.cars" is below the plan's only
		// group lcaPath "garages" — so the exclude collapses to rootDoc shape
		// (per-garage element) and drops doc1 entirely (root_idx=1 has zip in
		// some leaf), reproducing the pre-fix behavior for that combination.
		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		rawCity := roaringset.NewBitmap(
			enc(1, 3, 1), enc(1, 5, 1),
			enc(1, 3, 2),
		)
		excludeZip := roaringset.NewBitmap(
			enc(1, 3, 1),
			enc(1, 3, 2),
		)

		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build([]*propValuePair{city})
		exec := newRecExecutor(map[*propValuePair]*sroar.Bitmap{city: rawCity}, nil, ops, concurrency.SROAR_MERGE).
			withExcludes([]recExclude{{bitmap: excludeZip, lcaPath: "garages.cars"}}).
			withPlanLCAs(collectPlanLCAs(plan))

		result, release, err := exec.execute(context.Background(), plan)
		require.NoError(t, err)
		defer release()
		requireBitmapValid(t, result)
		assert.Empty(t, result.ToArray(), "rootDoc subtract drops both docs at (root,docID) granularity")
	})

	t.Run("returnMasked_keeps_rootDoc_after_excludes", func(t *testing.T) {
		// With returnMasked=true, execute returns the post-AndNot rootDoc
		// bitmap directly — MaskRootLeaf is skipped. Verify the equivalence
		// invariant: MaskRootLeaf'ing the masked result equals the unmasked
		// docID set. Confirms the exclude loop runs identically in both modes
		// (the returnMasked check happens after the loop, not before).
		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		rawCity := roaringset.NewBitmap(enc(1, 1, 700), enc(2, 1, 701))
		excludeZip := roaringset.NewBitmap(enc(2, 5, 701))

		opsU := newLifecycleOps(t)
		planU := newRecPlanBuilder(props).build([]*propValuePair{city})
		execU := newRecExecutor(map[*propValuePair]*sroar.Bitmap{city: rawCity}, nil, opsU, concurrency.SROAR_MERGE).
			withExcludes([]recExclude{{bitmap: excludeZip}})
		unmasked, unmaskedRel, err := execU.execute(context.Background(), planU)
		require.NoError(t, err)
		unmaskedDocs := unmasked.ToArray()
		unmaskedRel()

		opsM := newLifecycleOps(t)
		planM := newRecPlanBuilder(props).build([]*propValuePair{city})
		execM := newRecExecutor(map[*propValuePair]*sroar.Bitmap{city: rawCity}, nil, opsM, concurrency.SROAR_MERGE).
			withExcludes([]recExclude{{bitmap: excludeZip}}).
			withReturnMasked(true)
		masked, maskedRel, err := execM.execute(context.Background(), planM)
		require.NoError(t, err)
		ops := newLifecycleOps(t)
		fromMasked, fromMaskedRel := ops.MaskRootLeaf(masked)
		assert.Equal(t, unmaskedDocs, fromMasked.ToArray())
		fromMaskedRel()
		maskedRel()
	})
}

// TestRecExecutorContextCancellation asserts that the recursive executor
// honors a cancelled context across all entry paths.
//
// Two layers are exercised:
//
//   - Top-level: execute() checks ctxExpired at the very top, so any cancelled
//     context short-circuits regardless of plan shape. The four
//     `cancelled_via_*` sub-tests cover the four execute() dispatches:
//     rootAnchor (plan==nil), canUseRawAndAll (raw AndAll, no idx loop),
//     runIdxLoopRecursive (per-element idx iteration), and evalSplit
//     (multi-branch dispatch).
//
//   - Inner: runIdxLoopRecursive's start-of-function check and evalSplit's
//     per-branch check. These guard long-running work after the top-level
//     check has already passed (ctx cancels mid-flight). The
//     `inner_check_*` sub-tests bypass execute() and call evalNode directly
//     with a cancelled ctx so a future refactor that drops the top-level
//     check would still keep the inner contract.
func TestRecExecutorContextCancellation(t *testing.T) {
	enc := func(root, leaf uint16, docID uint64) uint64 { return invnested.Encode(root, leaf, docID) }
	class := filterExamplesClass()
	props := rootNestedProps(t, class, "countries")

	cancelled := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}

	t.Run("cancelled_via_rootAnchor_path", func(t *testing.T) {
		// plan==nil → executeRootAnchor; the top-level check fires before
		// the rootAnchor branch is taken.
		anchor := roaringset.NewBitmap(enc(1, 1, 100))
		ops := newLifecycleOps(t)
		exec := newRecExecutor(nil, nil, ops, concurrency.SROAR_MERGE).
			withRootAnchor(anchor)
		_, _, err := exec.execute(cancelled(), nil)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("cancelled_via_canUseRawAndAll_path", func(t *testing.T) {
		// Single positive at intermediate scope → GROUP@"garages.cars" with a
		// single here, no subs → canUseRawAndAll (no idx loop). The top-level
		// check returns before the AndAll runs.
		make := makeLeafPvp(class, "countries", "garages.cars.make", "honda")
		raw := roaringset.NewBitmap(enc(1, 1, 100))
		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build([]*propValuePair{make})
		exec := newRecExecutor(map[*propValuePair]*sroar.Bitmap{make: raw}, nil, ops, concurrency.SROAR_MERGE)
		_, _, err := exec.execute(cancelled(), plan)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("cancelled_via_idxLoop_path", func(t *testing.T) {
		// GROUP@"garages" here=[postcode] subs=[GROUP@"garages.cars"
		// here=[make]] forces evalGroup into runIdxLoopRecursive (subs reject
		// canUseRawAndAll, lcaPath != ""). The top-level check still fires
		// first, so the cursor loop is never entered.
		mb := newIdxBucket(t)
		writeIdx(t, mb, "garages", 0, []uint64{enc(1, 1, 100)})
		writeIdx(t, mb, "garages.cars", 0, []uint64{enc(1, 1, 100)})

		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		make := makeLeafPvp(class, "countries", "garages.cars.make", "honda")
		raws := map[*propValuePair]*sroar.Bitmap{
			postcode: roaringset.NewBitmap(enc(1, 1, 100)),
			make:     roaringset.NewBitmap(enc(1, 1, 100)),
		}

		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build([]*propValuePair{postcode, make})
		exec := newRecExecutor(raws, mb, ops, concurrency.SROAR_MERGE)
		_, _, err := exec.execute(cancelled(), plan)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("cancelled_via_split_path", func(t *testing.T) {
		// Two arr[N] indices on garages → SPLIT@"garages" with two branches.
		// The top-level execute() check fires before evalSplit's branch loop.
		mb := newIdxBucket(t)
		writeIdx(t, mb, "garages", 0, []uint64{enc(1, 1, 100)})
		writeIdx(t, mb, "garages", 1, []uint64{enc(1, 2, 100)})

		idx0 := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1 := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		city0 := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0)
		city1 := makeLeafPvpWithIdx(class, "countries", "garages.city", "munich", idx1)
		raws := map[*propValuePair]*sroar.Bitmap{
			city0: roaringset.NewBitmap(enc(1, 1, 100)),
			city1: roaringset.NewBitmap(enc(1, 2, 100)),
		}

		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build([]*propValuePair{city0, city1})
		exec := newRecExecutor(raws, mb, ops, concurrency.SROAR_MERGE)
		_, _, err := exec.execute(cancelled(), plan)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("inner_check_runIdxLoopRecursive_returns_ctx_err", func(t *testing.T) {
		// Bypass execute() and invoke evalNode directly so the top-level
		// check is skipped. runIdxLoopRecursive's own start-of-function
		// ctxExpired guard must still fire — this protects long-running idx
		// iterations after a previous ctx check passed.
		mb := newIdxBucket(t)
		writeIdx(t, mb, "garages", 0, []uint64{enc(1, 1, 100)})
		writeIdx(t, mb, "garages.cars", 0, []uint64{enc(1, 1, 100)})

		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		make := makeLeafPvp(class, "countries", "garages.cars.make", "honda")
		raws := map[*propValuePair]*sroar.Bitmap{
			postcode: roaringset.NewBitmap(enc(1, 1, 100)),
			make:     roaringset.NewBitmap(enc(1, 1, 100)),
		}

		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build([]*propValuePair{postcode, make})
		exec := newRecExecutor(raws, mb, ops, concurrency.SROAR_MERGE)
		_, _, err := exec.evalNode(cancelled(), plan, nil)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("inner_check_evalSplit_returns_ctx_err", func(t *testing.T) {
		// Bypass execute() and invoke evalNode directly. evalSplit's
		// per-branch ctxExpired guard must fire — this protects multi-branch
		// dispatch when each branch incurs a metaBucket read.
		mb := newIdxBucket(t)
		writeIdx(t, mb, "garages", 0, []uint64{enc(1, 1, 100)})
		writeIdx(t, mb, "garages", 1, []uint64{enc(1, 2, 100)})

		idx0 := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1 := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		city0 := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0)
		city1 := makeLeafPvpWithIdx(class, "countries", "garages.city", "munich", idx1)
		raws := map[*propValuePair]*sroar.Bitmap{
			city0: roaringset.NewBitmap(enc(1, 1, 100)),
			city1: roaringset.NewBitmap(enc(1, 2, 100)),
		}

		ops := newLifecycleOps(t)
		plan := newRecPlanBuilder(props).build([]*propValuePair{city0, city1})
		exec := newRecExecutor(raws, mb, ops, concurrency.SROAR_MERGE)
		_, _, err := exec.evalNode(cancelled(), plan, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}
