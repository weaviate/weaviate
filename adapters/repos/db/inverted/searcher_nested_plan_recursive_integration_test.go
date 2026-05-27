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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entcfg "github.com/weaviate/weaviate/entities/config"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/models"
)

// Nested filtering is preview-gated. Enable the gate at package init via
// the env var; individual tests that want the off state use t.Setenv.
func init() { os.Setenv(entcfg.EnvNestedFilteringPreview, "true") }

// rootNestedProps returns the NestedProperties slice for the named root
// property in filterExamplesClass(). Used by recursive plan structural tests
// so they share the schema shape with TestCorrelatedAndFilterExamplesIndexed.
func rootNestedProps(t *testing.T, class *models.Class, name string) []*models.NestedProperty {
	t.Helper()
	for _, p := range class.Properties {
		if p.Name == name {
			return p.NestedProperties
		}
	}
	require.Failf(t, "missing root prop", "filterExamplesClass has no %q", name)
	return nil
}

// TestRecPlanBuilderShape exercises the recursive plan builder against the
// same eight filter scenarios as TestCorrelatedAndFilterExamplesIndexed, but
// only checks the resulting tree shape (no bitmap execution). Each sub-test
// runs the builder on two reversed orderings to confirm shape stability.
func TestRecPlanBuilderShape(t *testing.T) {
	class := filterExamplesClass()
	props := rootNestedProps(t, class, "countries")
	builder := newRecPlanBuilder(props)

	assertShape := func(t *testing.T, want string, forward, reversed *propValuePair) {
		t.Helper()
		assert.Equal(t, want, describePlan(builder.build(forward.children)), "forward order")
		assert.Equal(t, want, describePlan(builder.build(reversed.children)), "reversed order")
	}

	t.Run("F1_garages[0].city_AND_garages[1].postcode_AND_garages[1].cars.{make,model}", func(t *testing.T) {
		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0g)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1g)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1g)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx1g)

		want := `SPLIT lcaPath="garages"
  branches:
    index=0
      GROUP lcaPath="garages"
        here=[garages.city]
        subs=[]
    index=1
      GROUP lcaPath="garages"
        here=[garages.postcode]
        subs:
          GROUP lcaPath="garages.cars"
            here=[garages.cars.make, garages.cars.model]
            subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel),
			makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city))
	})

	t.Run("F2_garages[0].city_AND_garages[1].postcode_AND_garages[2].cars.{make,model}", func(t *testing.T) {
		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		idx2g := filnested.ArrayIndex{RelPath: "garages", Index: 2}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0g)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1g)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx2g)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx2g)

		want := `SPLIT lcaPath="garages"
  branches:
    index=0
      GROUP lcaPath="garages"
        here=[garages.city]
        subs=[]
    index=1
      GROUP lcaPath="garages"
        here=[garages.postcode]
        subs=[]
    index=2
      GROUP lcaPath="garages.cars"
        here=[garages.cars.make, garages.cars.model]
        subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel),
			makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city))
	})

	t.Run("F3_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[1].garages.cars.{make,model}", func(t *testing.T) {
		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx1c)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx1c)

		want := `SPLIT lcaPath=""
  branches:
    index=0
      GROUP lcaPath="garages"
        here=[garages.city]
        subs=[]
    index=1
      GROUP lcaPath="garages"
        here=[garages.postcode]
        subs:
          GROUP lcaPath="garages.cars"
            here=[garages.cars.make, garages.cars.model]
            subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel),
			makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city))
	})

	t.Run("F4_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[2].garages.cars.{make,model}", func(t *testing.T) {
		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}
		idx2c := filnested.ArrayIndex{RelPath: "", Index: 2}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx2c)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx2c)

		want := `SPLIT lcaPath=""
  branches:
    index=0
      GROUP lcaPath="garages"
        here=[garages.city]
        subs=[]
    index=1
      GROUP lcaPath="garages"
        here=[garages.postcode]
        subs=[]
    index=2
      GROUP lcaPath="garages.cars"
        here=[garages.cars.make, garages.cars.model]
        subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel),
			makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city))
	})

	t.Run("F5_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[2].garages[3].cars.{make,model}", func(t *testing.T) {
		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}
		idx2c := filnested.ArrayIndex{RelPath: "", Index: 2}
		idx3g := filnested.ArrayIndex{RelPath: "garages", Index: 3}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)
		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx2c, idx3g)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx2c, idx3g)

		want := `SPLIT lcaPath=""
  branches:
    index=0
      GROUP lcaPath="garages"
        here=[garages.city]
        subs=[]
    index=1
      GROUP lcaPath="garages"
        here=[garages.postcode]
        subs=[]
    index=2
      SPLIT lcaPath="garages"
        branches:
          index=3
            GROUP lcaPath="garages.cars"
              here=[garages.cars.make, garages.cars.model]
              subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel),
			makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city))
	})

	t.Run("F6_countries.garages.city_AND_postcode_AND_cars.{make,model}", func(t *testing.T) {
		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		carsMake := makeLeafPvp(class, "countries", "garages.cars.make", "honda")
		carsModel := makeLeafPvp(class, "countries", "garages.cars.model", "civic")

		want := `GROUP lcaPath="garages"
  here=[garages.city, garages.postcode]
  subs:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make, garages.cars.model]
      subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode, carsMake, carsModel),
			makeCorrelatedPvp(class, "countries", carsModel, carsMake, postcode, city))
	})

	t.Run("F7_countries.garages.city_AND_postcode_AND_cars.accessories.type_AND_cars.tags", func(t *testing.T) {
		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		accType := makeLeafPvp(class, "countries", "garages.cars.accessories.type", "spolier")
		tags := makeLeafPvp(class, "countries", "garages.cars.tags", "electric")

		want := `GROUP lcaPath="garages"
  here=[garages.city, garages.postcode]
  subs:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.tags]
      subs:
        GROUP lcaPath="garages.cars.accessories"
          here=[garages.cars.accessories.type]
          subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode, accType, tags),
			makeCorrelatedPvp(class, "countries", tags, accType, postcode, city))
	})

	t.Run("F8_countries.garages.city_AND_postcode_AND_cars.accessories.type_AND_cars.tires.width", func(t *testing.T) {
		city := makeLeafPvp(class, "countries", "garages.city", "berlin")
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")
		accType := makeLeafPvp(class, "countries", "garages.cars.accessories.type", "spolier")
		tireWidth := makeLeafPvp(class, "countries", "garages.cars.tires.width", "225")

		want := `GROUP lcaPath="garages"
  here=[garages.city, garages.postcode]
  subs:
    GROUP lcaPath="garages.cars"
      here=[]
      subs:
        GROUP lcaPath="garages.cars.accessories"
          here=[garages.cars.accessories.type]
          subs=[]
        GROUP lcaPath="garages.cars.tires"
          here=[garages.cars.tires.width]
          subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode, accType, tireWidth),
			makeCorrelatedPvp(class, "countries", tireWidth, accType, postcode, city))
	})

	// F13 locks in the ≥2-bucket pure SPLIT shape at intermediate scope, wrapped
	// in a GROUP at the SPLIT's parent scope. The wrapping GROUP is what
	// enforces same-element semantics at the LCA above the conflict — its
	// per-element loop (runIdxLoopRecursive) iterates over each garages[K] and
	// evaluates the SPLIT inside, so cars[0] and cars[1] must land in the same
	// garage. Without the wrapping GROUP, the SPLIT combiner ANDs at rootDoc
	// only, which would lose the same-garage requirement for deeper schemas.
	t.Run("F13_garages.cars[0].make_AND_garages.cars[1].model_pure_split_intermediate", func(t *testing.T) {
		idx0cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 0}
		idx1cars := filnested.ArrayIndex{RelPath: "garages.cars", Index: 1}

		carsMake := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx0cars)
		carsModel := makeLeafPvpWithIdx(class, "countries", "garages.cars.model", "civic", idx1cars)

		want := `GROUP lcaPath="garages"
  here=[]
  subs:
    SPLIT lcaPath="garages.cars"
      branches:
        index=0
          GROUP lcaPath="garages.cars"
            here=[garages.cars.make]
            subs=[]
        index=1
          GROUP lcaPath="garages.cars"
            here=[garages.cars.model]
            subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", carsMake, carsModel),
			makeCorrelatedPvp(class, "countries", carsModel, carsMake))
	})

	// F15 locks in the ≥2-bucket pure SPLIT shape at root scope (lcaPath="").
	// Like F13 this shape is unreachable through production dispatch — conflicting
	// root indices are partitioned into separate compatibility groups — but the
	// planner must still produce the correct multi-branch tree when invoked
	// directly. The executor's matching combiner (andBranchesAtDocID) is covered
	// by F15 in TestRecExecutorFilterExamples.
	t.Run("F15_countries[0].garages.city_AND_countries[1].garages.postcode_pure_split_root", func(t *testing.T) {
		idx0c := filnested.ArrayIndex{RelPath: "", Index: 0}
		idx1c := filnested.ArrayIndex{RelPath: "", Index: 1}

		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0c)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1c)

		want := `SPLIT lcaPath=""
  branches:
    index=0
      GROUP lcaPath="garages"
        here=[garages.city]
        subs=[]
    index=1
      GROUP lcaPath="garages"
        here=[garages.postcode]
        subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode),
			makeCorrelatedPvp(class, "countries", postcode, city))
	})

	// F16 locks in the multi-branch SPLIT@intermediate shape where each branch
	// carries a non-flat GROUP (here + sub) — the branch dispatches to evalNode
	// → runIdxLoopRecursive instead of canUseRawAndAll's flat path. Like F13/F15
	// this configuration is unreachable through dispatch (conflicting "garages"
	// indices would split into separate compatibility groups). The executor's
	// matching path is covered by F16 in TestRecExecutorFilterExamples.
	t.Run("F16_garages[0]_AND_garages[1]_each_with_make_AND_tires.width_split_with_subs", func(t *testing.T) {
		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}

		makeHonda := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "honda", idx0g)
		width205 := makeLeafPvpWithIdx(class, "countries", "garages.cars.tires.width", "205", idx0g)
		makeFerrari := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "ferrari", idx1g)
		width225 := makeLeafPvpWithIdx(class, "countries", "garages.cars.tires.width", "225", idx1g)

		want := `SPLIT lcaPath="garages"
  branches:
    index=0
      GROUP lcaPath="garages.cars"
        here=[garages.cars.make]
        subs:
          GROUP lcaPath="garages.cars.tires"
            here=[garages.cars.tires.width]
            subs=[]
    index=1
      GROUP lcaPath="garages.cars"
        here=[garages.cars.make]
        subs:
          GROUP lcaPath="garages.cars.tires"
            here=[garages.cars.tires.width]
            subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", makeHonda, width205, makeFerrari, width225),
			makeCorrelatedPvp(class, "countries", width225, makeFerrari, width205, makeHonda))
	})
}

// TestRecPlanBuilderBucketingRules locks in the bucketing decision rules of
// recPlanBuilder.buildPlan at a single scope. The F-suite exercises these
// implicitly; this matrix isolates each rule with the smallest filter that
// triggers it so a regression in the planner is easy to localize.
//
// Rules under test (per buildPlan(items, scope)):
//
//	B1: 0 constrained at scope         → no SPLIT, just buildGroup at scope
//	B2: 1 constrained, 0 unconstrained → 1-branch SPLIT, branch contains the
//	                                     single constrained item
//	B3: 1 constrained, 1 unconstrained → 1-branch SPLIT, branch's GROUP merges
//	                                     unconstrained as additional here items
//	                                     when they terminate at the scope LCA
//	B4: 1 constrained, 1 unconstrained → 1-branch SPLIT, branch's GROUP places
//	                                     unconstrained into a deeper sub when
//	                                     they continue past the scope LCA
//	B5: ≥2 constrained, 0 unconstrained → multi-branch SPLIT, one branch per
//	                                      constrained index
//	B6: ≥2 constrained, ≥1 unconstrained → multi-branch SPLIT; unconstrained
//	                                       items are silently dropped (per the
//	                                       TODO in buildPlan — slated to be
//	                                       rejected by validation in
//	                                       entities/filters/filters_validator.go).
//	                                       Locking in the current behaviour
//	                                       makes the validation hand-off
//	                                       explicit when it lands.
func TestRecPlanBuilderBucketingRules(t *testing.T) {
	class := filterExamplesClass()
	props := rootNestedProps(t, class, "countries")
	builder := newRecPlanBuilder(props)

	assertShape := func(t *testing.T, want string, forward, reversed *propValuePair) {
		t.Helper()
		assert.Equal(t, want, describePlan(builder.build(forward.children)), "forward order")
		assert.Equal(t, want, describePlan(builder.build(reversed.children)), "reversed order")
	}

	t.Run("B1_zero_constrained_no_split", func(t *testing.T) {
		carsMake := makeLeafPvp(class, "countries", "garages.cars.make", "honda")
		carsModel := makeLeafPvp(class, "countries", "garages.cars.model", "civic")

		// Both items share LCA "garages.cars", so buildGroup collapses straight
		// to that scope without an intermediate empty GROUP@"garages".
		want := `GROUP lcaPath="garages.cars"
  here=[garages.cars.make, garages.cars.model]
  subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", carsMake, carsModel),
			makeCorrelatedPvp(class, "countries", carsModel, carsMake))
	})

	t.Run("B2_one_constrained_zero_unconstrained_single_branch", func(t *testing.T) {
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx1g)

		want := `SPLIT lcaPath="garages"
  branches:
    index=1
      GROUP lcaPath="garages"
        here=[garages.city]
        subs=[]`

		// Single condition — only one ordering exists, but assertShape still
		// runs the same input twice which is a harmless no-op.
		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city),
			makeCorrelatedPvp(class, "countries", city))
	})

	t.Run("B3_one_constrained_one_unconstrained_merged_into_here", func(t *testing.T) {
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx1g)
		postcode := makeLeafPvp(class, "countries", "garages.postcode", "12345")

		want := `SPLIT lcaPath="garages"
  branches:
    index=1
      GROUP lcaPath="garages"
        here=[garages.city, garages.postcode]
        subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode),
			makeCorrelatedPvp(class, "countries", postcode, city))
	})

	t.Run("B4_one_constrained_one_unconstrained_merged_into_sub", func(t *testing.T) {
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx1g)
		carsMake := makeLeafPvp(class, "countries", "garages.cars.make", "honda")

		want := `SPLIT lcaPath="garages"
  branches:
    index=1
      GROUP lcaPath="garages"
        here=[garages.city]
        subs:
          GROUP lcaPath="garages.cars"
            here=[garages.cars.make]
            subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, carsMake),
			makeCorrelatedPvp(class, "countries", carsMake, city))
	})

	t.Run("B5_two_constrained_zero_unconstrained_multi_branch", func(t *testing.T) {
		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0g)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1g)

		want := `SPLIT lcaPath="garages"
  branches:
    index=0
      GROUP lcaPath="garages"
        here=[garages.city]
        subs=[]
    index=1
      GROUP lcaPath="garages"
        here=[garages.postcode]
        subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode),
			makeCorrelatedPvp(class, "countries", postcode, city))
	})

	t.Run("B6_two_constrained_one_unconstrained_unconstrained_dropped", func(t *testing.T) {
		idx0g := filnested.ArrayIndex{RelPath: "garages", Index: 0}
		idx1g := filnested.ArrayIndex{RelPath: "garages", Index: 1}
		city := makeLeafPvpWithIdx(class, "countries", "garages.city", "berlin", idx0g)
		postcode := makeLeafPvpWithIdx(class, "countries", "garages.postcode", "12345", idx1g)
		carsMake := makeLeafPvp(class, "countries", "garages.cars.make", "honda")

		// garages.cars.make is unconstrained at "garages" and is silently
		// dropped by the multi-branch path. When validation lands, this filter
		// will be rejected upstream and this test should be updated to assert
		// the validation error instead.
		want := `SPLIT lcaPath="garages"
  branches:
    index=0
      GROUP lcaPath="garages"
        here=[garages.city]
        subs=[]
    index=1
      GROUP lcaPath="garages"
        here=[garages.postcode]
        subs=[]`

		assertShape(t, want,
			makeCorrelatedPvp(class, "countries", city, postcode, carsMake),
			makeCorrelatedPvp(class, "countries", carsMake, postcode, city))
	})
}

// TestRecPlanBuilderOrShape covers the recOrNode plan-shape: OR children
// are planned at their own natural scope and wrapped under an OR node
// whose lcaPath is the deepest common ancestor of children's lcaPaths.
//
// These cases aren't produced by the planner today (step 5 dispatch
// refactor will start passing OR/NOT into the planner). The tests
// exercise the planner directly with hand-constructed OR propValuePairs.
func TestRecPlanBuilderOrShape(t *testing.T) {
	class := filterExamplesClass()
	props := rootNestedProps(t, class, "countries")
	builder := newRecPlanBuilder(props)

	t.Run("OR_of_two_leaves_at_same_LCA_under_cars", func(t *testing.T) {
		// OR(make=tesla, model=civic) — both at garages.cars LCA.
		orPv := makeOrPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.make", "tesla"),
			makeLeafPvp(class, "countries", "garages.cars.model", "civic"),
		)
		want := `OR lcaPath="garages.cars"
  children:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make]
      subs=[]
    GROUP lcaPath="garages.cars"
      here=[garages.cars.model]
      subs=[]`
		assert.Equal(t, want, describePlan(builder.build([]*propValuePair{orPv})))
	})

	t.Run("OR_of_leaves_at_different_LCAs_picks_common_ancestor", func(t *testing.T) {
		// OR(garages.city=berlin, garages.cars.make=tesla) — operands at
		// different LCAs (garages vs garages.cars). Common ancestor: garages.
		orPv := makeOrPvp(class,
			makeLeafPvp(class, "countries", "garages.city", "berlin"),
			makeLeafPvp(class, "countries", "garages.cars.make", "tesla"),
		)
		want := `OR lcaPath="garages"
  children:
    GROUP lcaPath="garages"
      here=[garages.city]
      subs=[]
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make]
      subs=[]`
		assert.Equal(t, want, describePlan(builder.build([]*propValuePair{orPv})))
	})

	t.Run("OR_inside_correlated_AND_alongside_leaf", func(t *testing.T) {
		// make=tesla AND (color=red OR model=civic). All at garages.cars.
		// The OR's natural LCA matches the leaf's natural LCA, so the OR is
		// pushed into the garages.cars subgroup as a sibling of the make
		// leaf. Same-cars-element correlation now holds across the AND.
		//
		// The outer GROUP at "garages" is kept by needsWrappingGroup —
		// the cars group has an OR sub, which forces runIdxLoopRecursive
		// at cars[], and that idx loop needs parentScope from the
		// enclosing garages[] iteration to disambiguate same-K-different-
		// garage cars (otherwise tesla in garages[0].cars[0] and a sibling
		// non-tesla in garages[1].cars[0] would mix at K=0).
		orPv := makeOrPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.color", "red"),
			makeLeafPvp(class, "countries", "garages.cars.model", "civic"),
		)
		want := `GROUP lcaPath="garages"
  here=[]
  subs:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make]
      subs:
        OR lcaPath="garages.cars"
          children:
            GROUP lcaPath="garages.cars"
              here=[garages.cars.color]
              subs=[]
            GROUP lcaPath="garages.cars"
              here=[garages.cars.model]
              subs=[]`
		assert.Equal(t, want, describePlan(builder.build(
			[]*propValuePair{
				makeLeafPvp(class, "countries", "garages.cars.make", "tesla"),
				orPv,
			},
		)))
	})

	t.Run("nested_OR_of_OR_preserved_not_flattened", func(t *testing.T) {
		// OR(make=tesla, OR(model=civic, color=red)) — nested as written.
		// Flattening is a possible later optimization; Step 3 keeps the
		// structure as the user wrote it.
		innerOr := makeOrPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.model", "civic"),
			makeLeafPvp(class, "countries", "garages.cars.color", "red"),
		)
		outerOr := makeOrPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.make", "tesla"),
			innerOr,
		)
		want := `OR lcaPath="garages.cars"
  children:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make]
      subs=[]
    OR lcaPath="garages.cars"
      children:
        GROUP lcaPath="garages.cars"
          here=[garages.cars.model]
          subs=[]
        GROUP lcaPath="garages.cars"
          here=[garages.cars.color]
          subs=[]`
		assert.Equal(t, want, describePlan(builder.build([]*propValuePair{outerOr})))
	})

	t.Run("OR_of_leaves_at_different_depths_through_garages_chain", func(t *testing.T) {
		// OR(tires.width=205, accessories.type=spoiler) — both deeper than
		// garages.cars; common ancestor is garages.cars.
		orPv := makeOrPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.tires.width", "205"),
			makeLeafPvp(class, "countries", "garages.cars.accessories.type", "spoiler"),
		)
		want := `OR lcaPath="garages.cars"
  children:
    GROUP lcaPath="garages.cars.tires"
      here=[garages.cars.tires.width]
      subs=[]
    GROUP lcaPath="garages.cars.accessories"
      here=[garages.cars.accessories.type]
      subs=[]`
		assert.Equal(t, want, describePlan(builder.build([]*propValuePair{orPv})))
	})
}

// TestRecPlanBuilderNotShape covers the recNotNode plan-shape: the
// operand is planned recursively at its own scope and wrapped in a NOT
// node whose lcaPath is the operand's natural LCA. arr[N] pins on a
// leaf operand are lifted into the NOT node for universe restriction.
func TestRecPlanBuilderNotShape(t *testing.T) {
	class := filterExamplesClass()
	props := rootNestedProps(t, class, "countries")
	builder := newRecPlanBuilder(props)

	t.Run("NOT_of_leaf", func(t *testing.T) {
		notPv := makeNotPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.make", "tesla"),
		)
		want := `NOT lcaPath="garages.cars" pins=[]
  operand:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make]
      subs=[]`
		assert.Equal(t, want, describePlan(builder.build([]*propValuePair{notPv})))
	})

	t.Run("NOT_inside_correlated_AND_alongside_leaf", func(t *testing.T) {
		// make=tesla AND NOT tires.width=205. The NOT's natural LCA is
		// garages.cars.tires (deeper than the leaf's garages.cars). Both
		// items pivot through the garages.cars subgroup; the NOT is placed
		// as a sub of garages.cars, planned at scope garages.cars so its
		// operand's deeper sub appears under garages.cars.tires.
		//
		// The outer GROUP at "garages" is kept by needsWrappingGroup —
		// the cars group has a NOT sub, which forces
		// runIdxLoopRecursive at cars[], and that idx loop needs
		// parentScope from the enclosing garages[] iteration to
		// disambiguate same-K-different-garage cars.
		notPv := makeNotPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.tires.width", "205"),
		)
		want := `GROUP lcaPath="garages"
  here=[]
  subs:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make]
      subs:
        NOT lcaPath="garages.cars.tires" pins=[]
          operand:
            GROUP lcaPath="garages.cars.tires"
              here=[garages.cars.tires.width]
              subs=[]`
		assert.Equal(t, want, describePlan(builder.build(
			[]*propValuePair{
				makeLeafPvp(class, "countries", "garages.cars.make", "tesla"),
				notPv,
			},
		)))
	})

	t.Run("NOT_of_leaf_with_root_arrN_pin", func(t *testing.T) {
		// NOT countries[1].garages.cars.make=tesla — pin at root scope.
		// The NOT's natural LCA is the operand's LCA (garages.cars), so
		// NOT plants at lcaPath="garages.cars" and the operand is planned
		// at scope garages.cars (where the root pin doesn't apply, so no
		// SPLIT around the operand). The pin is lifted onto the NOT node
		// and applied at evaluation time as universe ∩ _idx."[1]" — same
		// result as the previous SPLIT-wrapped-operand shape but with a
		// flatter plan tree.
		pin := filnested.ArrayIndex{RelPath: "", Index: 1}
		operand := makeLeafPvpWithIdx(class, "countries", "garages.cars.make", "tesla", pin)
		notPv := makeNotPvp(class, operand)
		want := `NOT lcaPath="garages.cars" pins=[[1]]
  operand:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make]
      subs=[]`
		assert.Equal(t, want, describePlan(builder.build([]*propValuePair{notPv})))
	})

	t.Run("NOT_of_leaf_with_intermediate_arrN_pin", func(t *testing.T) {
		// NOT cars.tires[1].width=205 — pin at "tires" relpath (not lifted
		// to root). Verifies multi-level pin propagation.
		pin := filnested.ArrayIndex{RelPath: "garages.cars.tires", Index: 1}
		operand := makeLeafPvpWithIdx(class, "countries", "garages.cars.tires.width", "205", pin)
		notPv := makeNotPvp(class, operand)
		want := `NOT lcaPath="garages.cars.tires" pins=[garages.cars.tires[1]]
  operand:
    SPLIT lcaPath="garages.cars.tires"
      branches:
        index=1
          GROUP lcaPath="garages.cars.tires"
            here=[garages.cars.tires.width]
            subs=[]`
		assert.Equal(t, want, describePlan(builder.build([]*propValuePair{notPv})))
	})

	// NOTE: NOT of a nested correlated AND is intentionally not tested here.
	// The planner treats the inner correlated AND as a single opaque item
	// (using its first child's path for placement), which is the existing
	// behavior for compound items and is outside Step 3's scope. NOT-of-OR
	// (covered by TestRecPlanBuilderOrNotMixed/OR_inside_NOT) exercises the
	// compound-operand path that Step 3 does extend.
}

// TestRecPlanBuilderOrNotMixed exercises filters with OR and NOT
// combined inside correlated AND, including NOT inside OR and OR inside
// NOT. Confirms recursive planning composes the new node types
// correctly without unintended interactions with the AND-side dispatch.
func TestRecPlanBuilderOrNotMixed(t *testing.T) {
	class := filterExamplesClass()
	props := rootNestedProps(t, class, "countries")
	builder := newRecPlanBuilder(props)

	t.Run("AND_with_leaf_OR_NOT_at_same_LCA", func(t *testing.T) {
		// make=tesla AND (color=red OR model=civic) AND NOT tires.width=205.
		// OR's natural LCA is "garages.cars" (deeper than root "") so it is
		// pushed into the garages.cars subgroup alongside the leaf make
		// condition. NOT's natural LCA is "garages.cars.tires", also pushed
		// down through the same subgroup chain, ending up as a sub of the
		// garages.cars group. The outer GROUP at "garages" wraps because the
		// deeper garages.cars carries multi-subs (needs outer scope to
		// disambiguate parents per needsWrappingGroup).
		orPv := makeOrPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.color", "red"),
			makeLeafPvp(class, "countries", "garages.cars.model", "civic"),
		)
		notPv := makeNotPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.tires.width", "205"),
		)
		want := `GROUP lcaPath="garages"
  here=[]
  subs:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make]
      subs:
        NOT lcaPath="garages.cars.tires" pins=[]
          operand:
            GROUP lcaPath="garages.cars.tires"
              here=[garages.cars.tires.width]
              subs=[]
        OR lcaPath="garages.cars"
          children:
            GROUP lcaPath="garages.cars"
              here=[garages.cars.color]
              subs=[]
            GROUP lcaPath="garages.cars"
              here=[garages.cars.model]
              subs=[]`
		assert.Equal(t, want, describePlan(builder.build(
			[]*propValuePair{
				makeLeafPvp(class, "countries", "garages.cars.make", "tesla"),
				orPv,
				notPv,
			},
		)))
	})

	t.Run("NOT_inside_OR", func(t *testing.T) {
		// OR(make=tesla, NOT color=red)
		notPv := makeNotPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.color", "red"),
		)
		orPv := makeOrPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.make", "tesla"),
			notPv,
		)
		want := `OR lcaPath="garages.cars"
  children:
    GROUP lcaPath="garages.cars"
      here=[garages.cars.make]
      subs=[]
    NOT lcaPath="garages.cars" pins=[]
      operand:
        GROUP lcaPath="garages.cars"
          here=[garages.cars.color]
          subs=[]`
		assert.Equal(t, want, describePlan(builder.build([]*propValuePair{orPv})))
	})

	t.Run("OR_inside_NOT", func(t *testing.T) {
		// NOT (make=tesla OR color=red)
		orPv := makeOrPvp(class,
			makeLeafPvp(class, "countries", "garages.cars.make", "tesla"),
			makeLeafPvp(class, "countries", "garages.cars.color", "red"),
		)
		notPv := makeNotPvp(class, orPv)
		want := `NOT lcaPath="garages.cars" pins=[]
  operand:
    OR lcaPath="garages.cars"
      children:
        GROUP lcaPath="garages.cars"
          here=[garages.cars.make]
          subs=[]
        GROUP lcaPath="garages.cars"
          here=[garages.cars.color]
          subs=[]`
		assert.Equal(t, want, describePlan(builder.build([]*propValuePair{notPv})))
	})
}

// TestDeepestCommonLCA exercises the LCA-computing helper directly.
// Covers boundary cases that aren't easily exercised through the higher-
// level OR-plan tests.
func TestDeepestCommonLCA(t *testing.T) {
	cases := []struct {
		name  string
		paths []string
		want  string
	}{
		{name: "empty list returns empty", paths: nil, want: ""},
		{name: "single path returns itself", paths: []string{"cars.tires"}, want: "cars.tires"},
		{name: "two identical paths", paths: []string{"cars.tires", "cars.tires"}, want: "cars.tires"},
		{name: "one is prefix of other", paths: []string{"cars", "cars.tires"}, want: "cars"},
		{name: "sibling sub-arrays share parent", paths: []string{"cars.tires", "cars.accessories"}, want: "cars"},
		{name: "no common prefix returns empty", paths: []string{"cars.tires", "garages.city"}, want: ""},
		{name: "empty path forces empty common", paths: []string{"cars.tires", ""}, want: ""},
		{name: "three paths converge at deepest", paths: []string{"cars.tires.width", "cars.tires.brand", "cars.tires"}, want: "cars.tires"},
		{name: "three paths converge at parent", paths: []string{"cars.tires.width", "cars.accessories.type", "cars.model"}, want: "cars"},
		{name: "segment boundary not character prefix", paths: []string{"cars.tires", "cars.tirestore"}, want: "cars"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, deepestCommonLCA(tc.paths))
		})
	}
}
