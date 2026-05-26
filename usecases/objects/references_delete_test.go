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
//nolint:dupl // legacy + structural paths intentionally exercise the same
// edge cases (missing/non-ref property) against each function.

package objects

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
)

// TestRemoveReferenceStructural pins the NS removal contract:
//
//   - (short_class, TargetID) match between supplied (qualified) and
//     stored (short) beacons.
//   - Classless supplied beacon: no-op (safety net; handler returns 400).
//   - Foreign-NS supplied beacon: no-op (safety net; handler returns 422).
//   - Classless STORED beacon: no-op (legacy wildcard removed — same-UUID
//     refs to unrelated classes are no longer collateral).
//   - Malformed stored beacon: skipped with a Debug log; siblings still
//     evaluated.
//
// Production callers (DeleteObjectReference) submit a qualified source
// class (after resolveNS) and a qualified supplied beacon (after
// QualifyRefTarget). The cases below mirror that input shape.
func TestRemoveReferenceStructural(t *testing.T) {
	const (
		propName    = "hasAnimals"
		sourceClass = "customer1:Zoo"
		targetID    = strfmt.UUID("11111111-1111-1111-1111-111111111111")
		otherID     = strfmt.UUID("22222222-2222-2222-2222-222222222222")
	)

	beaconShort := strfmt.URI("weaviate://localhost/Animal/" + string(targetID))
	beaconQualified := strfmt.URI("weaviate://localhost/customer1:Animal/" + string(targetID))
	beaconClassless := strfmt.URI("weaviate://localhost/" + string(targetID))
	beaconOtherUUID := strfmt.URI("weaviate://localhost/Animal/" + string(otherID))
	beaconOtherClass := strfmt.URI("weaviate://localhost/Habitat/" + string(targetID))
	beaconForeignNS := strfmt.URI("weaviate://localhost/customer2:Animal/" + string(targetID))

	makeObj := func(beacons ...strfmt.URI) *models.Object {
		refs := models.MultipleRef{}
		for _, b := range beacons {
			refs = append(refs, &models.SingleRef{Beacon: b})
		}
		return &models.Object{
			ID:         "00000000-0000-0000-0000-000000000001",
			Properties: map[string]interface{}{propName: refs},
		}
	}
	parsed := func(t *testing.T, b strfmt.URI) *crossref.Ref {
		t.Helper()
		r, err := crossref.Parse(b.String())
		require.NoError(t, err)
		return r
	}

	cases := []struct {
		name          string
		stored        []strfmt.URI
		removeBeacon  strfmt.URI // production: already qualified by QualifyRefTarget
		wantOk        bool
		wantRemaining []strfmt.URI
	}{
		{
			name:          "qualified supplied matches short stored (production shape)",
			stored:        []strfmt.URI{beaconShort},
			removeBeacon:  beaconQualified,
			wantOk:        true,
			wantRemaining: []strfmt.URI{},
		},
		{
			name:          "classless supplied — no-op (safety net; handler returns 400)",
			stored:        []strfmt.URI{beaconShort, beaconOtherClass},
			removeBeacon:  beaconClassless,
			wantOk:        false,
			wantRemaining: []strfmt.URI{beaconShort, beaconOtherClass},
		},
		{
			name:          "foreign-NS supplied — no-op (safety net; handler returns 422)",
			stored:        []strfmt.URI{beaconShort},
			removeBeacon:  beaconForeignNS,
			wantOk:        false,
			wantRemaining: []strfmt.URI{beaconShort},
		},
		{
			name:          "different UUID — no match",
			stored:        []strfmt.URI{beaconOtherUUID},
			removeBeacon:  beaconQualified,
			wantOk:        false,
			wantRemaining: []strfmt.URI{beaconOtherUUID},
		},
		{
			name:          "different class same UUID — no match",
			stored:        []strfmt.URI{beaconOtherClass},
			removeBeacon:  beaconQualified,
			wantOk:        false,
			wantRemaining: []strfmt.URI{beaconOtherClass},
		},
		{
			name:          "legacy classless STORED beacon — no match (wildcard dropped)",
			stored:        []strfmt.URI{beaconClassless},
			removeBeacon:  beaconQualified,
			wantOk:        false,
			wantRemaining: []strfmt.URI{beaconClassless},
		},
		{
			name:          "malformed stored beacon is skipped, others still considered",
			stored:        []strfmt.URI{strfmt.URI("not-a-beacon"), beaconShort},
			removeBeacon:  beaconQualified,
			wantOk:        true,
			wantRemaining: []strfmt.URI{strfmt.URI("not-a-beacon")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := makeObj(tc.stored...)
			logger, _ := test.NewNullLogger()
			remove := parsed(t, tc.removeBeacon)
			ok, errmsg := removeReferenceStructural(obj, propName, sourceClass, remove, logger)
			require.Empty(t, errmsg)
			assert.Equal(t, tc.wantOk, ok, "ok return")

			got := obj.Properties.(map[string]interface{})[propName].(models.MultipleRef)
			gotBeacons := make([]strfmt.URI, len(got))
			for i, r := range got {
				gotBeacons[i] = r.Beacon
			}
			assert.Equal(t, tc.wantRemaining, gotBeacons, "remaining beacons")
		})
	}
}

// TestRemoveReferenceByteExact pins the non-NS removal contract: byte-exact
// compare on the beacon URI. No structural softening — preserves the
// pre-namespacing behavior.
func TestRemoveReferenceByteExact(t *testing.T) {
	const (
		propName = "hasAnimals"
		targetID = strfmt.UUID("11111111-1111-1111-1111-111111111111")
	)

	beaconShort := strfmt.URI("weaviate://localhost/Animal/" + string(targetID))
	beaconQualified := strfmt.URI("weaviate://localhost/customer1:Animal/" + string(targetID))
	beaconClassless := strfmt.URI("weaviate://localhost/" + string(targetID))

	makeObj := func(beacons ...strfmt.URI) *models.Object {
		refs := models.MultipleRef{}
		for _, b := range beacons {
			refs = append(refs, &models.SingleRef{Beacon: b})
		}
		return &models.Object{Properties: map[string]interface{}{propName: refs}}
	}

	cases := []struct {
		name          string
		stored        []strfmt.URI
		removeBeacon  strfmt.URI
		wantOk        bool
		wantRemaining []strfmt.URI
	}{
		{
			name:          "exact match",
			stored:        []strfmt.URI{beaconShort},
			removeBeacon:  beaconShort,
			wantOk:        true,
			wantRemaining: []strfmt.URI{},
		},
		{
			name:          "short stored, qualified supplied — NO match (byte-exact)",
			stored:        []strfmt.URI{beaconShort},
			removeBeacon:  beaconQualified,
			wantOk:        false,
			wantRemaining: []strfmt.URI{beaconShort},
		},
		{
			name:          "classless supplied does NOT wildcard across stored short class",
			stored:        []strfmt.URI{beaconShort},
			removeBeacon:  beaconClassless,
			wantOk:        false,
			wantRemaining: []strfmt.URI{beaconShort},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := makeObj(tc.stored...)
			ok, errmsg := removeReferenceByteExact(obj, propName, tc.removeBeacon)
			require.Empty(t, errmsg)
			assert.Equal(t, tc.wantOk, ok, "ok return")

			got := obj.Properties.(map[string]interface{})[propName].(models.MultipleRef)
			gotBeacons := make([]strfmt.URI, len(got))
			for i, r := range got {
				gotBeacons[i] = r.Beacon
			}
			assert.Equal(t, tc.wantRemaining, gotBeacons, "remaining beacons")
		})
	}
}

// TestRemoveReferenceStructural_NonRefProperty pins the errmsg path: a
// property that exists on the object but isn't a MultipleRef returns an
// error string (caller maps it to 500) and leaves the property untouched.
// The %T must report the type actually stored — formatting against the
// failed type-assertion result would always print "models.MultipleRef"
// and hide what was really there from operators.
func TestRemoveReferenceStructural_NonRefProperty(t *testing.T) {
	obj := &models.Object{
		Properties: map[string]interface{}{"bogus": "not-a-ref"},
	}
	logger, _ := test.NewNullLogger()
	r, err := crossref.Parse("weaviate://localhost/customer1:Animal/11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)
	ok, errmsg := removeReferenceStructural(obj, "bogus", "customer1:Zoo", r, logger)
	assert.False(t, ok)
	assert.Contains(t, errmsg, "not a valid cross-reference")
	assert.Contains(t, errmsg, "string",
		"errmsg must name the actual stored type, not the failed assertion's zero value")
	assert.NotContains(t, errmsg, "MultipleRef",
		"errmsg must not lie about the stored type by reporting the expected one")
}

// TestRemoveReferenceByteExact_NonRefProperty: same errmsg path for the
// non-NS function.
func TestRemoveReferenceByteExact_NonRefProperty(t *testing.T) {
	obj := &models.Object{
		Properties: map[string]interface{}{"bogus": "not-a-ref"},
	}
	ok, errmsg := removeReferenceByteExact(obj, "bogus",
		strfmt.URI("weaviate://localhost/Animal/11111111-1111-1111-1111-111111111111"))
	assert.False(t, ok)
	assert.Contains(t, errmsg, "not a valid cross-reference")
	assert.Contains(t, errmsg, "string")
}

// TestRemoveReferenceStructural_MissingProperty: no-op when the property
// is absent.
func TestRemoveReferenceStructural_MissingProperty(t *testing.T) {
	obj := &models.Object{Properties: map[string]interface{}{}}
	logger, _ := test.NewNullLogger()
	r, err := crossref.Parse("weaviate://localhost/customer1:Animal/11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)
	ok, errmsg := removeReferenceStructural(obj, "missing", "customer1:Zoo", r, logger)
	assert.False(t, ok)
	assert.Empty(t, errmsg)
}
