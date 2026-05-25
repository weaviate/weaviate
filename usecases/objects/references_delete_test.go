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

// TestRemoveReference pins the match semantics for the two modes:
//
//   - NS-enabled: structural match on short-class + TargetID. Either side
//     empty class falls back to a TargetID-only match (legacy classless
//     beacon contract). Admin-submitted qualified beacons must match
//     stored short ones.
//   - NS-disabled: byte-exact compare on beacon URI. No structural
//     softening — preserves pre-namespacing behavior.
//
// Legacy classless-match (empty class on either side matches across all
// classes for that TargetID) is intentional behavior the NS code path
// inherited from the pre-namespacing on-disk contract; the
// "classless-supplied wildcard" subtest pins it explicitly.
func TestRemoveReference(t *testing.T) {
	const (
		propName = "hasAnimals"
		targetID = strfmt.UUID("11111111-1111-1111-1111-111111111111")
		otherID  = strfmt.UUID("22222222-2222-2222-2222-222222222222")
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
		name              string
		namespacesEnabled bool
		stored            []strfmt.URI
		removeBeacon      strfmt.URI // supplied by the caller
		wantOk            bool
		wantRemaining     []strfmt.URI
	}{
		// --- NS-enabled: structural match ---
		{
			name:              "NS: short stored, short supplied — match",
			namespacesEnabled: true,
			stored:            []strfmt.URI{beaconShort},
			removeBeacon:      beaconShort,
			wantOk:            true,
			wantRemaining:     []strfmt.URI{},
		},
		{
			name:              "NS: short stored, qualified supplied — match (admin foot-gun)",
			namespacesEnabled: true,
			stored:            []strfmt.URI{beaconShort},
			removeBeacon:      beaconQualified,
			wantOk:            true,
			wantRemaining:     []strfmt.URI{},
		},
		{
			name:              "NS: classless supplied is a TargetID wildcard across stored classes",
			namespacesEnabled: true,
			stored:            []strfmt.URI{beaconShort, beaconOtherClass},
			removeBeacon:      beaconClassless,
			wantOk:            true,
			wantRemaining:     []strfmt.URI{}, // BOTH removed — pins legacy contract
		},
		{
			name:              "NS: different UUID — no match",
			namespacesEnabled: true,
			stored:            []strfmt.URI{beaconOtherUUID},
			removeBeacon:      beaconShort,
			wantOk:            false,
			wantRemaining:     []strfmt.URI{beaconOtherUUID},
		},
		{
			name:              "NS: different class same UUID — no match",
			namespacesEnabled: true,
			stored:            []strfmt.URI{beaconOtherClass},
			removeBeacon:      beaconShort,
			wantOk:            false,
			wantRemaining:     []strfmt.URI{beaconOtherClass},
		},
		{
			name:              "NS: foreign-NS-prefixed supplied still matches short stored by short-class equality",
			namespacesEnabled: true,
			stored:            []strfmt.URI{beaconShort},
			removeBeacon:      beaconForeignNS,
			wantOk:            true, // intentional: handler's QualifyRefTarget gate is responsible for rejecting cross-NS BEFORE removeReference is called. removeReference itself only does the structural compare.
			wantRemaining:     []strfmt.URI{},
		},
		{
			name:              "NS: malformed stored beacon is skipped, others still considered",
			namespacesEnabled: true,
			stored:            []strfmt.URI{strfmt.URI("not-a-beacon"), beaconShort},
			removeBeacon:      beaconShort,
			wantOk:            true,
			wantRemaining:     []strfmt.URI{strfmt.URI("not-a-beacon")},
		},

		// --- NS-disabled: byte-exact ---
		{
			name:              "non-NS: exact match",
			namespacesEnabled: false,
			stored:            []strfmt.URI{beaconShort},
			removeBeacon:      beaconShort,
			wantOk:            true,
			wantRemaining:     []strfmt.URI{},
		},
		{
			name:              "non-NS: short stored, qualified supplied — NO match (byte-exact)",
			namespacesEnabled: false,
			stored:            []strfmt.URI{beaconShort},
			removeBeacon:      beaconQualified,
			wantOk:            false,
			wantRemaining:     []strfmt.URI{beaconShort},
		},
		{
			name:              "non-NS: classless supplied does NOT wildcard across stored short class",
			namespacesEnabled: false,
			stored:            []strfmt.URI{beaconShort},
			removeBeacon:      beaconClassless,
			wantOk:            false,
			wantRemaining:     []strfmt.URI{beaconShort},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := makeObj(tc.stored...)
			logger, _ := test.NewNullLogger()
			remove := parsed(t, tc.removeBeacon)
			ok, errmsg := removeReference(obj, propName, remove, tc.removeBeacon, tc.namespacesEnabled, logger)
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

// TestRemoveReference_NonRefProperty pins the errmsg path: a property that
// exists on the object but isn't a MultipleRef returns an error string
// (caller maps it to 500) and leaves the property untouched. The %T must
// report the type actually stored — formatting against the failed
// type-assertion result would always print "models.MultipleRef" and hide
// what was really there from operators.
func TestRemoveReference_NonRefProperty(t *testing.T) {
	obj := &models.Object{
		Properties: map[string]interface{}{"bogus": "not-a-ref"},
	}
	logger, _ := test.NewNullLogger()
	r, err := crossref.Parse("weaviate://localhost/Animal/11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)
	ok, errmsg := removeReference(obj, "bogus", r, strfmt.URI(""), true, logger)
	assert.False(t, ok)
	assert.Contains(t, errmsg, "not a valid cross-reference")
	assert.Contains(t, errmsg, "string",
		"errmsg must name the actual stored type, not the failed assertion's zero value")
	assert.NotContains(t, errmsg, "MultipleRef",
		"errmsg must not lie about the stored type by reporting the expected one")
}

// TestRemoveReference_MissingProperty: no-op when the property is absent.
func TestRemoveReference_MissingProperty(t *testing.T) {
	obj := &models.Object{Properties: map[string]interface{}{}}
	logger, _ := test.NewNullLogger()
	r, err := crossref.Parse("weaviate://localhost/Animal/11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)
	ok, errmsg := removeReference(obj, "missing", r, strfmt.URI(""), true, logger)
	assert.False(t, ok)
	assert.Empty(t, errmsg)
}
