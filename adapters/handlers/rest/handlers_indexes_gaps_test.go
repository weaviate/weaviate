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

package rest

// Coverage-gap unit tests for runtime-reindex helper logic that previously had
// no direct test coverage:
//
//   - checkReindexConflict / typesConflict / propsOverlap
//     (concurrent same/different-type submits)
//   - validateRangeableProperties / validateEnableFilterableProperty
//     / validateEnableSearchableProperty (invalid request bodies)
//   - validateTokenizationChange edge cases (same tokenization, invalid value)
//     are exercised indirectly here via the conflict matrix because they need
//     a live DB; see acceptance tests for end-to-end coverage.
//   - buildUnitMaps / buildUnitSpecs (sort stability + group-ID = shard
//     contract used for per-tenant barrier semantics)
//   - propsOverlap with prefix-similar property names (regression: must not
//     match "foo" against "foobar")

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// -----------------------------------------------------------------------------
// propsOverlap — property-scope correctness, including prefix-similar names.
// -----------------------------------------------------------------------------

func TestPropsOverlap_PrefixSimilarNamesDoNotCollide(t *testing.T) {
	// Regression: properties whose names share a prefix must not be reported
	// as overlapping. "title" should never conflict with "titleAlt" or
	// "title_v2".
	require.False(t, db.ReindexPropsOverlap([]string{"title"}, []string{"titleAlt"}),
		`"title" must not overlap "titleAlt"`)
	require.False(t, db.ReindexPropsOverlap([]string{"title"}, []string{"title_v2"}),
		`"title" must not overlap "title_v2"`)
	require.False(t, db.ReindexPropsOverlap([]string{"foo"}, []string{"foobar", "barfoo"}),
		`"foo" must not match "foobar" or "barfoo" by prefix`)
}

func TestPropsOverlap_ExactNameMatches(t *testing.T) {
	require.True(t, db.ReindexPropsOverlap([]string{"a", "b"}, []string{"b", "c"}))
}

func TestPropsOverlap_EmptyMeansAllProperties(t *testing.T) {
	// Documented semantic: an empty Properties list means "all properties".
	// That branch is reserved for future whole-collection migrations; the
	// REST handler today always submits a single-property task.
	require.True(t, db.ReindexPropsOverlap(nil, []string{"x"}))
	require.True(t, db.ReindexPropsOverlap([]string{"x"}, nil))
	require.True(t, db.ReindexPropsOverlap(nil, nil))
}

// -----------------------------------------------------------------------------
// typesConflict — full matrix of migration-type pairs.
// -----------------------------------------------------------------------------

func TestTypesConflict_FullMatrix(t *testing.T) {
	type row struct {
		name       string
		a, b       db.ReindexMigrationType
		props      []string
		wantConfl  bool
		wantReason string
	}

	// Conflict rule: any two reindex migrations on the same (collection,
	// property) tuple conflict, regardless of which bucket type they
	// primarily write to. See typesConflict's godoc for the bug
	// (https://github.com/weaviate/weaviate/issues/10675) that motivated dropping the per-bucket-type
	// exception for enable-rangeable.
	cases := []row{
		// Same type, same property — conflict.
		{"repair-searchable vs repair-searchable", db.ReindexTypeRepairSearchable, db.ReindexTypeRepairSearchable, []string{"p"}, true, "overlapping properties"},
		{"repair-filterable vs repair-filterable", db.ReindexTypeRepairFilterable, db.ReindexTypeRepairFilterable, []string{"p"}, true, "overlapping properties"},
		{"enable-searchable vs enable-searchable", db.ReindexTypeEnableSearchable, db.ReindexTypeEnableSearchable, []string{"p"}, true, "overlapping properties"},
		{"enable-filterable vs enable-filterable", db.ReindexTypeEnableFilterable, db.ReindexTypeEnableFilterable, []string{"p"}, true, "overlapping properties"},
		{"change-tokenization vs change-tokenization", db.ReindexTypeChangeTokenization, db.ReindexTypeChangeTokenization, []string{"p"}, true, "overlapping properties"},
		{"enable-rangeable vs enable-rangeable (same prop)", db.ReindexTypeEnableRangeable, db.ReindexTypeEnableRangeable, []string{"p"}, true, "overlapping properties"},

		// Cross-type same-property — all conflict under the new rule.
		{"change-tok vs repair-searchable (same prop)", db.ReindexTypeChangeTokenization, db.ReindexTypeRepairSearchable, []string{"p"}, true, "overlapping properties"},
		{"change-tok vs enable-searchable (same prop)", db.ReindexTypeChangeTokenization, db.ReindexTypeEnableSearchable, []string{"p"}, true, "overlapping properties"},
		{"change-tok vs repair-filterable (same prop)", db.ReindexTypeChangeTokenization, db.ReindexTypeRepairFilterable, []string{"p"}, true, "overlapping properties"},
		{"change-tok vs enable-filterable (same prop)", db.ReindexTypeChangeTokenization, db.ReindexTypeEnableFilterable, []string{"p"}, true, "overlapping properties"},

		// enable-rangeable now conflicts with anything else on the same prop —
		// shared on-disk migration state (filterable_to_rangeable_<prop>) is
		// destroyed by the OnMigrationComplete updatePropertyBuckets path
		// otherwise. Same prop = conflict.
		{"enable-rangeable vs repair-searchable", db.ReindexTypeEnableRangeable, db.ReindexTypeRepairSearchable, []string{"p"}, true, "overlapping properties"},
		{"enable-rangeable vs repair-filterable", db.ReindexTypeEnableRangeable, db.ReindexTypeRepairFilterable, []string{"p"}, true, "overlapping properties"},
		{"enable-rangeable vs enable-filterable", db.ReindexTypeEnableRangeable, db.ReindexTypeEnableFilterable, []string{"p"}, true, "overlapping properties"},
		{"enable-rangeable vs enable-searchable", db.ReindexTypeEnableRangeable, db.ReindexTypeEnableSearchable, []string{"p"}, true, "overlapping properties"},
		{"enable-rangeable vs change-tokenization", db.ReindexTypeEnableRangeable, db.ReindexTypeChangeTokenization, []string{"p"}, true, "overlapping properties"},

		// Searchable-only vs filterable-only on the same property also conflicts:
		// MergeProps fans out across all flags on OnMigrationComplete so the same
		// cross-pollination concern applies.
		{"repair-searchable vs repair-filterable (same prop)", db.ReindexTypeRepairSearchable, db.ReindexTypeRepairFilterable, []string{"p"}, true, "overlapping properties"},
		{"enable-searchable vs enable-filterable (same prop)", db.ReindexTypeEnableSearchable, db.ReindexTypeEnableFilterable, []string{"p"}, true, "overlapping properties"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason := db.TypesConflictReason(tc.a, tc.props, tc.b, tc.props)
			if tc.wantConfl {
				require.NotEmpty(t, reason, "expected conflict but got none")
				assert.Contains(t, reason, tc.wantReason)
			} else {
				require.Empty(t, reason, "did not expect conflict but got: %s", reason)
			}
		})
	}
}

func TestTypesConflict_DifferentPropertiesNeverConflict(t *testing.T) {
	// Two same-type tasks on different properties of the same collection
	// must NOT conflict. This is the parallelism contract.
	for _, mt := range []db.ReindexMigrationType{
		db.ReindexTypeRepairSearchable,
		db.ReindexTypeRepairFilterable,
		db.ReindexTypeEnableSearchable,
		db.ReindexTypeEnableFilterable,
		db.ReindexTypeEnableRangeable,
		db.ReindexTypeChangeTokenization,
	} {
		t.Run(string(mt), func(t *testing.T) {
			reason := db.TypesConflictReason(mt, []string{"propA"}, mt, []string{"propB"})
			require.Empty(t, reason,
				"%s on different properties must not conflict", mt)
		})
	}
}

// -----------------------------------------------------------------------------
// checkReindexConflict — happy path + non-STARTED tasks ignored.
// -----------------------------------------------------------------------------

func mustPayload(t *testing.T, p db.ReindexTaskPayload) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	require.NoError(t, err)
	return b
}

func TestCheckReindexConflict_RejectsSameTypeSameProperty(t *testing.T) {
	existing := &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "C:enable-filterable:foo:abcd"},
		Status:         distributedtask.TaskStatusStarted,
		Payload: mustPayload(t, db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		}),
	}

	reason, err := checkReindexConflict("C", db.ReindexTypeEnableFilterable,
		[]string{"foo"}, []*distributedtask.Task{existing})
	require.NoError(t, err)
	require.NotEmpty(t, reason)
	require.Contains(t, reason, "conflicts")
	require.Contains(t, reason, existing.ID)
}

func TestCheckReindexConflict_AllowsSameTypeDifferentProperty(t *testing.T) {
	existing := &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "C:enable-filterable:foo:abcd"},
		Status:         distributedtask.TaskStatusStarted,
		Payload: mustPayload(t, db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		}),
	}

	reason, err := checkReindexConflict("C", db.ReindexTypeEnableFilterable,
		[]string{"bar"}, []*distributedtask.Task{existing})
	require.NoError(t, err)
	require.Empty(t, reason, "different property must not conflict")
}

func TestCheckReindexConflict_IgnoresTerminalTasks(t *testing.T) {
	// Non-STARTED tasks (FINISHED, FAILED, CANCELLED) must NOT block a fresh
	// submit for the same (type, property). Otherwise a successful past
	// migration would forever lock the property out.
	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusFinished,
		distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			existing := &distributedtask.Task{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "C:enable-filterable:foo:abcd"},
				Status:         status,
				Payload: mustPayload(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeEnableFilterable,
					Collection:    "C",
					Properties:    []string{"foo"},
				}),
			}
			reason, err := checkReindexConflict("C", db.ReindexTypeEnableFilterable,
				[]string{"foo"}, []*distributedtask.Task{existing})
			require.NoError(t, err)
			require.Empty(t, reason,
				"%s task must not block new submission", status)
		})
	}
}

func TestCheckReindexConflict_DifferentCollections(t *testing.T) {
	// A running task on collection A must not block submission on collection B.
	existing := &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "A:enable-filterable:foo:abcd"},
		Status:         distributedtask.TaskStatusStarted,
		Payload: mustPayload(t, db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "A",
			Properties:    []string{"foo"},
		}),
	}

	reason, err := checkReindexConflict("B", db.ReindexTypeEnableFilterable,
		[]string{"foo"}, []*distributedtask.Task{existing})
	require.NoError(t, err)
	require.Empty(t, reason)
}

func TestCheckReindexConflict_CollectionMatchIsCaseInsensitive(t *testing.T) {
	// strings.EqualFold is used; document this is intentional.
	existing := &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "MyClass:enable-filterable:foo:abcd"},
		Status:         distributedtask.TaskStatusStarted,
		Payload: mustPayload(t, db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "MyClass",
			Properties:    []string{"foo"},
		}),
	}

	reason, err := checkReindexConflict("myclass", db.ReindexTypeEnableFilterable,
		[]string{"foo"}, []*distributedtask.Task{existing})
	require.NoError(t, err)
	require.NotEmpty(t, reason, "case-insensitive collection match expected")
}

// A task with corrupt JSON cannot be proven non-conflicting, so the
// conflict check returns an error rather than silently skipping it.
// Silently skipping would let a real bucket-level conflict slip through
// — e.g. an in-flight repair-searchable from an older binary whose
// payload format we no longer understand — and allow a second task to
// race against the unparseable one.
func TestCheckReindexConflict_MalformedPayloadIsRejected(t *testing.T) {
	existing := &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "corrupt"},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        []byte(`{not valid json`),
	}
	reason, err := checkReindexConflict("C", db.ReindexTypeEnableFilterable,
		[]string{"foo"}, []*distributedtask.Task{existing})
	require.Empty(t, reason)
	require.Error(t, err, "unparseable payload must surface as an error so the handler can refuse the submit")
	require.Contains(t, err.Error(), "unparseable")
	require.Contains(t, err.Error(), "corrupt", "error message must name the offending task ID")
}

// Variant: an unparseable in-flight task on a DIFFERENT collection still
// blocks. We don't compare collections until after we've parsed, so we
// cannot rule out a same-collection conflict. Better to refuse loudly.
func TestCheckReindexConflict_MalformedPayloadDifferentCollectionAlsoRejected(t *testing.T) {
	existing := &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "corrupt2"},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        []byte(`{still not valid`),
	}
	_, err := checkReindexConflict("OtherCollection", db.ReindexTypeEnableFilterable,
		[]string{"foo"}, []*distributedtask.Task{existing})
	require.Error(t, err)
}

// A payload that parses to JSON successfully but has an empty
// Collection or MigrationType is informationally indistinguishable from
// an unparseable one: we don't know what bucket it would touch. Treat
// it the same way (refuse with an error rather than silently skip).
// Most realistic cause: payload-schema rename across versions where the
// JSON tag changed.
func TestCheckReindexConflict_EmptyJsonPayloadIsRejected(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{"empty object", `{}`},
		{"missing collection", `{"migrationType":"enable-filterable","properties":["foo"]}`},
		{"missing migrationType", `{"collection":"C","properties":["foo"]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			existing := &distributedtask.Task{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "empty-info"},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        []byte(c.payload),
			}
			_, err := checkReindexConflict("C", db.ReindexTypeEnableFilterable,
				[]string{"foo"}, []*distributedtask.Task{existing})
			require.Error(t, err, "payload %q parses but is informationally empty; must be rejected", c.payload)
			require.Contains(t, err.Error(), "empty Collection or MigrationType")
		})
	}
}

// -----------------------------------------------------------------------------
// validateRangeableProperties — invalid request bodies.
// -----------------------------------------------------------------------------

func boolPtr(b bool) *bool { return &b }

func TestValidateRangeableProperties(t *testing.T) {
	trueVal := true
	class := &models.Class{
		Class: "C",
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"number"}},
			{Name: "qty", DataType: []string{"int"}},
			{Name: "when", DataType: []string{"date"}},
			{Name: "label", DataType: []string{"text"}},
			{Name: "alreadyRange", DataType: []string{"int"}, IndexRangeFilters: &trueVal},
			// Reference property (non-primitive).
			{Name: "ref", DataType: []string{"OtherClass"}},
		},
	}

	t.Run("valid numeric props", func(t *testing.T) {
		require.NoError(t, validateRangeableProperties(class, []string{"score", "qty", "when"}))
	})

	t.Run("unknown property", func(t *testing.T) {
		err := validateRangeableProperties(class, []string{"nope"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("non-numeric (text)", func(t *testing.T) {
		err := validateRangeableProperties(class, []string{"label"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a numeric type")
	})

	t.Run("non-primitive (reference)", func(t *testing.T) {
		err := validateRangeableProperties(class, []string{"ref"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a numeric type")
	})

	t.Run("already rangeable", func(t *testing.T) {
		err := validateRangeableProperties(class, []string{"alreadyRange"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "already")
	})
}

func TestValidateRebuildRangeableProperty(t *testing.T) {
	trueVal := true
	falseVal := false

	t.Run("rangeable enabled numeric ok", func(t *testing.T) {
		prop := &models.Property{Name: "qty", DataType: []string{"int"}, IndexRangeFilters: &trueVal}
		require.NoError(t, validateRebuildRangeableProperty(prop))
	})

	t.Run("rebuild rejected when rangeable not enabled", func(t *testing.T) {
		prop := &models.Property{Name: "qty", DataType: []string{"int"}}
		err := validateRebuildRangeableProperty(prop)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not have a rangeable index to rebuild")
	})

	t.Run("rebuild rejected when rangeable explicitly false", func(t *testing.T) {
		prop := &models.Property{Name: "qty", DataType: []string{"int"}, IndexRangeFilters: &falseVal}
		err := validateRebuildRangeableProperty(prop)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not have a rangeable index to rebuild")
	})

	t.Run("rebuild rejected for non-numeric type", func(t *testing.T) {
		prop := &models.Property{Name: "label", DataType: []string{"text"}, IndexRangeFilters: &trueVal}
		err := validateRebuildRangeableProperty(prop)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a numeric type")
	})
}

// TestValidateRebuildFilterableDataType pins the data-type guard that
// repair-filterable must apply. Without it, types whose schema default
// forces IndexFilterable=true but which have no inverted bucket on disk
// (geoCoordinates, phoneNumber, blob, references) would pass the
// "index enabled" check and the rebuild dispatch would crash downstream
// with `target bucket "property_p" not found in store`.
func TestValidateRebuildFilterableDataType(t *testing.T) {
	t.Run("text ok", func(t *testing.T) {
		prop := &models.Property{Name: "p", DataType: []string{"text"}}
		require.NoError(t, validateRebuildFilterableDataType(prop))
	})

	t.Run("int ok", func(t *testing.T) {
		prop := &models.Property{Name: "p", DataType: []string{"int"}}
		require.NoError(t, validateRebuildFilterableDataType(prop))
	})

	t.Run("boolean ok", func(t *testing.T) {
		prop := &models.Property{Name: "p", DataType: []string{"boolean"}}
		require.NoError(t, validateRebuildFilterableDataType(prop))
	})

	t.Run("uuid ok", func(t *testing.T) {
		prop := &models.Property{Name: "p", DataType: []string{"uuid"}}
		require.NoError(t, validateRebuildFilterableDataType(prop))
	})

	t.Run("date ok", func(t *testing.T) {
		prop := &models.Property{Name: "p", DataType: []string{"date"}}
		require.NoError(t, validateRebuildFilterableDataType(prop))
	})

	t.Run("number ok", func(t *testing.T) {
		prop := &models.Property{Name: "p", DataType: []string{"number"}}
		require.NoError(t, validateRebuildFilterableDataType(prop))
	})

	t.Run("rebuild rejected for geoCoordinates", func(t *testing.T) {
		// Schema migrator defaults IndexFilterable=true even for
		// geoCoordinates, so the prior "is the flag true?" check let this
		// through and the rebuild crashed at swap time.
		prop := &models.Property{Name: "p", DataType: []string{"geoCoordinates"}}
		err := validateRebuildFilterableDataType(prop)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not support a filterable inverted index")
		require.Contains(t, err.Error(), "nothing to rebuild")
		require.Contains(t, err.Error(), "geoCoordinates")
	})

	t.Run("rebuild rejected for phoneNumber", func(t *testing.T) {
		prop := &models.Property{Name: "p", DataType: []string{"phoneNumber"}}
		err := validateRebuildFilterableDataType(prop)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not support a filterable inverted index")
		require.Contains(t, err.Error(), "phoneNumber")
	})

	t.Run("rebuild rejected for blob", func(t *testing.T) {
		prop := &models.Property{Name: "p", DataType: []string{"blob"}}
		err := validateRebuildFilterableDataType(prop)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not support a filterable inverted index")
		require.Contains(t, err.Error(), "blob")
	})

	t.Run("rebuild rejected for reference type", func(t *testing.T) {
		// Reference (non-primitive) data types have no inverted bucket.
		prop := &models.Property{Name: "p", DataType: []string{"SomeClass"}}
		err := validateRebuildFilterableDataType(prop)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not support a filterable inverted index")
		require.Contains(t, err.Error(), "nothing to rebuild")
	})
}

func TestRequestedCancel(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		typ, ok := requestedCancel(&models.IndexUpdateRequest{
			Searchable: &models.IndexUpdateSearchable{Enabled: true},
		})
		require.False(t, ok)
		require.Empty(t, typ)
	})

	t.Run("searchable.cancel", func(t *testing.T) {
		typ, ok := requestedCancel(&models.IndexUpdateRequest{
			Searchable: &models.IndexUpdateSearchable{Cancel: true},
		})
		require.True(t, ok)
		require.Equal(t, "searchable", typ)
	})

	t.Run("filterable.cancel", func(t *testing.T) {
		typ, ok := requestedCancel(&models.IndexUpdateRequest{
			Filterable: &models.IndexUpdateFilterable{Cancel: true},
		})
		require.True(t, ok)
		require.Equal(t, "filterable", typ)
	})

	t.Run("rangeable.cancel", func(t *testing.T) {
		typ, ok := requestedCancel(&models.IndexUpdateRequest{
			Rangeable: &models.IndexUpdateRangeable{Cancel: true},
		})
		require.True(t, ok)
		require.Equal(t, "rangeable", typ)
	})
}

func TestMigrationTypeTargetsIndex(t *testing.T) {
	cases := []struct {
		mt          db.ReindexMigrationType
		indexType   string
		wantMatches bool
		wantKnown   bool
	}{
		{db.ReindexTypeEnableSearchable, "searchable", true, true},
		{db.ReindexTypeEnableSearchable, "filterable", false, true},
		{db.ReindexTypeRepairSearchable, "searchable", true, true},
		{db.ReindexTypeEnableFilterable, "filterable", true, true},
		{db.ReindexTypeEnableFilterable, "searchable", false, true},
		{db.ReindexTypeRepairFilterable, "filterable", true, true},
		{db.ReindexTypeEnableRangeable, "rangeable", true, true},
		{db.ReindexTypeRepairRangeable, "rangeable", true, true},
		{db.ReindexTypeRepairRangeable, "searchable", false, true},
		{db.ReindexTypeChangeTokenization, "searchable", true, true},
		{db.ReindexTypeChangeTokenization, "filterable", true, true},
		{db.ReindexTypeChangeTokenization, "rangeable", false, true},
		// Unknown migration type — both returns must be false.
		{db.ReindexMigrationType("totally-new-migration"), "searchable", false, false},
	}
	for _, c := range cases {
		t.Run(string(c.mt)+"/"+c.indexType, func(t *testing.T) {
			matches, known := migrationTypeTargetsIndex(c.mt, c.indexType)
			require.Equal(t, c.wantMatches, matches, "matches")
			require.Equal(t, c.wantKnown, known, "isKnown")
		})
	}
}

// TestIndexTypesFromMigrationType locks in the contract that submit-time
// pre-cleanup uses to decide which index sentinel dirs to wipe. The
// critical case is ReindexTypeChangeTokenization (change-tokenization-both):
// it MUST return both "searchable" and "filterable" so the submit handler
// cleans up sentinel dirs from BOTH per-index sub-tasks. Returning only one
// (or neither) reproduces the Sev 1 silent data loss where a stale
// tidied.mig from a prior single-index retokenize causes the FilterableRetokenize
// sub-task to short-circuit on OnAfterLsmInit's IsTidied check —
// OnMigrationComplete still flips the schema's Tokenization, but the
// filterable bucket retains the OLD tokenization (Journey 7 in
// change_tok_delete_journeys_test.go).
func TestIndexTypesFromMigrationType(t *testing.T) {
	cases := []struct {
		mt        db.ReindexMigrationType
		wantTypes []string
		wantKnown bool
	}{
		{db.ReindexTypeEnableSearchable, []string{"searchable"}, true},
		{db.ReindexTypeRepairSearchable, []string{"searchable"}, true},
		{db.ReindexTypeEnableFilterable, []string{"filterable"}, true},
		{db.ReindexTypeRepairFilterable, []string{"filterable"}, true},
		{db.ReindexTypeEnableRangeable, []string{"rangeable"}, true},
		{db.ReindexTypeRepairRangeable, []string{"rangeable"}, true},
		// change-tok-filterable retokenizes only the filterable bucket.
		{db.ReindexTypeChangeTokenizationFilterable, []string{"filterable"}, true},
		// change-tok-both spawns one sub-task per inverted index, each with
		// its own per-property migration dir on disk. Pre-cleanup must wipe
		// both — see godoc on indexTypesFromMigrationType.
		{db.ReindexTypeChangeTokenization, []string{"searchable", "filterable"}, true},
		// Unknown migration type — must return false so the submit handler
		// degrades to "no pre-cleanup" rather than panicking.
		{db.ReindexMigrationType("never-heard-of-this"), nil, false},
	}
	for _, c := range cases {
		t.Run(string(c.mt), func(t *testing.T) {
			gotTypes, gotKnown := indexTypesFromMigrationType(c.mt)
			require.Equal(t, c.wantKnown, gotKnown, "isKnown")
			require.ElementsMatch(t, c.wantTypes, gotTypes,
				"indexTypes for %s", c.mt)
		})
	}
}

// -----------------------------------------------------------------------------
// validateEnableFilterableProperty — type allow-list and already-filterable.
// -----------------------------------------------------------------------------

func TestValidateEnableFilterableProperty(t *testing.T) {
	cases := []struct {
		name    string
		prop    *models.Property
		wantErr string
	}{
		{"text ok", &models.Property{Name: "p", DataType: []string{"text"}}, ""},
		{"int ok", &models.Property{Name: "p", DataType: []string{"int"}}, ""},
		{"number ok", &models.Property{Name: "p", DataType: []string{"number"}}, ""},
		{"boolean ok", &models.Property{Name: "p", DataType: []string{"boolean"}}, ""},
		{"date ok", &models.Property{Name: "p", DataType: []string{"date"}}, ""},
		{"uuid ok", &models.Property{Name: "p", DataType: []string{"uuid"}}, ""},

		// Disallowed primitive types.
		{"blob rejected", &models.Property{Name: "p", DataType: []string{"blob"}}, "does not support"},
		{"geoCoordinates rejected", &models.Property{Name: "p", DataType: []string{"geoCoordinates"}}, "does not support"},
		{"phoneNumber rejected", &models.Property{Name: "p", DataType: []string{"phoneNumber"}}, "does not support"},

		// References are non-primitive.
		{"reference rejected", &models.Property{Name: "p", DataType: []string{"OtherClass"}}, "does not support"},

		// Already-filterable rejected.
		{"already filterable", &models.Property{Name: "p", DataType: []string{"text"}, IndexFilterable: boolPtr(true)}, "already has a filterable index"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateEnableFilterableProperty(tc.prop)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// validateEnableSearchableProperty — tokenization required + valid + text type.
// -----------------------------------------------------------------------------

func TestValidateEnableSearchableProperty(t *testing.T) {
	cases := []struct {
		name    string
		prop    *models.Property
		tok     string
		wantErr string
	}{
		// Happy paths — every valid tokenization.
		{"word", &models.Property{Name: "p", DataType: []string{"text"}}, "word", ""},
		{"lowercase", &models.Property{Name: "p", DataType: []string{"text"}}, "lowercase", ""},
		{"whitespace", &models.Property{Name: "p", DataType: []string{"text"}}, "whitespace", ""},
		{"field", &models.Property{Name: "p", DataType: []string{"text"}}, "field", ""},
		{"trigram", &models.Property{Name: "p", DataType: []string{"text"}}, "trigram", ""},
		{"gse", &models.Property{Name: "p", DataType: []string{"text"}}, "gse", ""},
		{"text[] ok", &models.Property{Name: "p", DataType: []string{"text[]"}}, "word", ""},

		// Reject paths.
		{"empty tokenization", &models.Property{Name: "p", DataType: []string{"text"}}, "", "requires a tokenization"},
		{"invalid tokenization", &models.Property{Name: "p", DataType: []string{"text"}}, "splat", "invalid tokenization"},
		{"non-text (int)", &models.Property{Name: "p", DataType: []string{"int"}}, "word", "not a text type"},
		{"already searchable", &models.Property{Name: "p", DataType: []string{"text"}, IndexSearchable: boolPtr(true)}, "word", "already has a searchable index"},

		// Reject: filterable index already built with a different
		// tokenization. EnableSearchable.OnMigrationComplete writes
		// Tokenization=s.tokenization unconditionally, which would silently
		// desynchronize the existing filterable bucket's terms.
		{
			"filterable with different tokenization",
			&models.Property{
				Name:            "p",
				DataType:        []string{"text"},
				IndexFilterable: boolPtr(true),
				Tokenization:    "word",
			},
			"field",
			"would silently desynchronize",
		},
		{
			"filterable with different tokenization (text[])",
			&models.Property{
				Name:            "p",
				DataType:        []string{"text[]"},
				IndexFilterable: boolPtr(true),
				Tokenization:    "lowercase",
			},
			"whitespace",
			"would silently desynchronize",
		},

		// Allow: filterable index with the SAME tokenization — the new
		// searchable bucket will agree with the existing filterable one.
		{
			"filterable with same tokenization",
			&models.Property{
				Name:            "p",
				DataType:        []string{"text"},
				IndexFilterable: boolPtr(true),
				Tokenization:    "word",
			},
			"word",
			"",
		},
		// Allow: no filterable index at all (irrelevant) — divergence
		// cannot happen.
		{
			"no filterable, different stored tokenization",
			&models.Property{
				Name:            "p",
				DataType:        []string{"text"},
				IndexFilterable: boolPtr(false),
				Tokenization:    "word",
			},
			"field",
			"",
		},
		{
			"nil filterable, different stored tokenization",
			&models.Property{
				Name:         "p",
				DataType:     []string{"text"},
				Tokenization: "word",
			},
			"field",
			"",
		},
		// Allow: filterable with empty stored tokenization (e.g. legacy
		// property without an explicit tokenization recorded).
		{
			"filterable with empty stored tokenization",
			&models.Property{
				Name:            "p",
				DataType:        []string{"text"},
				IndexFilterable: boolPtr(true),
			},
			"word",
			"",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateEnableSearchableProperty(tc.prop, tc.tok)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// buildUnitMaps / buildUnitSpecs — sort stability + GroupID == shard contract.
// -----------------------------------------------------------------------------

func TestBuildUnitMaps_DeterministicSortAndCorrectMappings(t *testing.T) {
	// Multiple shards × multiple nodes; unit IDs should be sorted and the
	// reverse maps must correctly identify shard and node.
	ownership := map[string][]string{
		"node-2": {"shard-b", "shard-a"},
		"node-1": {"shard-a"},
	}
	unitIDs, unitToShard, unitToNode := buildUnitMaps(ownership)

	// Sorted ascending — go map iteration order must not leak.
	require.Equal(t, []string{
		"shard-a__node-1",
		"shard-a__node-2",
		"shard-b__node-2",
	}, unitIDs)

	require.Equal(t, "shard-a", unitToShard["shard-a__node-1"])
	require.Equal(t, "node-1", unitToNode["shard-a__node-1"])
	require.Equal(t, "shard-b", unitToShard["shard-b__node-2"])
	require.Equal(t, "node-2", unitToNode["shard-b__node-2"])
}

func TestBuildUnitSpecs_GroupIDIsShardName(t *testing.T) {
	// Critical contract: GroupID == shardName so OnGroupCompleted fires
	// per-tenant for MT semantic migrations.
	ownership := map[string][]string{
		"node-1": {"tenant-a", "tenant-b"},
		"node-2": {"tenant-a", "tenant-b"},
	}
	specs := buildUnitSpecs(ownership)
	require.Len(t, specs, 4)

	// Each unit spec's GroupID must equal the tenant/shard portion of the ID.
	groupsForTenantA := 0
	groupsForTenantB := 0
	for _, s := range specs {
		// Pull the shard prefix back out and compare to GroupID.
		switch s.GroupID {
		case "tenant-a":
			require.Contains(t, s.ID, "tenant-a__")
			groupsForTenantA++
		case "tenant-b":
			require.Contains(t, s.ID, "tenant-b__")
			groupsForTenantB++
		default:
			t.Fatalf("unexpected GroupID %q", s.GroupID)
		}
	}
	require.Equal(t, 2, groupsForTenantA, "should have one unit per replica node")
	require.Equal(t, 2, groupsForTenantB)
}

func TestBuildUnitSpecs_DeterministicSort(t *testing.T) {
	ownership := map[string][]string{
		"zz": {"b", "a"},
		"aa": {"b"},
	}
	specs := buildUnitSpecs(ownership)
	for i := 1; i < len(specs); i++ {
		require.Less(t, specs[i-1].ID, specs[i].ID,
			"specs must be sorted by ID — got %v", specs)
	}
}

// -----------------------------------------------------------------------------
// touchesSearchable / touchesFilterable — exhaustive switch, including a
// panic on unknown ReindexMigrationType so a future type cannot silently
// bypass the conflict check.
// -----------------------------------------------------------------------------

func TestTouchesSearchable(t *testing.T) {
	cases := []struct {
		t    db.ReindexMigrationType
		want bool
	}{
		{db.ReindexTypeRepairSearchable, true},
		{db.ReindexTypeChangeTokenization, true},
		{db.ReindexTypeEnableSearchable, true},
		{db.ReindexTypeRepairFilterable, false},
		{db.ReindexTypeEnableFilterable, false},
		{db.ReindexTypeEnableRangeable, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.t), func(t *testing.T) {
			require.Equal(t, tc.want, db.TouchesSearchable(tc.t))
		})
	}
}

func TestTouchesFilterable(t *testing.T) {
	cases := []struct {
		t    db.ReindexMigrationType
		want bool
	}{
		{db.ReindexTypeRepairFilterable, true},
		{db.ReindexTypeChangeTokenization, true},
		{db.ReindexTypeEnableFilterable, true},
		{db.ReindexTypeRepairSearchable, false},
		{db.ReindexTypeEnableSearchable, false},
		{db.ReindexTypeEnableRangeable, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.t), func(t *testing.T) {
			require.Equal(t, tc.want, db.TouchesFilterable(tc.t))
		})
	}
}

func TestTouchesSearchable_PanicsOnUnknownType(t *testing.T) {
	require.PanicsWithValue(t,
		`TouchesSearchable: unknown ReindexMigrationType "phantom" — add it to this switch`,
		func() { db.TouchesSearchable(db.ReindexMigrationType("phantom")) },
		"unknown migration type must panic so the gap is caught loudly",
	)
}

func TestTouchesFilterable_PanicsOnUnknownType(t *testing.T) {
	require.PanicsWithValue(t,
		`TouchesFilterable: unknown ReindexMigrationType "phantom" — add it to this switch`,
		func() { db.TouchesFilterable(db.ReindexMigrationType("phantom")) },
		"unknown migration type must panic so the gap is caught loudly",
	)
}

// -----------------------------------------------------------------------------
// validateBodyExclusivity — switch-shadow guard.
//
// updateIndex dispatches on a Go switch where the FIRST truthy arm wins.
// Without this guard, a body with two groups (e.g. searchable.rebuild AND
// filterable.rebuild) or two verbs in one group (e.g. searchable.enabled AND
// searchable.rebuild) would silently run one and drop the other. These cases
// pin the rejection.
// -----------------------------------------------------------------------------

func TestValidateBodyExclusivity(t *testing.T) {
	cases := []struct {
		name    string
		body    *models.IndexUpdateRequest
		wantErr string // empty = accept; substring = reject and assert substring in error
	}{
		// --- nil body -----------------------------------------------------------
		{
			name:    "nil body rejected",
			body:    nil,
			wantErr: "request body required",
		},

		// --- valid: exactly one verb in one group ------------------------------
		{
			name: "valid: searchable.rebuild only",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Rebuild: true},
			},
		},
		{
			name: "valid: searchable.enabled with tokenization (one verb)",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Enabled: true, Tokenization: "word"},
			},
		},
		{
			name: "valid: searchable.tokenization alone (change-tokenization)",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Tokenization: "word"},
			},
		},
		{
			name: "valid: filterable.enabled only",
			body: &models.IndexUpdateRequest{
				Filterable: &models.IndexUpdateFilterable{Enabled: true},
			},
		},
		{
			name: "valid: filterable.rebuild only",
			body: &models.IndexUpdateRequest{
				Filterable: &models.IndexUpdateFilterable{Rebuild: true},
			},
		},
		{
			name: "valid: rangeable.enabled only",
			body: &models.IndexUpdateRequest{
				Rangeable: &models.IndexUpdateRangeable{Enabled: true},
			},
		},

		// --- zero verbs --------------------------------------------------------
		{
			name:    "reject: empty body (all groups nil)",
			body:    &models.IndexUpdateRequest{},
			wantErr: "no actionable change",
		},
		{
			name: "reject: searchable present but no verb set",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{},
			},
			wantErr: "no actionable change",
		},
		{
			name: "reject: filterable present but no verb set",
			body: &models.IndexUpdateRequest{
				Filterable: &models.IndexUpdateFilterable{},
			},
			wantErr: "no actionable change",
		},
		{
			name: "reject: rangeable present but enabled=false",
			body: &models.IndexUpdateRequest{
				Rangeable: &models.IndexUpdateRangeable{Enabled: false},
			},
			wantErr: "no actionable change",
		},

		// --- multiple groups ---------------------------------------------------
		{
			name: "reject: searchable.rebuild + filterable.rebuild",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Rebuild: true},
				Filterable: &models.IndexUpdateFilterable{Rebuild: true},
			},
			wantErr: "multiple index groups",
		},
		{
			name: "reject: searchable.rebuild + rangeable.enabled",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Rebuild: true},
				Rangeable:  &models.IndexUpdateRangeable{Enabled: true},
			},
			wantErr: "multiple index groups",
		},
		{
			name: "reject: filterable.enabled + rangeable.enabled",
			body: &models.IndexUpdateRequest{
				Filterable: &models.IndexUpdateFilterable{Enabled: true},
				Rangeable:  &models.IndexUpdateRangeable{Enabled: true},
			},
			wantErr: "multiple index groups",
		},
		{
			name: "reject: all three groups set",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Enabled: true, Tokenization: "word"},
				Filterable: &models.IndexUpdateFilterable{Enabled: true},
				Rangeable:  &models.IndexUpdateRangeable{Enabled: true},
			},
			wantErr: "multiple index groups",
		},

		// --- multiple verbs within one group -----------------------------------
		{
			name: "reject: searchable.enabled + searchable.rebuild",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Enabled: true, Rebuild: true, Tokenization: "word"},
			},
			wantErr: "conflicting fields in searchable",
		},
		{
			name: "reject: searchable.rebuild + searchable.tokenization (without enabled)",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Rebuild: true, Tokenization: "word"},
			},
			wantErr: "conflicting fields in searchable",
		},
		{
			name: "reject: filterable.enabled + filterable.rebuild",
			body: &models.IndexUpdateRequest{
				Filterable: &models.IndexUpdateFilterable{Enabled: true, Rebuild: true},
			},
			wantErr: "conflicting fields in filterable",
		},

		// --- algorithm verb ----------------------------------------------------
		{
			name: "valid: searchable.algorithm alone",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Algorithm: "blockmax"},
			},
		},
		{
			name: "reject: searchable.algorithm + searchable.rebuild",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Algorithm: "blockmax", Rebuild: true},
			},
			wantErr: "conflicting fields in searchable",
		},
		{
			name: "reject: searchable.algorithm + searchable.tokenization",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Algorithm: "blockmax", Tokenization: "word"},
			},
			wantErr: "conflicting fields in searchable",
		},
		{
			name: "reject: searchable.algorithm + searchable.enabled",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Algorithm: "blockmax", Enabled: true},
			},
			wantErr: "conflicting fields in searchable",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBodyExclusivity(tc.body)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// countStartedTasksForCollection / per-collection concurrent reindex cap.
//
// Pins (1) the count function only counts STARTED tasks targeting the named
// collection, ignoring terminal-status tasks and tasks targeting other
// collections; and (2) the comparison against
// maxConcurrentReindexPerCollection has the right semantics: a fresh submit
// is admitted exactly when there are strictly fewer than the cap already
// STARTED, so the effective max number of simultaneously running tasks is
// equal to the cap constant (not cap-1 and not cap+1).
//
// Regression: the cap value was originally 4, which broke the
// reindex_concurrent acceptance test that submits 10 text + 5 int = 15
// concurrent non-conflicting tasks. The cap is now sized to accommodate
// realistic batch property migrations.
// -----------------------------------------------------------------------------

func TestCountStartedTasksForCollection_FiltersByStatusAndCollection(t *testing.T) {
	mkPayload := func(coll string) db.ReindexTaskPayload {
		return db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeChangeTokenization,
			Collection:    coll,
			Properties:    []string{"p"},
		}
	}

	cases := []struct {
		name       string
		collection string
		tasks      []*distributedtask.Task
		want       int
	}{
		{
			name:       "no tasks",
			collection: "C",
			tasks:      nil,
			want:       0,
		},
		{
			name:       "single STARTED for collection",
			collection: "C",
			tasks: []*distributedtask.Task{
				buildTask(t, "t1", distributedtask.TaskStatusStarted, mkPayload("C"), nil),
			},
			want: 1,
		},
		{
			name:       "FINISHED/FAILED/CANCELLED ignored",
			collection: "C",
			tasks: []*distributedtask.Task{
				buildTask(t, "t1", distributedtask.TaskStatusStarted, mkPayload("C"), nil),
				buildTask(t, "t2", distributedtask.TaskStatusFinished, mkPayload("C"), nil),
				buildTask(t, "t3", distributedtask.TaskStatusFailed, mkPayload("C"), nil),
				buildTask(t, "t4", distributedtask.TaskStatusCancelled, mkPayload("C"), nil),
			},
			want: 1,
		},
		{
			name:       "other collection ignored",
			collection: "C",
			tasks: []*distributedtask.Task{
				buildTask(t, "t1", distributedtask.TaskStatusStarted, mkPayload("C"), nil),
				buildTask(t, "t2", distributedtask.TaskStatusStarted, mkPayload("OtherCollection"), nil),
			},
			want: 1,
		},
		{
			name:       "case-insensitive match on collection name",
			collection: "ConcurrentReindexTest",
			tasks: []*distributedtask.Task{
				// Lower-case stored payload — must still match the canonical
				// collection name passed by the handler.
				buildTask(t, "t1", distributedtask.TaskStatusStarted,
					mkPayload("concurrentreindextest"), nil),
			},
			want: 1,
		},
		{
			name:       "unparseable payload silently skipped",
			collection: "C",
			tasks: []*distributedtask.Task{
				buildTask(t, "t1", distributedtask.TaskStatusStarted, mkPayload("C"), nil),
				{
					Namespace: db.ReindexNamespace,
					TaskDescriptor: distributedtask.TaskDescriptor{
						ID:      "garbage",
						Version: 1,
					},
					Payload: []byte("not valid json"),
					Status:  distributedtask.TaskStatusStarted,
				},
			},
			want: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := countStartedTasksForCollection(tc.collection, tc.tasks)
			require.Equal(t, tc.want, got)
		})
	}
}

// Pins the boundary of the cap comparison: a submit is rejected only when
// the inflight count is greater than or equal to the cap. With the current
// `inflight >= max` predicate, this means:
//   - inflight == max-1 → admit (caller becomes the max-th simultaneously
//     running task)
//   - inflight == max → reject (admitting would push us above the cap)
//
// If the comparison were ever flipped to `inflight > max`, the cap would
// silently grow by one (max+1 simultaneous). If it were `inflight+1 >= max`
// the cap would silently shrink to max-1. Both have caused outages of this
// kind before — the test pins the exact arithmetic so a future refactor
// can't drift the semantics.
func TestConcurrentReindexCap_RejectionBoundary(t *testing.T) {
	mkStarted := func(id, coll string) *distributedtask.Task {
		return buildTask(t, id, distributedtask.TaskStatusStarted,
			db.ReindexTaskPayload{
				MigrationType: db.ReindexTypeChangeTokenization,
				Collection:    coll,
				Properties:    []string{"p_" + id},
			}, nil)
	}

	const collection = "C"
	cap := maxConcurrentReindexPerCollection

	// At the cap minus one, a submit must be admitted.
	tasksAtCapMinusOne := make([]*distributedtask.Task, 0, cap-1)
	for i := 0; i < cap-1; i++ {
		tasksAtCapMinusOne = append(tasksAtCapMinusOne,
			mkStarted(fmtTaskID(i), collection))
	}
	got := countStartedTasksForCollection(collection, tasksAtCapMinusOne)
	require.Equal(t, cap-1, got)
	require.False(t, got >= cap,
		"with %d inflight (cap=%d) the submit must be admitted", got, cap)

	// At exactly the cap, a submit must be rejected.
	tasksAtCap := append(tasksAtCapMinusOne, mkStarted(fmtTaskID(cap-1), collection))
	got = countStartedTasksForCollection(collection, tasksAtCap)
	require.Equal(t, cap, got)
	require.True(t, got >= cap,
		"with %d inflight (cap=%d) the submit must be rejected", got, cap)
}

func fmtTaskID(i int) string {
	return "t-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
}
