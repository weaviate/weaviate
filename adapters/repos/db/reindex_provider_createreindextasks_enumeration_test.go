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

package db

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
)

// Structural pin for createReindexTasks' switch: every
// reindex.ReindexMigrationType in reindex_provider_payload.go must dispatch to
// at least one reindex.ShardReindexTaskGeneric or fail with a documented
// error. A new constant without a matching case fails the loop below
// instead of silently producing "unknown migration type" at runtime.

// allKnownMigrationTypes is the authoritative enumeration; sorted to
// match the declaration order in reindex_provider_payload.go.
// New type? Add here AND in createReindexTasksEnumerationCase.
func allKnownMigrationTypes() []reindex.ReindexMigrationType {
	return []reindex.ReindexMigrationType{
		reindex.ReindexTypeChangeAlgorithm,
		reindex.ReindexTypeRebuildSearchable,
		reindex.ReindexTypeRepairFilterable,
		reindex.ReindexTypeEnableRangeable,
		reindex.ReindexTypeRepairRangeable,
		reindex.ReindexTypeEnableFilterable,
		reindex.ReindexTypeEnableSearchable,
		reindex.ReindexTypeChangeTokenization,
		reindex.ReindexTypeChangeTokenizationFilterable,
	}
}

// createReindexTasksEnumerationCase describes the expected dispatch for
// one migration type. Each case lists a minimum-viable payload (the
// shape the production REST handler / RAFT replay constructs) plus the
// expected outcome: either a positive number of tasks, or an explicit
// error substring.
//
// Adding a new reindex.ReindexMigrationType to allKnownMigrationTypes() without
// adding a matching case here fails TestCreateReindexTasks_EnumerationExhaustive.
type createReindexTasksEnumerationCase struct {
	mt           reindex.ReindexMigrationType
	payload      *reindex.ReindexTaskPayload
	wantNTasks   int    // 0 means error expected
	wantErrSubst string // empty means no error expected
}

func enumerationCases() []createReindexTasksEnumerationCase {
	return []createReindexTasksEnumerationCase{
		{
			mt:         reindex.ReindexTypeChangeAlgorithm,
			payload:    &reindex.ReindexTaskPayload{Collection: "MyClass", Properties: []string{"title"}},
			wantNTasks: 1,
		},
		{
			mt:         reindex.ReindexTypeRebuildSearchable,
			payload:    &reindex.ReindexTaskPayload{Collection: "MyClass", Properties: []string{"title"}},
			wantNTasks: 1,
		},
		{
			mt:         reindex.ReindexTypeRepairFilterable,
			payload:    &reindex.ReindexTaskPayload{Collection: "MyClass", Properties: []string{"tag"}},
			wantNTasks: 1,
		},
		{
			mt:         reindex.ReindexTypeEnableRangeable,
			payload:    &reindex.ReindexTaskPayload{Collection: "MyClass", Properties: []string{"age"}},
			wantNTasks: 1,
		},
		{
			mt:         reindex.ReindexTypeRepairRangeable,
			payload:    &reindex.ReindexTaskPayload{Collection: "MyClass", Properties: []string{"age"}},
			wantNTasks: 1,
		},
		{
			mt:         reindex.ReindexTypeEnableFilterable,
			payload:    &reindex.ReindexTaskPayload{Collection: "MyClass", Properties: []string{"tag"}},
			wantNTasks: 1,
		},
		{
			mt: reindex.ReindexTypeEnableSearchable,
			payload: &reindex.ReindexTaskPayload{
				Collection:         "MyClass",
				Properties:         []string{"title"},
				TargetTokenization: "word",
			},
			wantNTasks: 1,
		},
		{
			mt: reindex.ReindexTypeEnableSearchable,
			payload: &reindex.ReindexTaskPayload{
				Collection: "MyClass",
				Properties: []string{"title"},
				// TargetTokenization deliberately empty → required-field error.
			},
			wantNTasks:   0,
			wantErrSubst: "enable-searchable requires targetTokenization",
		},
		{
			mt: reindex.ReindexTypeChangeTokenization,
			payload: &reindex.ReindexTaskPayload{
				Collection:         "MyClass",
				Properties:         []string{"title"},
				TargetTokenization: "field",
				BucketStrategy:     "MapCollection",
			},
			// With nil schemaManager, propertyHasFilterableBucket returns
			// true (defensive), so both searchable + filterable sub-tasks
			// are created.
			wantNTasks: 2,
		},
		{
			mt: reindex.ReindexTypeChangeTokenization,
			payload: &reindex.ReindexTaskPayload{
				Collection: "MyClass",
				Properties: []string{"title"},
				// Missing TargetTokenization + BucketStrategy →
				// required-field error.
			},
			wantNTasks:   0,
			wantErrSubst: "change-tokenization requires targetTokenization",
		},
		{
			mt: reindex.ReindexTypeChangeTokenization,
			payload: &reindex.ReindexTaskPayload{
				Collection:         "MyClass",
				Properties:         []string{"a", "b"}, // more than 1 prop
				TargetTokenization: "field",
				BucketStrategy:     "MapCollection",
			},
			wantNTasks:   0,
			wantErrSubst: "exactly one property",
		},
		{
			mt: reindex.ReindexTypeChangeTokenizationFilterable,
			payload: &reindex.ReindexTaskPayload{
				Collection:         "MyClass",
				Properties:         []string{"title"},
				TargetTokenization: "field",
			},
			wantNTasks: 1,
		},
		{
			mt: reindex.ReindexTypeChangeTokenizationFilterable,
			payload: &reindex.ReindexTaskPayload{
				Collection: "MyClass",
				Properties: []string{"title"},
				// TargetTokenization deliberately empty.
			},
			wantNTasks:   0,
			wantErrSubst: "change-tokenization-filterable requires targetTokenization",
		},
	}
}

// TestCreateReindexTasks_EnumerationExhaustive verifies that every type
// in allKnownMigrationTypes() has at least one happy-path case in
// enumerationCases(). The test FAILS if you add a new type to the
// enumeration without describing its expected dispatch.
//
// Pair-test with TestCreateReindexTasks_AllKnownTypesDispatched below
// to catch the inverse drift (a case in enumerationCases for a type
// not in allKnownMigrationTypes).
func TestCreateReindexTasks_EnumerationExhaustive(t *testing.T) {
	hasHappyPath := map[reindex.ReindexMigrationType]bool{}
	for _, c := range enumerationCases() {
		if c.wantNTasks > 0 {
			hasHappyPath[c.mt] = true
		}
	}
	for _, mt := range allKnownMigrationTypes() {
		assert.Truef(t, hasHappyPath[mt],
			"reindex.ReindexMigrationType %q has no happy-path case in enumerationCases(); "+
				"every type the production code can dispatch must be exercised here",
			mt)
	}
}

// TestCreateReindexTasks_AllKnownTypesDispatched calls createReindexTasks
// for each enumerationCase and asserts the documented outcome. This is
// the load-bearing structural pin: a new reindex.ReindexMigrationType constant
// without a matching `case` in the dispatch switch produces "unknown
// migration type %q" — which this test would catch immediately for the
// new type (added to allKnownMigrationTypes), since the corresponding
// enumerationCase would fail with the unknown-type error string.
func TestCreateReindexTasks_AllKnownTypesDispatched(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tmpLsmPath := t.TempDir() // empty dir → all generations resolve to 1

	// Build a minimal provider literal. createReindexTasks references
	// p.logger and p.schemaManager only; nil schemaManager is OK
	// because propertyHasFilterableBucket falls back to true when nil
	// (and we never actually run the tasks, only construct them).
	p := &reindex.ReindexProvider{
		Logger:        logger,
		SchemaManager: nil, // defensive default = treat as "filterable bucket exists"
	}

	for _, c := range enumerationCases() {
		// Name encodes both type and the err substring (if any) so the
		// test output distinguishes the happy-path case from each
		// validation case for the same type.
		name := string(c.mt)
		if c.wantErrSubst != "" {
			name += "_err=" + shortenErr(c.wantErrSubst)
		}
		t.Run(name, func(t *testing.T) {
			// Inject MigrationType so the case structs don't have to set
			// it redundantly — the case row already names the type via
			// c.mt and we want the payload to match.
			payload := *c.payload
			payload.MigrationType = c.mt
			tasks, err := p.CreateReindexTasks(&payload, tmpLsmPath, false)

			if c.wantErrSubst != "" {
				require.Errorf(t, err,
					"migration type %q with payload %+v should have errored", c.mt, c.payload)
				assert.Containsf(t, err.Error(), c.wantErrSubst,
					"error message for %q should contain %q; got %q",
					c.mt, c.wantErrSubst, err.Error())
				assert.Emptyf(t, tasks,
					"migration type %q errored but returned %d tasks; should return no tasks on error",
					c.mt, len(tasks))
				return
			}

			require.NoErrorf(t, err,
				"migration type %q with payload %+v should have produced %d tasks, got error: %v",
				c.mt, c.payload, c.wantNTasks, err)
			assert.Lenf(t, tasks, c.wantNTasks,
				"migration type %q should produce %d tasks, got %d",
				c.mt, c.wantNTasks, len(tasks))
			for i, task := range tasks {
				assert.NotNilf(t, task,
					"migration type %q produced nil task at index %d (production code would NPE on RunOnShard)",
					c.mt, i)
			}
		})
	}
}

// TestCreateReindexTasks_UnknownTypeRejected pins the catch-all behavior:
// an unrecognised reindex.ReindexMigrationType must error explicitly, not
// silently return an empty slice. The catch-all is what protects
// against forgetting a `case` after adding a new type — without this
// rejection, a missed case would dispatch nothing and the unit would
// "complete" with no work done.
func TestCreateReindexTasks_UnknownTypeRejected(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tmpLsmPath := t.TempDir()
	p := &reindex.ReindexProvider{Logger: logger}

	tasks, err := p.CreateReindexTasks(&reindex.ReindexTaskPayload{
		MigrationType: reindex.ReindexMigrationType("definitely-not-a-real-type"),
		Collection:    "MyClass",
		Properties:    []string{"title"},
	}, tmpLsmPath, false)

	require.Error(t, err, "unknown migration type must error")
	assert.Contains(t, err.Error(), "unknown migration type",
		"error should mention 'unknown migration type'")
	assert.Empty(t, tasks, "unknown migration type must not produce tasks")
}

// TestCreateReindexTasks_EmptyPropertiesRejected pins the up-front
// validation: every migration requires at least one property. Without
// this guard, the per-strategy logic would NPE or silently produce
// zero tasks.
func TestCreateReindexTasks_EmptyPropertiesRejected(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tmpLsmPath := t.TempDir()
	p := &reindex.ReindexProvider{Logger: logger}

	for _, mt := range allKnownMigrationTypes() {
		t.Run(string(mt), func(t *testing.T) {
			tasks, err := p.CreateReindexTasks(&reindex.ReindexTaskPayload{
				MigrationType: mt,
				Collection:    "MyClass",
				// Properties: nil — the gate is in createReindexTasks itself.
			}, tmpLsmPath, false)
			require.Errorf(t, err, "%q with empty properties should error", mt)
			assert.Containsf(t, err.Error(), "requires at least one property",
				"%q empty-properties error should mention 'requires at least one property'; got %q",
				mt, err.Error())
			assert.Empty(t, tasks)
		})
	}
}

// TestCreateReindexTasks_TooManyPropertiesRejected pins the defensive
// upper bound: payload.Properties length is capped at
// reindex.MaxReindexPropertiesPerTask (1024). A pathological RAFT replay or
// future internal caller submitting a longer array gets rejected up
// front instead of OOM'ing the per-strategy loop.
func TestCreateReindexTasks_TooManyPropertiesRejected(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tmpLsmPath := t.TempDir()
	p := &reindex.ReindexProvider{Logger: logger}

	tooMany := make([]string, reindex.MaxReindexPropertiesPerTask+1)
	for i := range tooMany {
		tooMany[i] = "p"
	}

	tasks, err := p.CreateReindexTasks(&reindex.ReindexTaskPayload{
		MigrationType: reindex.ReindexTypeRepairFilterable, // arbitrary; the gate is migration-agnostic
		Collection:    "MyClass",
		Properties:    tooMany,
	}, tmpLsmPath, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max is")
	assert.Empty(t, tasks)
}

// shortenErr trims a long error substring to a stable, filesystem-safe
// suffix for use in subtest names.
func shortenErr(s string) string {
	if len(s) > 40 {
		s = s[:40]
	}
	// Replace whitespace with underscore so the subtest name doesn't
	// contain quotes the test runner would have to escape in output.
	out := make([]byte, 0, len(s))
	for _, r := range s {
		switch {
		case r == ' ', r == '\t':
			out = append(out, '_')
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			out = append(out, byte(r))
		default:
			// drop punctuation
		}
	}
	return string(out)
}
