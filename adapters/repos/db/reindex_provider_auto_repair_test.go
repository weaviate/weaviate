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
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestDispatchAutoRepair_* pin the auto-repair half of
// weaviate/0-weaviate-issues#221: on a FAILED semantic migration,
// OnTaskCompleted's FAILED path must not merely LOG the repair command
// (weaviate/weaviate#11982 predecessor behavior) but auto-DISPATCH the
// idempotent repair-* migration that rebuilds the torn inverted bucket.
//
// The dispatch is gated OFF until weaviate/weaviate#11982
// (0-weaviate-issues#222) merges, so these tests pin BOTH sides of the gate:
//   - gate CLOSED (default) → no dispatch (the #222-safe status quo);
//   - gate OPEN → the correct repair-* migration(s) are submitted for the
//     affected (collection, properties).

type recordedRepairDispatch struct {
	collection    string
	properties    []string
	migrationType ReindexMigrationType
}

// newAutoRepairProviderCapturing builds a bare provider whose dispatcher
// records every submission into the returned slice pointer. gate controls
// [ReindexProvider.autoRepairEnabled].
func newAutoRepairProviderCapturing(gate bool) (*ReindexProvider, *[]recordedRepairDispatch) {
	var captured []recordedRepairDispatch
	p := &ReindexProvider{
		autoRepairEnabled: func() bool { return gate },
		repairDispatch: func(_ context.Context, collection string, properties []string, mt ReindexMigrationType) error {
			captured = append(captured, recordedRepairDispatch{
				collection:    collection,
				properties:    append([]string(nil), properties...),
				migrationType: mt,
			})
			return nil
		},
	}
	return p, &captured
}

// TestDispatchAutoRepair_ChangeTokenizationDispatchesFilterableRepair is the
// green-with assertion: a FAILED change-tokenization task auto-dispatches
// repair-filterable for the affected property (the #218 torn bucket). The
// searchable side stays on the retained log guidance because the conflict rule
// forbids a concurrent second repair on the same property — see
// primaryRepairForFailedMigration.
func TestDispatchAutoRepair_ChangeTokenizationDispatchesFilterableRepair(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	p, captured := newAutoRepairProviderCapturing(true)

	payload := &ReindexTaskPayload{
		Collection:         "Products",
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{"name"},
		TargetTokenization: "word",
	}
	p.dispatchAutoRepairOnFailedSemanticMigration(context.Background(), payload, logger.WithField("taskID", "T1"))

	require.Len(t, *captured, 1, "exactly one repair may be dispatched (conflict rule forbids concurrent same-property repairs)")
	require.Equal(t, "Products", (*captured)[0].collection)
	require.Equal(t, []string{"name"}, (*captured)[0].properties)
	require.Equal(t, ReindexTypeRepairFilterable, (*captured)[0].migrationType)
}

// TestDispatchAutoRepair_PerMigrationTypeScope pins the single primary repair
// each semantic migration dispatches: filterable-if-present, else searchable.
// Single-index migrations are fully repaired; change-tokenization repairs the
// filterable side (the rest is documented on primaryRepairForFailedMigration).
func TestDispatchAutoRepair_PerMigrationTypeScope(t *testing.T) {
	cases := []struct {
		name string
		mt   ReindexMigrationType
		want ReindexMigrationType
	}{
		{"change-tokenization", ReindexTypeChangeTokenization, ReindexTypeRepairFilterable},
		{"change-tokenization-filterable", ReindexTypeChangeTokenizationFilterable, ReindexTypeRepairFilterable},
		{"enable-filterable", ReindexTypeEnableFilterable, ReindexTypeRepairFilterable},
		{"enable-searchable", ReindexTypeEnableSearchable, ReindexTypeRebuildSearchable},
		{"change-algorithm", ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			p, captured := newAutoRepairProviderCapturing(true)
			payload := &ReindexTaskPayload{
				Collection:         "Products",
				MigrationType:      tc.mt,
				Properties:         []string{"p"},
				TargetTokenization: "word",
			}
			p.dispatchAutoRepairOnFailedSemanticMigration(context.Background(), payload, logger.WithField("taskID", "T"))

			require.Len(t, *captured, 1)
			require.Equal(t, tc.want, (*captured)[0].migrationType)
		})
	}
}

// TestDispatchAutoRepair_GateClosedIsLogOnly is the red pinning assertion for
// the #11982/#222 sequencing: with the gate CLOSED (the default) a FAILED
// semantic migration must NOT dispatch any repair, even with a dispatcher
// wired. Auto-dispatching repair-filterable before #11982's preserve-set fix
// feeds the #222 data-loss hazard; this test fails loudly if a future change
// flips the default open.
func TestDispatchAutoRepair_GateClosedIsLogOnly(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	p, captured := newAutoRepairProviderCapturing(false)

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	}
	p.dispatchAutoRepairOnFailedSemanticMigration(context.Background(), payload, logger.WithField("taskID", "T"))

	require.Empty(t, *captured, "gate closed → auto-repair must stay log-only (no dispatch)")
}

// TestDispatchAutoRepair_NilGateIsLogOnly pins that a provider with no gate
// wired (the zero value on nodes/tests that never call SetAutoRepairDispatcher)
// treats auto-repair as disabled rather than panicking.
func TestDispatchAutoRepair_NilGateIsLogOnly(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	var dispatched bool
	p := &ReindexProvider{
		repairDispatch: func(context.Context, string, []string, ReindexMigrationType) error {
			dispatched = true
			return nil
		},
	}
	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	}
	p.dispatchAutoRepairOnFailedSemanticMigration(context.Background(), payload, logger.WithField("taskID", "T"))
	require.False(t, dispatched, "nil gate → auto-repair disabled")
}

// TestDispatchAutoRepair_EnabledButNoDispatcherWarns pins that enabling the
// gate without a dispatcher (misconfiguration) degrades to log-only rather
// than panicking on a nil func call.
func TestDispatchAutoRepair_EnabledButNoDispatcherWarns(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	p := &ReindexProvider{autoRepairEnabled: func() bool { return true }}

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	}
	require.NotPanics(t, func() {
		p.dispatchAutoRepairOnFailedSemanticMigration(context.Background(), payload, logger.WithField("taskID", "T"))
	})
	require.Len(t, hook.Entries, 1)
	require.Equal(t, logrus.WarnLevel, hook.Entries[0].Level)
}

// TestDispatchAutoRepair_EmptyPropertiesNoDispatch pins that the reserved
// whole-collection form (empty Properties) does not dispatch a targeted
// repair — there is no property to scope it to.
func TestDispatchAutoRepair_EmptyPropertiesNoDispatch(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	p, captured := newAutoRepairProviderCapturing(true)

	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    nil,
	}
	p.dispatchAutoRepairOnFailedSemanticMigration(context.Background(), payload, logger.WithField("taskID", "T"))
	require.Empty(t, *captured)
}

// TestDispatchAutoRepair_FormatOnlyMigrationNoDispatch pins non-recursion: a
// dispatched repair-* migration is itself format-only (non-semantic), so if it
// FAILs its own FAILED path must not re-dispatch — otherwise auto-repair loops.
func TestDispatchAutoRepair_FormatOnlyMigrationNoDispatch(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	for _, mt := range []ReindexMigrationType{
		ReindexTypeRepairFilterable,
		ReindexTypeRebuildSearchable,
		ReindexTypeRepairRangeable,
		ReindexTypeEnableRangeable,
	} {
		t.Run(string(mt), func(t *testing.T) {
			p, captured := newAutoRepairProviderCapturing(true)
			payload := &ReindexTaskPayload{
				Collection:    "Products",
				MigrationType: mt,
				Properties:    []string{"name"},
			}
			p.dispatchAutoRepairOnFailedSemanticMigration(context.Background(), payload, logger.WithField("taskID", "T"))
			require.Empty(t, *captured, "format-only migration %s must not auto-dispatch a repair", mt)
		})
	}
}

// TestDispatchAutoRepair_PeerNodeConflictIsBenign pins that a dispatcher
// error (the expected outcome on the losing nodes, where the (collection,
// property) conflict check rejects the duplicate submit) is swallowed with a
// warning rather than panicking or propagating — OnTaskCompleted fires on
// every node, so all but one submit conflicts by design.
func TestDispatchAutoRepair_PeerNodeConflictIsBenign(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	p := &ReindexProvider{
		autoRepairEnabled: func() bool { return true },
		repairDispatch: func(context.Context, string, []string, ReindexMigrationType) error {
			return errors.New("reindex task conflicts: already running repair-filterable for overlapping properties")
		},
	}
	payload := &ReindexTaskPayload{
		Collection:    "Products",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	}
	require.NotPanics(t, func() {
		p.dispatchAutoRepairOnFailedSemanticMigration(context.Background(), payload, logger.WithField("taskID", "T"))
	})
	require.Len(t, hook.Entries, 1)
	require.Equal(t, logrus.WarnLevel, hook.Entries[0].Level)
}

// TestPrimaryRepairForFailedMigration_DerivesFromIndexTypes pins the
// single-source-of-truth contract: the primary repair is derived from
// semanticMigrationIndexTypes (filterable-if-present, else searchable), so a
// new semantic migration that adds an index type is covered without a second
// edit here. Format-only / non-semantic types have no primary repair.
func TestPrimaryRepairForFailedMigration_DerivesFromIndexTypes(t *testing.T) {
	semantic := []ReindexMigrationType{
		ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeChangeAlgorithm,
	}
	for _, mt := range semantic {
		var hasFilterable, hasSearchable bool
		for _, it := range semanticMigrationIndexTypes(mt) {
			switch it {
			case "filterable":
				hasFilterable = true
			case "searchable":
				hasSearchable = true
			}
		}
		var want ReindexMigrationType
		switch {
		case hasFilterable:
			want = ReindexTypeRepairFilterable
		case hasSearchable:
			want = ReindexTypeRebuildSearchable
		}
		got, ok := primaryRepairForFailedMigration(mt)
		require.True(t, ok, "semantic migration %s must have a primary repair", mt)
		require.Equal(t, want, got, "primary repair for %s must match its written index types", mt)
	}

	for _, mt := range []ReindexMigrationType{
		ReindexTypeRepairFilterable,
		ReindexTypeRebuildSearchable,
		ReindexTypeRepairRangeable,
		ReindexTypeEnableRangeable,
	} {
		_, ok := primaryRepairForFailedMigration(mt)
		require.False(t, ok, "format-only migration %s must have no primary repair", mt)
	}
}
