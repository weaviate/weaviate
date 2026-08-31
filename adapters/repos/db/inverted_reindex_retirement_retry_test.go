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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// TestAFailedRetirementKeepsItsRetry pins the contract retireOneSealed writes
// down and promoteSealed used to take away forty lines later: a directory whose
// removal failed must keep the record naming it, so the next load retries.
//
// Superseded says retirement OWNS a property, not that it has RUN. Promoted
// asserts a disk shape, so writing it while the staged directory is still there
// leaves the record lying about the disk — and once it is written the successor
// completes, supersession lapses, and nothing on any code path retires the
// predecessor again.
//
// The fault is an ordinary one: any EACCES, EIO or read-only remount fails the
// removal with every seal granted.
func TestAFailedRetirementKeepsItsRetry(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	const canonical = "property_title_searchable"
	predecessor := testMigrationSubject(41, StrategyCodeSearchableRetokenize, "title")
	predecessor.TrackerDir = "searchable_retokenize_title_1"
	predecessor.StagedDirs = map[string]string{"title": "property_title_searchable__retokenize_ingest_1"}
	predecessor.SidecarDirs = map[string]string{"title": "property_title_searchable__retokenize_reindex_1"}
	predecessor.CanonicalDirs = map[string]string{"title": canonical}

	successor := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	successor.TrackerDir = "searchable_retokenize_title_2"
	successor.StagedDirs = map[string]string{"title": "property_title_searchable__retokenize_ingest_2"}
	successor.SidecarDirs = map[string]string{"title": "property_title_searchable__retokenize_reindex_2"}
	successor.CanonicalDirs = map[string]string{"title": canonical}

	f.mkdirs(predecessor.StagedDirs["title"], successor.StagedDirs["title"], canonical)
	f.put(NewMigrationRecordSwapped(predecessor, []string{"title"},
		map[string]string{"title": canonical}))
	f.put(NewMigrationRecordSwapped(successor, []string{"title"},
		map[string]string{"title": canonical}))

	// One pass in which the removal cannot run. Record files live one level
	// down, so writing them still works — this fails exactly the one step the
	// contract is about.
	require.NoError(t, os.Chmod(f.lsmPath, 0o555))
	if err := os.RemoveAll(f.lsmPath + "/" + predecessor.StagedDirs["title"]); err == nil {
		os.Chmod(f.lsmPath, 0o755)
		t.Skip("this user can remove a directory from a read-only parent, so the fault cannot be staged")
	}
	f.reconcile()
	require.NoError(t, os.Chmod(f.lsmPath, 0o755))

	state, present := f.state(predecessor.Key)
	require.True(t, present, "the record whose retirement failed is the only thing that attributes its directory")
	require.Equal(t, MigrationStateSwapped, state,
		"Promoted asserts the staged directory is gone; the removal failed, so it is not")
	require.True(t, f.exists(predecessor.StagedDirs["title"]),
		"fixture: the removal really did fail, or there is nothing to retry")

	// The very next pass, with no fault at all, retires it.
	f.reconcile()
	_, stillThere := f.state(predecessor.Key)
	require.False(t, stillThere, "the retry the contract promises has to actually happen")
	require.False(t, f.exists(predecessor.StagedDirs["title"]))

	// And the successor's own rebuild is what the property serves.
	require.True(t, f.exists(canonical))
	require.Equal(t, successor.StagedDirs["title"], f.contentOf(canonical),
		"the successor's promotion is what puts data at the canonical name")
}

// TestARepromotionSkipsWhatRetirementOwns pins the second reader of the same
// question. repromoteWhatTheRecordOutran skipped only a staged directory a
// successor claims as displaced, and that claim is strictly narrower than
// supersession: a successor that flipped from the canonical name — the ordinary
// post-restart shape — records displaced == canonical, so the claim is absent
// while supersession still holds. The rename then puts the predecessor's
// rebuild, built against the old tokenization, over the successor's.
func TestARepromotionSkipsWhatRetirementOwns(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	const canonical = "property_title_searchable"
	predecessor := testMigrationSubject(41, StrategyCodeSearchableRetokenize, "title")
	predecessor.TrackerDir = "searchable_retokenize_title_1"
	predecessor.StagedDirs = map[string]string{"title": "property_title_searchable__retokenize_ingest_1"}
	predecessor.SidecarDirs = map[string]string{"title": "property_title_searchable__retokenize_reindex_1"}
	predecessor.CanonicalDirs = map[string]string{"title": canonical}

	successor := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	successor.TrackerDir = "searchable_retokenize_title_2"
	successor.StagedDirs = map[string]string{"title": "property_title_searchable__retokenize_ingest_2"}
	successor.SidecarDirs = map[string]string{"title": "property_title_searchable__retokenize_reindex_2"}
	successor.CanonicalDirs = map[string]string{"title": canonical}

	// Only the predecessor's staged directory exists: the shape that makes the
	// second reader want to rename it onto the canonical name. The successor
	// flipped from the canonical name, which is the ordinary post-restart
	// shape and the one whose displaced claim is absent.
	f.mkdirs(predecessor.StagedDirs["title"])
	f.put(NewMigrationRecordPromoted(predecessor, []string{"title"},
		map[string]string{"title": canonical}))
	f.put(NewMigrationRecordSwapped(successor, []string{"title"},
		map[string]string{"title": canonical}))

	all := f.store.Records()
	require.True(t, migrationPropertySuperseded(all, predecessor, "title"),
		"fixture: the successor has to supersede, or this reader is not the one under test")
	require.False(t, migrationDirClaimedAsDisplaced(all, predecessor, predecessor.StagedDirs["title"]),
		"fixture: the displaced claim has to be absent, or the narrower check would already answer")

	// Driven directly: retirement and this reader take the same seal, so no
	// single pass reaches this reader with the record still standing.
	r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
	require.NoError(t, r.repromoteWhatTheRecordOutran(all, predecessor))

	require.Equal(t, predecessor.StagedDirs["title"], f.contentOf(predecessor.StagedDirs["title"]),
		"the predecessor's rebuild is retirement's to reclaim, not this reader's to promote")
	require.False(t, f.exists(canonical),
		"renaming the predecessor's rebuild onto the canonical name puts the old tokenization where the successor's belongs")
}
