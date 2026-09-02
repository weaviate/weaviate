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

func TestAFailedRetirementKeepsItsRetry(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	const canonical = "property_title_searchable"
	predecessor := testMigrationSubject(41, StrategyCodeSearchableRetokenize, "title")
	predecessor.TrackerDir = "searchable_retokenize_title_1"
	predecessor.Props = map[string]MigrationPropertyDirs{"title": {
		Staged:    "property_title_searchable__retokenize_ingest_1",
		Sidecar:   "property_title_searchable__retokenize_reindex_1",
		Canonical: canonical,
	}}

	successor := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	successor.TrackerDir = "searchable_retokenize_title_2"
	successor.Props = map[string]MigrationPropertyDirs{"title": {
		Staged:    "property_title_searchable__retokenize_ingest_2",
		Sidecar:   "property_title_searchable__retokenize_reindex_2",
		Canonical: canonical,
	}}

	f.mkdirs(predecessor.Props["title"].Staged, successor.Props["title"].Staged, canonical)
	f.put(NewMigrationRecordSwapped(predecessor, []string{"title"},
		map[string]string{"title": canonical}))
	f.put(NewMigrationRecordSwapped(successor, []string{"title"},
		map[string]string{"title": canonical}))

	require.NoError(t, os.Chmod(f.lsmPath, 0o555))
	if err := os.RemoveAll(f.lsmPath + "/" + predecessor.Props["title"].Staged); err == nil {
		os.Chmod(f.lsmPath, 0o755)
		t.Skip("this user can remove a directory from a read-only parent, so the fault cannot be staged")
	}
	f.reconcile()
	require.NoError(t, os.Chmod(f.lsmPath, 0o755))

	state, present := f.state(predecessor.Key)
	require.True(t, present, "the record whose retirement failed is the only thing that attributes its directory")
	require.Equal(t, MigrationStateSwapped, state,
		"Promoted asserts the staged directory is gone; the removal failed, so it is not")
	require.True(t, f.exists(predecessor.Props["title"].Staged),
		"fixture: the removal really did fail, or there is nothing to retry")

	f.reconcile()
	_, stillThere := f.state(predecessor.Key)
	require.False(t, stillThere, "the retry the contract promises has to actually happen")
	require.False(t, f.exists(predecessor.Props["title"].Staged))

	require.True(t, f.exists(canonical))
	require.Equal(t, successor.Props["title"].Staged, f.contentOf(canonical),
		"the successor's promotion is what puts data at the canonical name")
}

func TestARepromotionSkipsWhatRetirementOwns(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	const canonical = "property_title_searchable"
	predecessor := testMigrationSubject(41, StrategyCodeSearchableRetokenize, "title")
	predecessor.TrackerDir = "searchable_retokenize_title_1"
	predecessor.Props = map[string]MigrationPropertyDirs{"title": {
		Staged:    "property_title_searchable__retokenize_ingest_1",
		Sidecar:   "property_title_searchable__retokenize_reindex_1",
		Canonical: canonical,
	}}

	successor := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	successor.TrackerDir = "searchable_retokenize_title_2"
	successor.Props = map[string]MigrationPropertyDirs{"title": {
		Staged:    "property_title_searchable__retokenize_ingest_2",
		Sidecar:   "property_title_searchable__retokenize_reindex_2",
		Canonical: canonical,
	}}

	f.mkdirs(predecessor.Props["title"].Staged)
	f.put(NewMigrationRecordPromoted(predecessor, []string{"title"},
		map[string]string{"title": canonical}))
	f.put(NewMigrationRecordSwapped(successor, []string{"title"},
		map[string]string{"title": canonical}))

	all := f.store.Records()
	require.True(t, migrationPropertySuperseded(all, predecessor, "title"),
		"fixture: the successor has to supersede, or this reader is not the one under test")
	require.False(t, migrationDirClaimedAsDisplaced(all, predecessor, predecessor.Props["title"].Staged),
		"fixture: the displaced claim has to be absent, or the narrower check would already answer")

	r := newMigrationReconciler(f.store, f.lsmPath, f.logger, f.deps())
	require.NoError(t, r.repromoteWhatTheRecordOutran(testCtx(), all, predecessor))

	require.Equal(t, predecessor.Props["title"].Staged, f.contentOf(predecessor.Props["title"].Staged),
		"the predecessor's rebuild is retirement's to reclaim, not this reader's to promote")
	require.False(t, f.exists(canonical),
		"renaming the predecessor's rebuild onto the canonical name puts the old tokenization where the successor's belongs")
}
