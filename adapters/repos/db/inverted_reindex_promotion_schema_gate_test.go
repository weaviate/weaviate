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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

const promotionGateProp = "score"

// The message the deferral logs; an operator asking why a shard is not promoted
// greps for it.
const promotionGateNotice = "keep their staged name"

type propertyIndexField string

const (
	fieldFilterable propertyIndexField = "IndexFilterable"
	fieldSearchable propertyIndexField = "IndexSearchable"
	fieldRangeable  propertyIndexField = "IndexRangeFilters"
)

// The two flags the strategy does not follow take the opposite value, so a
// promotion reading the wrong one fails here.
func promotionGateClass(field propertyIndexField, flag *bool) *models.Class {
	other := flag == nil || !*flag
	prop := &models.Property{
		Name:              promotionGateProp,
		IndexFilterable:   &other,
		IndexSearchable:   &other,
		IndexRangeFilters: &other,
	}
	switch field {
	case fieldFilterable:
		prop.IndexFilterable = flag
	case fieldSearchable:
		prop.IndexSearchable = flag
	case fieldRangeable:
		prop.IndexRangeFilters = flag
	}
	return &models.Class{Class: "Books", Properties: []*models.Property{prop}}
}

// The load-time sweep deletes the canonical directory whenever the collection
// turns the index off, so promotion waits for exactly what the sweep spares: an
// explicit false defers, unset and true promote, unreadable defers.
func TestPromotionFollowsTheSchemaFlagThatOwnsTheCanonicalName(t *testing.T) {
	off, on := false, true

	schemas := []struct {
		name     string
		class    func(propertyIndexField) *models.Class
		deferred bool
	}{
		{
			name:     "the collection turns the index off",
			class:    func(f propertyIndexField) *models.Class { return promotionGateClass(f, &off) },
			deferred: true,
		},
		{
			name:  "the collection turns the index on",
			class: func(f propertyIndexField) *models.Class { return promotionGateClass(f, &on) },
		},
		{
			name:  "the collection leaves the flag unset",
			class: func(f propertyIndexField) *models.Class { return promotionGateClass(f, nil) },
		},
		{
			// The sweep walks the collection's own properties, so it never reaches
			// this one.
			name: "the collection does not hold the property",
			class: func(propertyIndexField) *models.Class {
				return &models.Class{Class: "Books", Properties: []*models.Property{{Name: "title"}}}
			},
		},
		{
			// Not the row above: the sweep may hold a false this read cannot see.
			name:     "the collection is not in the locally applied schema",
			class:    func(propertyIndexField) *models.Class { return nil },
			deferred: true,
		},
	}

	strategies := []struct {
		code  MigrationStrategyCode
		field propertyIndexField
	}{
		{StrategyCodeSearchableMapToBlockmax, fieldSearchable},
		{StrategyCodeEnableSearchable, fieldSearchable},
		{StrategyCodeRebuildSearchable, fieldSearchable},
		{StrategyCodeSearchableRetokenize, fieldSearchable},
		{StrategyCodeFilterableToRangeable, fieldRangeable},
		{StrategyCodeFilterableRoaringsetRefresh, fieldFilterable},
		{StrategyCodeFilterableRetokenize, fieldFilterable},
		{StrategyCodeEnableFilterable, fieldFilterable},
	}
	require.Len(t, strategies, len(migrationStrategyCodes),
		"every strategy code has to name the flag that owns the directory it promotes onto")

	for _, strategy := range strategies {
		for _, schema := range schemas {
			t.Run(string(strategy.code)+"/"+schema.name, func(t *testing.T) {
				f := newReconcileFixture(t)
				f.class = schema.class(strategy.field)
				subject, staged, canonical := plantPromotableSwappedRecord(f, strategy.code)

				r := f.reconcile()

				require.Zero(t, r.WedgedCount(), "a promotion that waits is not a shard that is stuck")
				require.Empty(t, f.errorLines(""), "and it is not a fault either")

				state, present := f.state(subject.Key)
				require.True(t, present)
				if !schema.deferred {
					require.Equal(t, MigrationStatePromoted, state)
					require.Equal(t, staged, f.contentOf(canonical), "the rebuilt data reaches the canonical name")
					require.False(t, f.exists(staged))
					require.Empty(t, f.linesAt(logrus.InfoLevel, promotionGateNotice))
					return
				}
				require.Equal(t, MigrationStateSwapped, state, "the record stays the answer for this property")
				require.Equal(t, staged, f.contentOf(staged), "the rebuilt data keeps its staged name")
				require.False(t, f.exists(canonical),
					"the next load's sweep deletes a canonical directory under an index the collection turns off")
				require.Empty(t, f.swapped(t, subject.Key).PromotionOf(promotionGateProp),
					"a rename that never ran must not be recorded as one that did")
				require.Len(t, f.linesAt(logrus.InfoLevel, promotionGateNotice), 1,
					"one line per record per load, so an operator can find the wait without it flooding the log")
			})
		}
	}
}

// enable-* migrations run with their flag off throughout, so the first load
// after the cluster-wide flip is what promotes.
func TestPromotionRunsOnTheFirstPassAfterTheFlagLands(t *testing.T) {
	off, on := false, true

	f := newReconcileFixture(t)
	f.class = promotionGateClass(fieldFilterable, &off)
	subject, staged, canonical := plantPromotableSwappedRecord(f, StrategyCodeEnableFilterable)

	require.Zero(t, f.reconcile().WedgedCount())
	state, _ := f.state(subject.Key)
	require.Equal(t, MigrationStateSwapped, state)
	require.False(t, f.exists(canonical))

	f.class = promotionGateClass(fieldFilterable, &on)

	require.Zero(t, f.reconcile().WedgedCount())
	state, _ = f.state(subject.Key)
	require.Equal(t, MigrationStatePromoted, state)
	require.Equal(t, staged, f.contentOf(canonical))
	require.False(t, f.exists(staged))
	require.Empty(t, f.errorLines(""))
}

// The closure sweep re-promotes with a second rename onto the same canonical
// directory, so it follows the same flag. Withholding must also stop the
// reclaim that follows, which would remove the staged directory it spared.
func TestTheClosureSweepFollowsTheSchemaFlagToo(t *testing.T) {
	off, on := false, true

	tests := []struct {
		name     string
		flag     *bool
		deferred bool
	}{
		{name: "the collection turns the index off", flag: &off, deferred: true},
		{name: "the collection turns the index on", flag: &on},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = promotionGateClass(fieldFilterable, test.flag)

			subject := testMigrationSubject(42, StrategyCodeEnableFilterable, promotionGateProp)
			staged := subject.Props[promotionGateProp].Staged
			canonical := subject.Props[promotionGateProp].Canonical
			f.mkdirs(staged)
			f.put(NewMigrationRecordPromoted(subject, []string{promotionGateProp},
				map[string]string{promotionGateProp: canonical}))

			r := f.reconcile()

			require.Zero(t, r.WedgedCount())
			if !test.deferred {
				require.Equal(t, staged, f.contentOf(canonical))
				return
			}
			require.Empty(t, f.errorLines(""), "waiting is not a fault")
			require.False(t, f.exists(canonical),
				"the next load's sweep deletes a canonical directory under an index the collection turns off")
			require.Equal(t, staged, f.contentOf(staged),
				"and the reclaim that follows the sweep must not take the only copy that is left")
			_, present := f.state(subject.Key)
			require.True(t, present, "the record has to survive, or nothing promotes once the flag lands")
			require.Len(t, f.linesAt(logrus.InfoLevel, promotionGateNotice), 1)
		})
	}
}

// The shape every shard is in between its own flip and the next load's
// promotion: staged directory on disk, nothing at the canonical name.
func plantPromotableSwappedRecord(f *reconcileFixture, code MigrationStrategyCode,
) (subject MigrationSubject, staged, canonical string) {
	f.t.Helper()
	subject = testMigrationSubject(42, code, promotionGateProp)
	staged = subject.Props[promotionGateProp].Staged
	canonical = subject.Props[promotionGateProp].Canonical
	f.mkdirs(staged)
	f.put(NewMigrationRecordSwapped(subject, []string{promotionGateProp},
		map[string]string{promotionGateProp: canonical}))
	return subject, staged, canonical
}
