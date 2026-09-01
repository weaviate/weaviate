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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// entriesAbout returns every log entry whose message carries needle.
func entriesAbout(hook *logrustest.Hook, needle string) []*logrus.Entry {
	var out []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, needle) {
			out = append(out, entry)
		}
	}
	return out
}

// linesOf renders entries for a failure message. logrus entries are pointers,
// so a failed count assertion on them prints addresses instead of the lines the
// assertion is about.
func linesOf(entries []*logrus.Entry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = fmt.Sprintf("%s %v", entry.Message, entry.Data)
	}
	return out
}

// TestRecoveryWalkReportsMissingPayloadsOnce pins the startup walk's
// missing-payload reporting to one line for the whole walk. The fault is per
// tracker per shard, so a line at the point of failure follows the tenant count
// at every boot.
func TestRecoveryWalkReportsMissingPayloadsOnce(t *testing.T) {
	const (
		shards     = 12
		trackerDir = "searchable_retokenize_title_1"
	)
	root := t.TempDir()
	indexPath := filepath.Join(root, "books_abc")
	fixtureLogger, _ := logrustest.NewNullLogger()

	for i := 0; i < shards; i++ {
		lsm := filepath.Join(indexPath, fmt.Sprintf("tenant-%02d", i), "lsm")
		// The tracker dir exists but carries no payload.mig, which is the arm
		// that used to warn once per tracker.
		require.NoError(t, os.MkdirAll(filepath.Join(lsm, ".migrations", trackerDir), 0o777))

		subject := testMigrationSubject(uint64(i+1), StrategyCodeSearchableRetokenize, "title")
		subject.TrackerDir = trackerDir
		require.NoError(t, NewMigrationRecordStore(lsm, fixtureLogger).
			Put(NewMigrationRecordMerged(subject)))
	}

	logger, hook := logrustest.NewNullLogger()
	recovered, err := DiscoverInFlightReindexTasks(root, logger, nil)
	require.NoError(t, err)
	require.Empty(t, recovered, "a tracker with no payload recovers nothing")

	about := entriesAbout(hook, "have no readable payload.mig")
	require.Len(t, about, 1,
		"one line for the whole walk, not one per tracker: %v", linesOf(about))
	require.Equal(t, logrus.WarnLevel, about[0].Level)
	require.Contains(t, about[0].Message, fmt.Sprintf("%d migration(s)", shards),
		"the one line carries the count the per-tracker lines used to carry")

	// The line names trackers too, so the names have to be capped or the one
	// line grows with the tenant count instead of the line count.
	names, ok := about[0].Data["trackers"].([]string)
	require.True(t, ok, "the line carries the tracker names it counted")
	require.Len(t, names, maxReportedErrors+1,
		"the capped names plus the one entry that says how many are unaccounted for")
	require.Contains(t, names[len(names)-1], fmt.Sprintf("and %d more", shards-maxReportedErrors))
}

// TestOrphanTrackerStringBoundsItsPropertyList pins that the formatter the
// orphan audit logs with caps its property list. A property list is
// user-chosen and unbounded, and the audit emits one line per orphan on a walk
// over every shard on the node.
func TestOrphanTrackerStringBoundsItsPropertyList(t *testing.T) {
	const props = maxReportedErrors*2 + 5

	names := make([]string, props)
	for i := range names {
		names[i] = fmt.Sprintf("prop_%02d", i)
	}
	beyondCap := names[props-1]

	orphan := &orphanReindexTracker{
		collection:  "Books",
		shardName:   "tenant-00",
		dirName:     "searchable_retokenize_title_1",
		generation:  1,
		taskID:      "Books:change-tokenization:title:ab12",
		taskVersion: 42,
		unitID:      "shard-1__node-0",
		properties:  names,
		indexTypes:  []string{"searchable"},
	}

	line := orphan.String()
	require.Contains(t, line, fmt.Sprintf("property_count=%d", props),
		"the count of properties survives the cap")
	require.NotContains(t, line, beyondCap,
		"a property past the cap must not reach the line")
	require.Contains(t, line, fmt.Sprintf("(and %d more", props-maxReportedErrors),
		"the line says how many names it left out")
}

// TestOverlayConflictReportsManyPropertiesInOneLine pins the overlay-conflict
// warning to one line whatever the property count, with the names on it
// bounded. Both registrations name every property, so a line per property is a
// line per property the user configured.
func TestOverlayConflictReportsManyPropertiesInOneLine(t *testing.T) {
	const props = 50

	names := make([]string, props)
	forced := map[string]inverted.PropertyOverlay{}
	for i := range names {
		names[i] = fmt.Sprintf("prop_%02d", i)
		forced[names[i]] = inverted.PropertyOverlay{ForceSearchable: true}
	}

	logger, hook := logrustest.NewNullLogger()
	s := &Shard{index: &Index{logger: logger}}

	s.registerDoubleWriteWithScope(names, forced, noopMirrorCallbacks)
	require.Empty(t, entriesAbout(hook, "different analyzer overlays"),
		"fixture: one registration cannot conflict with itself")

	s.registerDoubleWriteWithScope(names, nil, noopMirrorCallbacks)

	about := entriesAbout(hook, "different analyzer overlays")
	require.Len(t, about, 1,
		"one line for the transition, not one per conflicting property: %v", linesOf(about))
	require.Equal(t, props, about[0].Data["property_count"],
		"the line names how many properties conflict")

	reported, ok := about[0].Data["props"].([]string)
	require.True(t, ok, "the line carries the property names it counted")
	require.Less(t, len(reported), props,
		"the names on the one line are capped, or the line grows with the property count")
	require.Len(t, reported, maxReportedErrors+1,
		"the capped names plus the one entry that says how many are unaccounted for")
}

// TestUpdatePropertySummaryPrintsWithoutPayloadReads pins that the apply's
// sweep summary is emitted on the common path, where no tracker payload is read
// at all. record_set_reads is the count a once-per-shard regression shows up
// in, and gating the line on payload reads alone hid it.
func TestUpdatePropertySummaryPrintsWithoutPayloadReads(t *testing.T) {
	ctx := testCtx()
	className := "SweepSummaryNoPayloads_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})

	logger, hook := logrustest.NewNullLogger()
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, func(i *Index) { i.logger = logger })
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	// No tracker dir on disk, so the sweep reads no payload but still reads the
	// shard's record set to find out there is nothing to preserve.
	require.NoDirExists(t, filepath.Join(shard.pathLSM(), ".migrations"))

	prop := class.Properties[0]
	off := false
	prop.IndexFilterable = &off
	prop.IndexSearchable = &off
	prop.IndexRangeFilters = &off

	hook.Reset()
	require.NoError(t, idx.updateProperty(ctx, prop))

	about := entriesAbout(hook, "partial-reindex cleanup: migration dirs swept for disabled index types")
	require.Len(t, about, 1, "one summary line for the apply: %v", linesOf(about))
	require.Equal(t, int64(0), about[0].Data["payload_reads"],
		"fixture: no tracker on disk means no payload was read, which is the gate under test")
	require.GreaterOrEqual(t, about[0].Data["record_set_reads"], int64(1),
		"the summary reports the record-set read the sweep still paid")
}
