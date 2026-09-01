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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// The fault entry names a file and a scope, never a strategy, so the only
// thing that says which migration a bad record covered is its file name.
// Latching on every fault turns rangeable readiness off shard-wide for a bad
// record of a migration that has nothing to do with rangeable indexes.
func TestOnlyAFaultThatCouldBeARangeableRecordTurnsRangeableOff(t *testing.T) {
	tests := []struct {
		name  string
		fault MigrationRecordUnreadable
		want  bool
	}{
		{
			name:  "a store-scope fault read no file at all",
			fault: MigrationRecordUnreadable{Reason: "records dir unreadable", Scope: MigrationRecordFaultStore},
			want:  true,
		},
		{
			name:  "the rangeable migration's own record",
			fault: MigrationRecordUnreadable{FileName: "7_filterable_to_rangeable_shard-1__node-0.json"},
			want:  true,
		},
		{
			name:  "a change-tokenization record",
			fault: MigrationRecordUnreadable{FileName: "7_searchable_retokenize_shard-1__node-0.json"},
		},
		{
			name:  "a rebuild-searchable record",
			fault: MigrationRecordUnreadable{FileName: "12_rebuild_searchable_shard-1__node-0.json"},
		},
		{
			name:  "a name this build cannot take apart",
			fault: MigrationRecordUnreadable{FileName: "junk.json"},
			want:  true,
		},
		{
			name:  "a name whose strategy this build does not know",
			fault: MigrationRecordUnreadable{FileName: "7_some_future_strategy_shard-1__node-0.json"},
			want:  true,
		},
		{
			name:  "a name with no task version",
			fault: MigrationRecordUnreadable{FileName: "filterable_retokenize_shard-1__node-0.json"},
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want,
				migrationFaultCouldHideARangeableRecord([]MigrationRecordUnreadable{tt.fault}))
		})
	}
}

// One fault of any kind decides the shard, so a set is only as safe as its
// least readable member.
func TestOneUnreadableRangeableRecordDecidesAWholeFaultSet(t *testing.T) {
	require.False(t, migrationFaultCouldHideARangeableRecord(nil))
	require.True(t, migrationFaultCouldHideARangeableRecord([]MigrationRecordUnreadable{
		{FileName: "7_searchable_retokenize_shard-1__node-0.json"},
		{FileName: "7_filterable_to_rangeable_shard-1__node-0.json"},
	}))
}

// The name the latch reads back is the one the store writes; a rename on
// either side would make every fault read as some other strategy.
func TestARecordFileNameRoundTripsToItsStrategy(t *testing.T) {
	for _, code := range migrationStrategyCodes {
		key := MigrationRecordKey{TaskVersion: 7, StrategyCode: code, UnitID: "shard-1__node-0"}
		read, ok := migrationStrategyCodeOfRecordFile(key.fileName())
		require.True(t, ok, key.fileName())
		require.Equal(t, code, read)
	}
}

// Shard init is what latches, and the latch has no writer of false anywhere,
// so a record of an unrelated migration would turn every rangeable property on
// the shard off for as long as the file is there.
func TestShardInitLatchesOnlyOnAFaultThatCouldBeRangeable(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		want     bool
	}{
		{
			name:     "an unreadable change-tokenization record",
			fileName: "7_searchable_retokenize_shard-1__node-0.json",
		},
		{
			name:     "an unreadable rangeable record",
			fileName: "7_filterable_to_rangeable_shard-1__node-0.json",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			lsmPath := t.TempDir()
			store := NewMigrationRecordStore(lsmPath, logger)
			require.NoError(t, os.MkdirAll(store.Dir(), 0o777))
			require.NoError(t, os.WriteFile(
				filepath.Join(store.Dir(), tt.fileName), []byte("{"), 0o600))
			require.NoError(t, store.Load())
			require.Len(t, store.Unreadable(), 1, "the planted file is the fault under test")

			shard := &Shard{migrationRecords: store}
			markInFlightRangeableMigrationsNotReady(shard)

			require.Equal(t, tt.want, shard.rangeableUndecidable.Load())
		})
	}
}
