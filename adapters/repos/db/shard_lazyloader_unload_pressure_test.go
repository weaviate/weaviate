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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestLazyLoadShard_TryUnloadUnderResourcePressure(t *testing.T) {
	tests := []struct {
		name   string
		mode   UnloadMode
		diskRO bool
		want   UnloadOutcome
	}{
		{name: "disk read-only refuses a make-room unload", mode: UnloadModeRequest, diskRO: true, want: UnloadOutcomeResourceReadOnly},
		{name: "disk read-only refuses a background unload", mode: UnloadModeBackground, diskRO: true, want: UnloadOutcomeResourceReadOnly},
		{name: "resource read-only without disk pressure still unloads, which frees memory", mode: UnloadModeRequest, want: UnloadOutcomeOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			s := f.lazy.currentShard()
			require.NoError(t, s.SetStatusReadonly(statusReasonResourcePressure))
			f.index.db.resourceScanState.diskReadOnly.Store(tt.diskRO)

			got := tryUnloadPromptly(t, f.lazy, UnloadRequest{Mode: tt.mode, Reindex: noReindex})
			require.Equal(t, tt.want, got)
			if tt.want != UnloadOutcomeOK {
				require.NotNil(t, f.lazy.currentShard(), "a refused unload keeps the shard loaded")
				require.Equal(t, storagestate.StatusReadOnly, f.lazy.GetStatus())
				f.index.db.resourceScanState.diskReadOnly.Store(false)
				require.Equal(t, UnloadOutcomeOK, tryUnloadPromptly(t, f.lazy, UnloadRequest{Mode: tt.mode, Reindex: noReindex}),
					"the refusal lifts once the disk recovers")
			}
		})
	}
}

func TestDiskReadOnlyFlagIsRefreshedEveryScan(t *testing.T) {
	type scan struct {
		memUsed int64
		disk    diskUse
		want    bool
	}
	diskUsed := func(percent uint64) diskUse {
		return diskUse{total: 100, free: 100 - percent, avail: 100 - percent}
	}
	const high, low = 95, 10
	tests := []struct {
		name          string
		diskThreshold uint64
		scans         []scan
		wantReadOnly  bool
		wantUnload    UnloadOutcome
	}{
		{
			name:          "disk fills while memory holds the node read-only",
			diskThreshold: 90,
			scans: []scan{
				{memUsed: high, disk: diskUsed(50), want: false},
				{memUsed: high, disk: diskUsed(95), want: true},
			},
			wantReadOnly: true,
			wantUnload:   UnloadOutcomeResourceReadOnly,
		},
		{
			name:          "disk recovers while memory stays high",
			diskThreshold: 90,
			scans: []scan{
				{memUsed: high, disk: diskUsed(95), want: true},
				{memUsed: high, disk: diskUsed(50), want: false},
			},
			wantReadOnly: true,
			wantUnload:   UnloadOutcomeOK,
		},
		{
			name:          "memory recovers while the disk stays full",
			diskThreshold: 90,
			scans: []scan{
				{memUsed: high, disk: diskUsed(95), want: true},
				{memUsed: low, disk: diskUsed(95), want: true},
			},
			wantReadOnly: true,
			wantUnload:   UnloadOutcomeResourceReadOnly,
		},
		{
			name:          "disk only, full",
			diskThreshold: 90,
			scans:         []scan{{memUsed: low, disk: diskUsed(95), want: true}},
			wantReadOnly:  true,
			wantUnload:    UnloadOutcomeResourceReadOnly,
		},
		{
			name:          "disk only, recovered",
			diskThreshold: 90,
			scans: []scan{
				{memUsed: low, disk: diskUsed(95), want: true},
				{memUsed: low, disk: diskUsed(50), want: false},
			},
			wantUnload: UnloadOutcomeOK,
		},
		{
			name:          "neither",
			diskThreshold: 90,
			scans:         []scan{{memUsed: low, disk: diskUsed(50), want: false}},
			wantUnload:    UnloadOutcomeOK,
		},
		{
			name:       "threshold 0 is off",
			scans:      []scan{{memUsed: low, disk: diskUsed(99), want: false}},
			wantUnload: UnloadOutcomeOK,
		},
		{
			name:          "a failed disk read keeps the last reading",
			diskThreshold: 90,
			scans: []scan{
				{memUsed: high, disk: diskUsed(95), want: true},
				{memUsed: high, disk: diskUse{}, want: true},
			},
			wantReadOnly: true,
			wantUnload:   UnloadOutcomeResourceReadOnly,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t, func(c *Config) {
				c.ResourceUsage.DiskUse.ReadOnlyPercentage = tt.diskThreshold
				c.ResourceUsage.MemUse.ReadOnlyPercentage = 90
			})
			f.put(t)
			db := f.index.db
			var used atomic.Int64
			mon := memwatch.NewMonitor(used.Load, func(int64) int64 { return 100 }, 1.0)

			for i, s := range tt.scans {
				used.Store(s.memUsed)
				db.scanResourceUsageOnce(mon, s.disk, time.Now())
				require.Equal(t, s.want, db.diskPressureReadOnly(), "scan %d", i)
			}

			require.Equal(t, tt.wantReadOnly, db.resourcePressureReadOnly())
			require.Equal(t, tt.wantUnload, tryUnloadPromptly(t, f.lazy, UnloadRequest{Reindex: noReindex}))
		})
	}
}
