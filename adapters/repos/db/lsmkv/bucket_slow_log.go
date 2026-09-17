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

package lsmkv

import (
	"context"
	"sort"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// Keys for per-lookup slow-log entries, split by whether the entry carries the
// view cost. A lookup that takes its own view per key records under
// SlowLogKeyGetBySecondary and puts that cost in View. A lookup that resolves
// many keys under one view records under SlowLogKeyGetBySecondaryWithView with
// View zero, so the view cost is not in these percentiles. Callers that cannot
// tell which path ran reduce both.
const (
	SlowLogKeyGetBySecondary         = "lsm_get_by_secondary"
	SlowLogKeyGetBySecondaryWithView = "lsm_get_by_secondary_with_view"
)

// ReduceSlowLogEntries replaces the per-lookup slow-log entries under key with
// their stats, so a slow query logs one fixed-size summary instead of one
// entry per key.
func ReduceSlowLogEntries(ctx context.Context, key string) {
	helpers.ReplaceSlowQueryEntry(ctx, key, func(old []BucketSlowLogEntry) BucketSlowLogEntryStats {
		return BucketSlowLogEntries(old).Reduce()
	})
}

type BucketSlowLogEntry struct {
	Total            time.Duration
	View             time.Duration
	ActiveMemtable   time.Duration
	FlushingMemtable time.Duration
	Segments         time.Duration
	Recheck          time.Duration // only for secondary index reads
}

type BucketSlowLogEntries []BucketSlowLogEntry

func (b BucketSlowLogEntries) Reduce() BucketSlowLogEntryStats {
	if len(b) == 0 {
		return BucketSlowLogEntryStats{}
	}

	var totalDurations, viewDurations, activeMemtableDurations,
		flushingMemtableDurations, segmentsDurations, recheckDurations []time.Duration

	for _, entry := range b {
		totalDurations = append(totalDurations, entry.Total)
		viewDurations = append(viewDurations, entry.View)
		activeMemtableDurations = append(activeMemtableDurations, entry.ActiveMemtable)
		flushingMemtableDurations = append(flushingMemtableDurations, entry.FlushingMemtable)
		segmentsDurations = append(segmentsDurations, entry.Segments)
		recheckDurations = append(recheckDurations, entry.Recheck)
	}

	return BucketSlowLogEntryStats{
		Total:            reduceDurationStats(totalDurations),
		View:             reduceDurationStats(viewDurations),
		ActiveMemtable:   reduceDurationStats(activeMemtableDurations),
		FlushingMemtable: reduceDurationStats(flushingMemtableDurations),
		Segments:         reduceDurationStats(segmentsDurations),
		Recheck:          reduceDurationStats(recheckDurations),
	}
}

type BucketSlowLogEntryStats struct {
	Total            DurationStats `json:"total"`
	View             DurationStats `json:"view"`
	ActiveMemtable   DurationStats `json:"activeMemtable"`
	FlushingMemtable DurationStats `json:"flushingMemtable"`
	Segments         DurationStats `json:"segments"`
	Recheck          DurationStats `json:"recheck"`
}

type DurationStats struct {
	Mean time.Duration `json:"mean"`
	Min  time.Duration `json:"min"`
	Max  time.Duration `json:"max"`
	P50  time.Duration `json:"p50"`
	P90  time.Duration `json:"p90"`
	P95  time.Duration `json:"p95"`
	P99  time.Duration `json:"p99"`
}

func reduceDurationStats(durations []time.Duration) DurationStats {
	if len(durations) == 0 {
		return DurationStats{}
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	total := time.Duration(0)
	for _, d := range durations {
		total += d
	}

	mean := total / time.Duration(len(durations))
	min := durations[0]
	max := durations[len(durations)-1]

	p50 := durations[len(durations)/2]
	p90 := durations[(len(durations)*90)/100]
	p95 := durations[(len(durations)*95)/100]
	p99 := durations[(len(durations)*99)/100]

	return DurationStats{
		Mean: mean,
		Min:  min,
		Max:  max,
		P50:  p50,
		P90:  p90,
		P95:  p95,
		P99:  p99,
	}
}
