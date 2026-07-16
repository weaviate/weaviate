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
	"sort"
	"time"
)

type BucketSlowLogEntry struct {
	Total            time.Duration
	View             time.Duration
	ActiveMemtable   time.Duration
	FlushingMemtable time.Duration
	Segments         time.Duration
	Recheck          time.Duration // only for secondary index reads

	// The fields below are populated only by the batched secondary resolver
	// (GetBySecondaryBatch, log key lsm_get_by_secondary_batch). They split the
	// opaque Segments timing into the per-phase timings the design calls for so an
	// engineer reading the slow log can see WHERE cold cost sits: index_descents
	// dominant -> cold index pages; value_reads dominant -> the device/semaphore.
	IndexDescents time.Duration // phase 1: per-segment sorted index descents
	ValueReads    time.Duration // phase 2: bounded-concurrent offset-sorted value reads
	// ConcurrentBatchCount is the number of secondary batches concurrently
	// resolving on this node at the moment this batch was counted. It is the
	// leading indicator for adding a node-wide read-concurrency cap: the batch
	// resolver removes the serial-chain slowness that today throttles how many
	// cold queries a node accepts at once, so sustained concurrency climbs as the
	// speedup lands. Sustained values well above the modeled range are the signal
	// to bound node-wide concurrency before device queue latency rises.
	ConcurrentBatchCount int64
	// ArenaBytes is the size in bytes of the single per-batch value-read arena
	// (phase 2): the sum of the value sizes of every live hit read in this batch.
	// Surfaced so the memory cost of a batch is visible in the slow log rather
	// than only in benchmarks; a per-500-key batch is bounded to roughly a few MB.
	ArenaBytes int64
}

type BucketSlowLogEntries []BucketSlowLogEntry

func (b BucketSlowLogEntries) Reduce() BucketSlowLogEntryStats {
	if len(b) == 0 {
		return BucketSlowLogEntryStats{}
	}

	var totalDurations, viewDurations, activeMemtableDurations,
		flushingMemtableDurations, segmentsDurations, recheckDurations,
		indexDescentsDurations, valueReadsDurations []time.Duration
	var concurrentBatchCountMax, arenaBytesMax int64

	for _, entry := range b {
		totalDurations = append(totalDurations, entry.Total)
		viewDurations = append(viewDurations, entry.View)
		activeMemtableDurations = append(activeMemtableDurations, entry.ActiveMemtable)
		flushingMemtableDurations = append(flushingMemtableDurations, entry.FlushingMemtable)
		segmentsDurations = append(segmentsDurations, entry.Segments)
		recheckDurations = append(recheckDurations, entry.Recheck)
		indexDescentsDurations = append(indexDescentsDurations, entry.IndexDescents)
		valueReadsDurations = append(valueReadsDurations, entry.ValueReads)
		if entry.ConcurrentBatchCount > concurrentBatchCountMax {
			concurrentBatchCountMax = entry.ConcurrentBatchCount
		}
		if entry.ArenaBytes > arenaBytesMax {
			arenaBytesMax = entry.ArenaBytes
		}
	}

	return BucketSlowLogEntryStats{
		Total:                   reduceDurationStats(totalDurations),
		View:                    reduceDurationStats(viewDurations),
		ActiveMemtable:          reduceDurationStats(activeMemtableDurations),
		FlushingMemtable:        reduceDurationStats(flushingMemtableDurations),
		Segments:                reduceDurationStats(segmentsDurations),
		Recheck:                 reduceDurationStats(recheckDurations),
		IndexDescents:           reduceDurationStats(indexDescentsDurations),
		ValueReads:              reduceDurationStats(valueReadsDurations),
		ConcurrentBatchCountMax: concurrentBatchCountMax,
		ArenaBytesMax:           arenaBytesMax,
	}
}

type BucketSlowLogEntryStats struct {
	Total                   DurationStats `json:"total"`
	View                    DurationStats `json:"view"`
	ActiveMemtable          DurationStats `json:"activeMemtable"`
	FlushingMemtable        DurationStats `json:"flushingMemtable"`
	Segments                DurationStats `json:"segments"`
	Recheck                 DurationStats `json:"recheck"`
	IndexDescents           DurationStats `json:"indexDescents"`
	ValueReads              DurationStats `json:"valueReads"`
	ConcurrentBatchCountMax int64         `json:"concurrentBatchCountMax"`
	ArenaBytesMax           int64         `json:"arenaBytesMax"`
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
