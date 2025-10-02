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
