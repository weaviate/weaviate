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

package hfresh

import (
	"math"
	"math/bits"
	"os"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const profileLogEvery = 2000

// searchStats collects one query's phase durations and counters. It is
// stack-allocated per query and mutated without synchronization; the profiler
// aggregates it atomically at the end of the query.
type searchStats struct {
	Centroid time.Duration // centroid HNSW search
	Filter   time.Duration // candidate filtering (maxDist + posting sizes)
	Read     time.Duration // posting reads from the LSM store
	Scan     time.Duration // RQ1 scan over posting vectors
	Rescore  time.Duration // exact rescoring of top candidates

	PostingsRequested uint32 // centroids selected for reading
	PostingsRead      uint32 // postings that returned data
	PostingsEmpty     uint32 // postings that returned nothing
	SegmentReads      uint32 // disk segments that contained posting data (fragmentation)
	MemtableReads     uint32 // active/flushing memtables that contained posting data
	PostingBytes      uint64 // total posting bytes returned

	Candidates       uint32 // vectors scanned across all postings
	Duplicates       uint32 // vectors skipped by the visited set (replica overlap)
	Deleted          uint32 // vectors skipped due to tombstones
	AllowlistSkipped uint32 // vectors skipped by the allow list

	RescoreFetched  uint32 // full vectors fetched during rescore
	RescoreNotFound uint32 // rescore candidates deleted between scan and rescore
	Results         uint32 // final results returned
}

// add merges another stats object's counters into st. Used to fold
// per-worker stats from parallel phases back into the query's stats.
// Durations are not merged: phases are timed as wall clock by the caller.
func (st *searchStats) add(o *searchStats) {
	st.PostingsRequested += o.PostingsRequested
	st.PostingsRead += o.PostingsRead
	st.PostingsEmpty += o.PostingsEmpty
	st.SegmentReads += o.SegmentReads
	st.MemtableReads += o.MemtableReads
	st.PostingBytes += o.PostingBytes
	st.Candidates += o.Candidates
	st.Duplicates += o.Duplicates
	st.Deleted += o.Deleted
	st.AllowlistSkipped += o.AllowlistSkipped
	st.RescoreFetched += o.RescoreFetched
	st.RescoreNotFound += o.RescoreNotFound
	st.Results += o.Results
}

// newSearchProfilerFromEnv returns a profiler when HFRESH_SEARCH_PROFILE is set
// to a truthy value, otherwise nil (profiling disabled, near-zero hot-path cost).
func newSearchProfilerFromEnv(logger logrus.FieldLogger) *searchProfiler {
	switch os.Getenv("HFRESH_SEARCH_PROFILE") {
	case "true", "1", "on", "yes":
		logger.WithField("action", "hfresh_search_profile").
			Infof("hfresh search profiler enabled, logging every %d queries", profileLogEvery)
		return newSearchProfiler(logger, profileLogEvery)
	default:
		return nil
	}
}

// searchProfiler aggregates per-query searchStats so we can see where time and
// IO go (centroid vs posting read vs scan vs rescore) including tails, not
// just means. Every everyN queries it logs a breakdown of the queries recorded
// since the previous log line (one window), then starts a fresh window — so a
// benchmark sweep produces per-setting lines instead of a cumulative blend.
// All state is atomic; record() is safe from concurrent searches.
type searchProfiler struct {
	logger logrus.FieldLogger
	everyN uint64

	queries atomic.Uint64 // cumulative across windows
	window  atomic.Pointer[profileWindow]
}

// profileWindow holds the histograms for one logging window.
type profileWindow struct {
	// phase durations, recorded in microseconds
	centroid logHist
	filter   logHist
	read     logHist
	scan     logHist
	rescore  logHist

	// per-query counters
	postingsRequested logHist
	postingsRead      logHist
	postingsEmpty     logHist
	segmentReads      logHist
	memtableReads     logHist
	postingBytes      logHist
	candidates        logHist
	duplicates        logHist
	deleted           logHist
	allowlistSkipped  logHist
	rescoreFetched    logHist
	rescoreNotFound   logHist
	results           logHist
}

func newSearchProfiler(logger logrus.FieldLogger, everyN uint64) *searchProfiler {
	if everyN < 1 {
		everyN = 1
	}
	p := &searchProfiler{logger: logger, everyN: everyN}
	p.window.Store(&profileWindow{})
	return p
}

func (p *searchProfiler) enabled() bool {
	return p != nil
}

// record aggregates one query's stats. Safe to call on a nil profiler.
func (p *searchProfiler) record(st *searchStats) {
	if p == nil {
		return
	}

	w := p.window.Load()

	w.centroid.Record(uint64(st.Centroid / time.Microsecond))
	w.filter.Record(uint64(st.Filter / time.Microsecond))
	w.read.Record(uint64(st.Read / time.Microsecond))
	w.scan.Record(uint64(st.Scan / time.Microsecond))
	w.rescore.Record(uint64(st.Rescore / time.Microsecond))

	w.postingsRequested.Record(uint64(st.PostingsRequested))
	w.postingsRead.Record(uint64(st.PostingsRead))
	w.postingsEmpty.Record(uint64(st.PostingsEmpty))
	w.segmentReads.Record(uint64(st.SegmentReads))
	w.memtableReads.Record(uint64(st.MemtableReads))
	w.postingBytes.Record(st.PostingBytes)
	w.candidates.Record(uint64(st.Candidates))
	w.duplicates.Record(uint64(st.Duplicates))
	w.deleted.Record(uint64(st.Deleted))
	w.allowlistSkipped.Record(uint64(st.AllowlistSkipped))
	w.rescoreFetched.Record(uint64(st.RescoreFetched))
	w.rescoreNotFound.Record(uint64(st.RescoreNotFound))
	w.results.Record(uint64(st.Results))

	n := p.queries.Add(1)
	if n%p.everyN == 0 {
		p.log(n)
	}
}

type profileSnapshot struct {
	Queries uint64 // cumulative queries recorded across all windows

	// phase durations in microseconds
	Centroid, Filter, Read, Scan, Rescore *logHist

	PostingsRequested, PostingsRead, PostingsEmpty *logHist
	SegmentReads, MemtableReads, PostingBytes      *logHist
	Candidates, Duplicates, Deleted                *logHist
	AllowlistSkipped                               *logHist
	RescoreFetched, RescoreNotFound, Results       *logHist

	ReadPct            float64 // posting-read share of the summed phase time
	SegmentsPerPosting float64 // avg disk segments contributing to one posting read
	DuplicatePct       float64 // share of scanned candidates that were replica duplicates
}

// snapshot describes the current (not yet logged) window.
func (p *searchProfiler) snapshot() profileSnapshot {
	return p.snapshotWindow(p.window.Load())
}

func (p *searchProfiler) snapshotWindow(w *profileWindow) profileSnapshot {
	s := profileSnapshot{
		Queries:           p.queries.Load(),
		Centroid:          &w.centroid,
		Filter:            &w.filter,
		Read:              &w.read,
		Scan:              &w.scan,
		Rescore:           &w.rescore,
		PostingsRequested: &w.postingsRequested,
		PostingsRead:      &w.postingsRead,
		PostingsEmpty:     &w.postingsEmpty,
		SegmentReads:      &w.segmentReads,
		MemtableReads:     &w.memtableReads,
		PostingBytes:      &w.postingBytes,
		Candidates:        &w.candidates,
		Duplicates:        &w.duplicates,
		Deleted:           &w.deleted,
		AllowlistSkipped:  &w.allowlistSkipped,
		RescoreFetched:    &w.rescoreFetched,
		RescoreNotFound:   &w.rescoreNotFound,
		Results:           &w.results,
	}

	phaseTotal := w.centroid.Sum() + w.filter.Sum() + w.read.Sum() + w.scan.Sum() + w.rescore.Sum()
	if phaseTotal > 0 {
		s.ReadPct = float64(w.read.Sum()) / float64(phaseTotal) * 100
	}
	if w.postingsRead.Sum() > 0 {
		s.SegmentsPerPosting = float64(w.segmentReads.Sum()) / float64(w.postingsRead.Sum())
	}
	if w.candidates.Sum() > 0 {
		s.DuplicatePct = float64(w.duplicates.Sum()) / float64(w.candidates.Sum()) * 100
	}

	return s
}

// log emits the current window and starts a fresh one. Records racing with
// the swap may land in either window; that skew is negligible for profiling.
func (p *searchProfiler) log(cumulative uint64) {
	old := p.window.Swap(&profileWindow{})
	s := p.snapshotWindow(old)

	us := func(mean float64) string {
		return (time.Duration(mean) * time.Microsecond).String()
	}
	usQ := func(h *logHist, q float64) string {
		return (time.Duration(h.Quantile(q)) * time.Microsecond).String()
	}

	p.logger.WithFields(logrus.Fields{
		"action":  "hfresh_search_profile",
		"queries": cumulative,
		"window":  old.results.Count(),

		"centroid_mean": us(s.Centroid.Mean()),
		"centroid_p99":  usQ(s.Centroid, 0.99),
		"filter_mean":   us(s.Filter.Mean()),
		"read_mean":     us(s.Read.Mean()),
		"read_p99":      usQ(s.Read, 0.99),
		"scan_mean":     us(s.Scan.Mean()),
		"scan_p99":      usQ(s.Scan, 0.99),
		"rescore_mean":  us(s.Rescore.Mean()),
		"rescore_p99":   usQ(s.Rescore, 0.99),
		"read_pct":      math.Round(s.ReadPct*10) / 10,

		"postings_requested_mean": math.Round(s.PostingsRequested.Mean()),
		"postings_read_mean":      math.Round(s.PostingsRead.Mean()),
		"postings_empty_mean":     math.Round(s.PostingsEmpty.Mean()),
		"segments_per_posting":    math.Round(s.SegmentsPerPosting*100) / 100,
		"memtable_reads_mean":     math.Round(s.MemtableReads.Mean()),
		"posting_kb_mean":         math.Round(s.PostingBytes.Mean() / 1024),

		"candidates_mean":   math.Round(s.Candidates.Mean()),
		"duplicate_pct":     math.Round(s.DuplicatePct*10) / 10,
		"deleted_mean":      math.Round(s.Deleted.Mean()),
		"allowlist_skipped": math.Round(s.AllowlistSkipped.Mean()),
		"rescore_fetched":   math.Round(s.RescoreFetched.Mean()),
		"rescore_not_found": math.Round(s.RescoreNotFound.Mean()),
		"results_mean":      math.Round(s.Results.Mean()),
	}).Info("hfresh search profile")
}

// logHist is a lock-free histogram with power-of-two buckets. Values land in
// bucket bits.Len64(v), so quantile estimates are upper bounds rounded up to
// the next power of two — precise enough to separate microseconds from
// milliseconds, which is all we need. The zero value is ready to use.
type logHist struct {
	buckets [65]atomic.Uint64
	sum     atomic.Uint64
	count   atomic.Uint64
	max     atomic.Uint64
}

func (h *logHist) Record(v uint64) {
	h.buckets[bits.Len64(v)].Add(1)
	h.sum.Add(v)
	h.count.Add(1)

	for {
		cur := h.max.Load()
		if v <= cur || h.max.CompareAndSwap(cur, v) {
			return
		}
	}
}

func (h *logHist) Count() uint64 { return h.count.Load() }
func (h *logHist) Sum() uint64   { return h.sum.Load() }
func (h *logHist) Max() uint64   { return h.max.Load() }

func (h *logHist) Mean() float64 {
	count := h.count.Load()
	if count == 0 {
		return 0
	}
	return float64(h.sum.Load()) / float64(count)
}

// Quantile returns an upper bound for the q-quantile: the upper edge of the
// power-of-two bucket the quantile falls into.
func (h *logHist) Quantile(q float64) uint64 {
	count := h.count.Load()
	if count == 0 {
		return 0
	}

	target := max(uint64(math.Ceil(q*float64(count))), 1)

	var cum uint64
	for i := range h.buckets {
		cum += h.buckets[i].Load()
		if cum >= target {
			return 1<<i - 1
		}
	}

	return h.max.Load()
}
