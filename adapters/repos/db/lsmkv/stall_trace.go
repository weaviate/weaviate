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

// THROWAWAY measurement code for a CI flake investigation. Never merge.
//
// LSMKV_STALL_TRACE=1 records memtable lock hold/wait times and bucket
// flush events. Entries go to LSMKV_STALL_TRACE_FILE (default stderr).

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

var stallTraceEnabled = os.Getenv("LSMKV_STALL_TRACE") == "1"

const (
	stallThreshold  = 20 * time.Millisecond
	stallEntryCap   = 500
	stallEventCap   = 5000
	stallTimeFormat = "15:04:05.000000"
)

var (
	stallOut       io.Writer = os.Stderr
	stallOutMu     sync.Mutex
	stallEntries   atomic.Int64
	stallEvents    atomic.Int64
	stallOverruns  atomic.Int64
	stallDropped   atomic.Int64
	stallHistMu    sync.Mutex
	stallHistByKey = map[string]*stallHist{}
)

// histogram bucket upper bounds
var stallBuckets = []time.Duration{
	1 * time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond,
	50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond,
	500 * time.Millisecond,
}

type stallHist struct {
	counts [8]atomic.Int64
	max    atomic.Int64
	sum    atomic.Int64
}

func init() {
	if !stallTraceEnabled {
		return
	}
	if p := os.Getenv("LSMKV_STALL_TRACE_FILE"); p != "" {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err == nil {
			stallOut = f
		}
	}
}

func stallHistFor(key string) *stallHist {
	stallHistMu.Lock()
	defer stallHistMu.Unlock()
	h, ok := stallHistByKey[key]
	if !ok {
		h = &stallHist{}
		stallHistByKey[key] = h
	}
	return h
}

func stallObserve(key string, d time.Duration) {
	h := stallHistFor(key)
	i := 0
	for i < len(stallBuckets) && d >= stallBuckets[i] {
		i++
	}
	h.counts[i].Add(1)
	h.sum.Add(int64(d))
	for {
		cur := h.max.Load()
		if int64(d) <= cur || h.max.CompareAndSwap(cur, int64(d)) {
			break
		}
	}
}

func stallWrite(line string) {
	stallOutMu.Lock()
	defer stallOutMu.Unlock()
	fmt.Fprintln(stallOut, line)
}

// StallTracef logs a threshold-exceeding entry (capped). Exported so the
// acceptance test can put its own outliers on the same timeline.
func StallTracef(at time.Time, kind string, format string, args ...any) {
	if !stallTraceEnabled {
		return
	}
	stallOverruns.Add(1)
	if stallEntries.Add(1) > stallEntryCap {
		stallDropped.Add(1)
		return
	}
	stallWrite(fmt.Sprintf("STALL %s %-22s %s", at.UTC().Format(stallTimeFormat), kind,
		fmt.Sprintf(format, args...)))
}

// stallEventf logs an always-on event such as a memtable switch (capped).
func stallEventf(at time.Time, kind string, format string, args ...any) {
	if !stallTraceEnabled {
		return
	}
	if stallEvents.Add(1) > stallEventCap {
		return
	}
	stallWrite(fmt.Sprintf("EVENT %s %-22s %s", at.UTC().Format(stallTimeFormat), kind,
		fmt.Sprintf(format, args...)))
}

// StallTraceObserve records a duration into a named histogram.
func StallTraceObserve(key string, d time.Duration) {
	if stallTraceEnabled {
		stallObserve(key, d)
	}
}

// StallTraceSummary writes counters and histograms.
func StallTraceSummary() {
	if !stallTraceEnabled {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SUMMARY overruns=%d logged=%d dropped=%d events=%d\n",
		stallOverruns.Load(), stallLoggedCount(), stallDropped.Load(), stallEvents.Load())
	stallHistMu.Lock()
	keys := make([]string, 0, len(stallHistByKey))
	for k := range stallHistByKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintf(&b, "SUMMARY %-34s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s\n", "histogram",
		"<1ms", "1-5ms", "5-20ms", "20-50ms", "50-100ms", "100-200ms", "200-500ms", ">=500ms", "max_ms", "mean_us")
	for _, k := range keys {
		h := stallHistByKey[k]
		var total int64
		fmt.Fprintf(&b, "SUMMARY %-34s", k)
		for i := range h.counts {
			c := h.counts[i].Load()
			total += c
			fmt.Fprintf(&b, " %10d", c)
		}
		mean := int64(0)
		if total > 0 {
			mean = h.sum.Load() / total / int64(time.Microsecond)
		}
		fmt.Fprintf(&b, " %10.1f %10d\n", float64(h.max.Load())/float64(time.Millisecond), mean)
	}
	stallHistMu.Unlock()
	stallWrite(strings.TrimRight(b.String(), "\n"))
}

// memtableStall holds per-hold breakdown state. All fields are guarded by
// the memtable's write lock.
type memtableStall struct {
	cl    time.Duration // commit log put/append/add/writeEntry
	flush time.Duration // flushBuffers
	sync  time.Duration // fsync
}

// lockW takes the write lock and returns the unlock func, recording wait
// and hold time when tracing is enabled.
func (m *Memtable) lockW(path string) func() {
	if !stallTraceEnabled {
		m.Lock()
		return m.Unlock
	}
	before := time.Now()
	m.Lock()
	acquired := time.Now()
	wait := acquired.Sub(before)
	m.stall = memtableStall{}
	return func() {
		hold := time.Since(acquired)
		st := m.stall
		m.Unlock()
		stallObserve("hold/"+path, hold)
		stallObserve("wait/"+path, wait)
		if hold > stallThreshold {
			StallTracef(acquired, "hold/"+path,
				"hold=%s cl_write=%s flushBuffers=%s fsync=%s other=%s wait=%s mt=%s",
				ms(hold), ms(st.cl), ms(st.flush), ms(st.sync), ms(hold-st.cl-st.flush-st.sync), ms(wait), shortPath(m.path))
		}
		if wait > stallThreshold {
			StallTracef(before, "wait/"+path, "wait=%s hold=%s mt=%s", ms(wait), ms(hold), shortPath(m.path))
		}
	}
}

// rlockR takes the read lock, recording the wait.
func (m *Memtable) rlockR(path string) {
	if !stallTraceEnabled {
		m.RLock()
		return
	}
	before := time.Now()
	m.RLock()
	wait := time.Since(before)
	stallObserve("rwait/"+path, wait)
	if wait > stallThreshold {
		StallTracef(before, "rwait/"+path, "wait=%s mt=%s", ms(wait), shortPath(m.path))
	}
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
}

func shortPath(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// tracingCommitLogger times commit log calls into the owning memtable's
// stall state. Timed methods are only called under the memtable write lock.
type tracingCommitLogger struct {
	inner memtableCommitLogger
	m     *Memtable
}

var _ memtableCommitLogger = (*tracingCommitLogger)(nil)

func (t *tracingCommitLogger) writeEntry(c CommitType, b []byte) error {
	s := time.Now()
	err := t.inner.writeEntry(c, b)
	t.m.stall.cl += time.Since(s)
	return err
}

func (t *tracingCommitLogger) put(n segmentReplaceNode) error {
	s := time.Now()
	err := t.inner.put(n)
	t.m.stall.cl += time.Since(s)
	return err
}

func (t *tracingCommitLogger) append(n segmentCollectionNode) error {
	s := time.Now()
	err := t.inner.append(n)
	t.m.stall.cl += time.Since(s)
	return err
}

func (t *tracingCommitLogger) add(n *roaringset.SegmentNodeList) error {
	s := time.Now()
	err := t.inner.add(n)
	t.m.stall.cl += time.Since(s)
	return err
}

func (t *tracingCommitLogger) flushBuffers() error {
	s := time.Now()
	err := t.inner.flushBuffers()
	t.m.stall.flush += time.Since(s)
	return err
}

func (t *tracingCommitLogger) sync() error {
	s := time.Now()
	err := t.inner.sync()
	t.m.stall.sync += time.Since(s)
	return err
}

func (t *tracingCommitLogger) walPath() string { return t.inner.walPath() }
func (t *tracingCommitLogger) size() int64     { return t.inner.size() }
func (t *tracingCommitLogger) close() error {
	s := time.Now()
	err := t.inner.close()
	stallEventf(s, "wal_close", "took=%s mt=%s", ms(time.Since(s)), shortPath(t.m.path))
	return err
}
func (t *tracingCommitLogger) delete() error { return t.inner.delete() }

func stallLoggedCount() int64 {
	if n := stallEntries.Load(); n < stallEntryCap {
		return n
	}
	return stallEntryCap
}

// StallTraceEnabled reports whether LSMKV_STALL_TRACE=1.
func StallTraceEnabled() bool { return stallTraceEnabled }

// StallTraceRaw writes an uncapped line; callers bound their own volume.
func StallTraceRaw(at time.Time, kind string, format string, args ...any) {
	if !stallTraceEnabled {
		return
	}
	stallWrite(fmt.Sprintf("%-5s %s %-22s %s", "TEST", at.UTC().Format(stallTimeFormat), kind,
		fmt.Sprintf(format, args...)))
}
