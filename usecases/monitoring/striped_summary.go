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

package monitoring

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Observe adds to one of stripedSummarySlots slots picked at random, so concurrent
// callers rarely share a cache line. stripedSummarySlotSize spans one 128-byte line
// or a prefetched pair of 64-byte lines. BenchmarkStripedSummary sweeps both.
const (
	stripedSummarySlots    = 64
	stripedSummarySlotSize = 128
)

type stripedSummarySlot struct {
	count atomic.Uint64
	sum   atomic.Uint64
	_     [stripedSummarySlotSize - 16]byte
}

func (s *stripedSummarySlot) add(v uint64) {
	s.count.Add(1)
	s.sum.Add(v)
}

// StripedSummary is a summary without quantiles whose Observe rarely contends
// with other goroutines observing it. Its size is a whole allocator
// size class, so the slots start on a cache line.
type StripedSummary struct {
	slots [stripedSummarySlots]stripedSummarySlot
}

func (s *StripedSummary) Observe(v uint64) {
	s.slots[rand.Uint32()%stripedSummarySlots].add(v)
}

func (s *StripedSummary) countAndSum() (count, sum uint64) {
	for i := range s.slots {
		count += s.slots[i].count.Load()
		sum += s.slots[i].sum.Load()
	}
	return count, sum
}

type stripedSummaryChild struct {
	summary *StripedSummary
	created time.Time
}

// StripedSummaryVec collects a StripedSummary per value of its one label, in
// the same exposition format as a prometheus.SummaryVec without objectives.
type StripedSummaryVec struct {
	desc *prometheus.Desc

	mu       sync.Mutex
	children map[string]stripedSummaryChild
}

func NewStripedSummaryVec(name, help, label string) *StripedSummaryVec {
	return &StripedSummaryVec{
		desc:     prometheus.NewDesc(name, help, []string{label}, nil),
		children: map[string]stripedSummaryChild{},
	}
}

func (v *StripedSummaryVec) WithLabelValue(value string) *StripedSummary {
	v.mu.Lock()
	defer v.mu.Unlock()

	child, ok := v.children[value]
	if !ok {
		child = stripedSummaryChild{summary: &StripedSummary{}, created: time.Now()}
		v.children[value] = child
	}
	return child.summary
}

func (v *StripedSummaryVec) Describe(ch chan<- *prometheus.Desc) {
	ch <- v.desc
}

func (v *StripedSummaryVec) Collect(ch chan<- prometheus.Metric) {
	v.mu.Lock()
	defer v.mu.Unlock()

	for value, child := range v.children {
		count, sum := child.summary.countAndSum()
		ch <- prometheus.MustNewConstSummaryWithCreatedTimestamp(v.desc, count, float64(sum), nil, child.created, value)
	}
}
