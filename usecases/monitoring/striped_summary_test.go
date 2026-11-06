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
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func TestStripedSummaryVecMatchesSummaryVec(t *testing.T) {
	type observation struct {
		operation string
		bytes     uint64
	}

	tests := []struct {
		name         string
		writers      int
		observations []observation
	}{
		{
			name:         "one operation",
			writers:      1,
			observations: []observation{{"preadInto", 4096}, {"preadInto", 18}, {"preadInto", 0}},
		},
		{
			name:    "several operations",
			writers: 1,
			observations: []observation{
				{"preadInto", 4096},
				{"readSegmentFile", 1 << 30},
				{"loadBloomfilter", 512},
				{"netAdditions", 12},
				{"preadInto", 7},
			},
		},
		{
			name:    "concurrent writers",
			writers: 16,
			observations: []observation{
				{"preadInto", 4096}, {"loadMetadata", 33}, {"preadInto", 18}, {"netAdditions", 12},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			summary := prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: "reads", Help: "help"}, []string{"operation"})
			striped := NewStripedSummaryVec("reads", "help", "operation")

			// a scrape running beside the writers races each first WithLabelValue
			stopScraping := make(chan struct{})
			scraperDone := make(chan struct{})
			go func() {
				defer close(scraperDone)
				for {
					select {
					case <-stopScraping:
						return
					default:
						striped.Collect(make(chan prometheus.Metric, len(tt.observations)))
					}
				}
			}()

			before := time.Now()
			var wg sync.WaitGroup
			for range tt.writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 100 {
						for _, o := range tt.observations {
							summary.WithLabelValues(o.operation).Observe(float64(o.bytes))
							striped.WithLabelValue(o.operation).Observe(o.bytes)
						}
					}
				}()
			}
			wg.Wait()
			after := time.Now()
			close(stopScraping)
			<-scraperDone

			want, err := testutil.CollectAndFormat(summary, expfmt.TypeTextPlain, "reads")
			require.NoError(t, err)
			require.NoError(t, testutil.CollectAndCompare(striped, bytes.NewReader(want)))

			// protobuf scrapes carry the created timestamp, which the text compare above drops
			created := map[string]time.Time{}
			for range 2 {
				ch := make(chan prometheus.Metric, len(tt.observations))
				striped.Collect(ch)
				close(ch)
				for m := range ch {
					var got dto.Metric
					require.NoError(t, m.Write(&got))
					operation := got.GetLabel()[0].GetValue()
					ts := got.GetSummary().GetCreatedTimestamp().AsTime()
					require.False(t, ts.Before(before) || ts.After(after), "%s created at %v, outside [%v, %v]", operation, ts, before, after)
					if first, ok := created[operation]; ok {
						require.True(t, first.Equal(ts), "%s created timestamp moved between scrapes", operation)
					}
					created[operation] = ts
				}
			}
		})
	}
}

type benchSlot16 struct{ count, sum atomic.Uint64 }

func (s *benchSlot16) add(v uint64) { s.count.Add(1); s.sum.Add(v) }

type benchSlot64 struct {
	count, sum atomic.Uint64
	_          [48]byte
}

func (s *benchSlot64) add(v uint64) { s.count.Add(1); s.sum.Add(v) }

// benchStripedObserve observes into slots of slotSize bytes, one picked at
// random per call like StripedSummary.Observe.
func benchStripedObserve(slots, slotSize int) func(uint64) {
	mask := uint32(slots - 1)
	switch slotSize {
	case 16:
		s := make([]benchSlot16, slots)
		return func(v uint64) { s[rand.Uint32()&mask].add(v) }
	case 64:
		s := make([]benchSlot64, slots)
		return func(v uint64) { s[rand.Uint32()&mask].add(v) }
	default:
		s := make([]stripedSummarySlot, slots)
		return func(v uint64) { s[rand.Uint32()&mask].add(v) }
	}
}

func BenchmarkStripedSummary(b *testing.B) {
	summary := prometheus.NewSummary(prometheus.SummaryOpts{Name: "reads", Help: "help"})
	type benchCase struct {
		name    string
		observe func(uint64)
	}
	cases := []benchCase{{name: "summary", observe: func(v uint64) { summary.Observe(float64(v)) }}}
	for _, slots := range []int{16, 32, 64, 128} {
		for _, slotSize := range []int{16, 64, stripedSummarySlotSize} {
			cases = append(cases, benchCase{
				name:    fmt.Sprintf("slots=%d/slotSize=%d", slots, slotSize),
				observe: benchStripedObserve(slots, slotSize),
			})
		}
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.observe(4096)
				}
			})
		})
	}
}
