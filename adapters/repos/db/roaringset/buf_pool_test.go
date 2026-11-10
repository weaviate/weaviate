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

package roaringset

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"weak"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestBufPoolFixedSync(t *testing.T) {
	t.Run("pool returns buffers of given cap", func(t *testing.T) {
		pool123 := NewBufPoolFixedSync(123)
		pool234 := NewBufPoolFixedSync(234)
		pool345 := NewBufPoolFixedSync(345)

		t.Run("buf1", func(t *testing.T) {
			buf1_123, put := pool123.Get()
			defer put()
			buf1_234, put := pool234.Get()
			defer put()
			buf1_345, put := pool345.Get()
			defer put()

			assert.Equal(t, 0, len(buf1_123))
			assert.Equal(t, 123, cap(buf1_123))
			assert.Equal(t, 0, len(buf1_234))
			assert.Equal(t, 234, cap(buf1_234))
			assert.Equal(t, 0, len(buf1_345))
			assert.Equal(t, 345, cap(buf1_345))
		})

		t.Run("buf2", func(t *testing.T) {
			buf2_123, put := pool123.Get()
			defer put()
			buf2_234, put := pool234.Get()
			defer put()
			buf2_345, put := pool345.Get()
			defer put()

			assert.Equal(t, 0, len(buf2_123))
			assert.Equal(t, 123, cap(buf2_123))
			assert.Equal(t, 0, len(buf2_234))
			assert.Equal(t, 234, cap(buf2_234))
			assert.Equal(t, 0, len(buf2_345))
			assert.Equal(t, 345, cap(buf2_345))
		})
	})
}

// testIdleBudget keeps every buffer the pooled-bitmap tests put back.
const testIdleBudget = 1 << 30

var testInMemoSizes = []int{512, 1024, 2048}

type countingInMemoMetrics struct {
	created, got, put, discarded, cleanedUp, evicted int
}

func (m *countingInMemoMetrics) bufCreated()   { m.created++ }
func (m *countingInMemoMetrics) bufGot()       { m.got++ }
func (m *countingInMemoMetrics) bufPut()       { m.put++ }
func (m *countingInMemoMetrics) bufDiscarded() { m.discarded++ }
func (m *countingInMemoMetrics) bufCleanedUp() { m.cleanedUp++ }
func (m *countingInMemoMetrics) bufEvicted()   { m.evicted++ }

func (m *countingInMemoMetrics) add(o *countingInMemoMetrics) {
	m.created += o.created
	m.got += o.got
	m.put += o.put
	m.discarded += o.discarded
	m.cleanedUp += o.cleanedUp
	m.evicted += o.evicted
}

// newCountingInMemoBufPool gives each stack its own counting metrics, so it
// suits single-goroutine tests only.
func newCountingInMemoBufPool(budget int, ttl time.Duration, decayFraction float64,
) (*inMemoBufPool, []*countingInMemoMetrics) {
	p := newInMemoBufPool(nil, testInMemoSizes, budget, ttl, decayFraction)
	metrics := make([]*countingInMemoMetrics, len(p.stacks))
	for i, s := range p.stacks {
		metrics[i] = &countingInMemoMetrics{}
		s.metrics = metrics[i]
	}
	return p, metrics
}

func bufID(buf []byte) *byte { return &buf[:1][0] }

func stackedBufIDs(s *idleStack) []*byte {
	var ids []*byte
	for _, b := range s.bufs[s.head:] {
		ids = append(ids, bufID(b.pb.buf))
	}
	return ids
}

func TestInMemoBufPool(t *testing.T) {
	newPool := func(budget int) (*inMemoBufPool, []*countingInMemoMetrics) {
		return newCountingInMemoBufPool(budget, inMemoIdleTTL, inMemoDecayFraction)
	}

	t.Run("get returns the buffer put last", func(t *testing.T) {
		p, _ := newPool(testIdleBudget)
		_, putOlder := p.get(1)
		newer, putNewer := p.get(1)
		putOlder()
		putNewer()

		buf, put := p.get(1)
		defer put()
		assert.Same(t, bufID(newer), bufID(buf))
	})

	type step struct {
		tick  time.Duration // used when label is empty
		label string
		stack int
	}
	tick := func(d time.Duration) step { return step{tick: d} }
	put := func(label string, stack int) step { return step{label: label, stack: stack} }
	expired := inMemoIdleTTL + 10*time.Second

	tests := []struct {
		name   string
		budget int
		steps  []step
		want   []string // stacked buffers, stack by stack, oldest first
	}{
		{
			name:   "over budget evicts the oldest buffer across sizes",
			budget: 512 + 1024 + 2048,
			steps: []step{
				tick(0), put("A", 1), tick(10 * time.Second), put("B", 2),
				tick(20 * time.Second), put("C", 0), tick(30 * time.Second), put("D", 0),
			},
			want: []string{"C", "D", "B"},
		},
		{
			name:   "two buffers over budget evicts the two oldest",
			budget: 3 * 512,
			steps: []step{
				tick(0), put("A", 0), tick(10 * time.Second), put("B", 0),
				tick(20 * time.Second), put("C", 0), tick(30 * time.Second), put("D", 1),
			},
			want: []string{"C", "D"},
		},
		{
			name:   "equal stamps across sizes evict the larger buffer",
			budget: 2048 + 4*512,
			steps: []step{
				tick(0), put("A", 2), put("B", 0), put("C", 0), put("D", 0), put("E", 0), put("F", 2),
			},
			want: []string{"B", "C", "D", "E", "F"},
		},
		{
			name:   "decay drops the older half of the expired buffers",
			budget: testIdleBudget,
			steps: []step{
				tick(0), put("A", 0), tick(time.Second), put("B", 0),
				tick(2 * time.Second), put("C", 0), tick(3 * time.Second), put("D", 0),
				tick(expired),
			},
			want: []string{"C", "D"},
		},
		{
			name:   "decay drops a lone expired buffer",
			budget: testIdleBudget,
			steps:  []step{tick(0), put("A", 0), tick(expired)},
		},
		{
			name:   "decay waits a TTL before dropping again",
			budget: testIdleBudget,
			steps: []step{
				tick(0), put("A", 0), put("B", 0), put("C", 0), put("D", 0),
				tick(expired), tick(expired + 10*time.Second),
			},
			want: []string{"C", "D"},
		},
		{
			name:   "each size keeps its own decay schedule",
			budget: testIdleBudget,
			steps: []step{
				tick(0), put("A", 0), tick(10 * time.Second), put("B", 1),
				tick(70 * time.Second), tick(80 * time.Second),
			},
		},
		{
			name:   "decay keeps the survivors' stamps",
			budget: 512 + 512 + 1024,
			steps: []step{
				tick(0), put("A", 0), put("B", 0), tick(50 * time.Second), put("C", 1),
				tick(100 * time.Second), put("D", 1),
			},
			want: []string{"C", "D"},
		},
		{
			name:   "put stamps the clock tick last stored",
			budget: testIdleBudget,
			steps:  []step{tick(100 * time.Second), put("A", 0), tick(150 * time.Second)},
			want:   []string{"A"},
		},
		{
			name:   "decay returns its bytes to the budget",
			budget: 2 * 512,
			steps:  []step{tick(0), put("A", 0), put("B", 0), tick(expired), put("C", 0)},
			want:   []string{"B", "C"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newPool(tc.budget)
			// Get every buffer up front, so each is created rather than reused.
			labels := map[*byte]string{}
			puts := map[string]func(){}
			for _, st := range tc.steps {
				if st.label != "" {
					buf, put := p.get(st.stack)
					labels[bufID(buf)] = st.label
					puts[st.label] = put
				}
			}
			for _, st := range tc.steps {
				if st.label == "" {
					p.tick(st.tick)
				} else {
					puts[st.label]()
				}
			}

			var got []string
			for _, s := range p.stacks {
				for _, id := range stackedBufIDs(s) {
					got = append(got, labels[id])
				}
			}
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("put with a full budget and nothing to evict drops the buffer", func(t *testing.T) {
		p, _ := newPool(1024)
		_, put := p.get(0)
		p.idle.Store(p.budget)

		done := make(chan struct{})
		go func() {
			defer close(done)
			put()
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("put must drop the buffer, not retry eviction forever")
		}
		assert.Equal(t, p.budget, p.idle.Load())
		assert.Empty(t, stackedBufIDs(p.stacks[0]))
	})

	t.Run("concurrent puts keep every buffer that fits", func(t *testing.T) {
		for range 500 {
			p := newInMemoBufPool(nil, testInMemoSizes, 4*1024, inMemoIdleTTL, inMemoDecayFraction)
			puts := make([]func(), 16)
			for i := range puts {
				_, puts[i] = p.get(1)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			for _, put := range puts {
				wg.Go(func() {
					<-start
					put()
				})
			}
			close(start)
			wg.Wait()
			require.Len(t, stackedBufIDs(p.stacks[1]), 4)
			require.Equal(t, int64(4*1024), p.idle.Load())
		}
	})

	t.Run("a full stack cycled past its capacity reuses its backing array", func(t *testing.T) {
		p, _ := newPool(testIdleBudget)
		s := p.stacks[0]
		puts := make([]func(), 4)
		ids := make([]*byte, len(puts))
		for i := range puts {
			var buf []byte
			buf, puts[i] = p.get(0)
			ids[i] = bufID(buf)
		}
		for _, put := range puts {
			put()
		}
		capacity := cap(s.bufs)
		require.Equal(t, len(s.bufs), capacity, "the stack must start full")

		for range 3 * capacity {
			s.popOldest().put()
			ids = append(ids[1:], ids[0])
		}
		assert.Equal(t, ids, stackedBufIDs(s))
		assert.Equal(t, capacity, cap(s.bufs))
	})

	t.Run("a buffer evicted after a copy-down is collectable", func(t *testing.T) {
		p, _ := newPool(testIdleBudget)
		// Fill stack 0, pop two so the next push copies the rest down, push,
		// then evict the second buffer the copy moved.
		evictAfterCopyDown := func() weak.Pointer[pooledBuf] {
			s := p.stacks[0]
			puts := make([]func(), 5)
			for i := range puts {
				_, puts[i] = p.get(0)
			}
			for _, put := range puts[:4] {
				put()
			}
			s.popOldest()
			s.popOldest()
			puts[4]()
			victim := weak.Make(s.bufs[s.head+1].pb)
			s.popOldest()
			s.popOldest()
			return victim
		}
		victim := evictAfterCopyDown()
		runtime.GC()
		assert.Nil(t, victim.Value(), "a vacated slot must not keep the evicted buffer alive")
		runtime.KeepAlive(p)
	})

	t.Run("a decayed buffer is collectable", func(t *testing.T) {
		p, _ := newPool(testIdleBudget)
		decay := func() weak.Pointer[pooledBuf] {
			s := p.stacks[0]
			p.tick(0)
			_, putA := p.get(0)
			_, putB := p.get(0)
			putA()
			putB()
			victim := weak.Make(s.bufs[s.head].pb)
			p.tick(inMemoIdleTTL + 10*time.Second)
			require.Len(t, stackedBufIDs(s), 1)
			return victim
		}
		victim := decay()
		runtime.GC()
		assert.Nil(t, victim.Value(), "a decayed slot must not keep the buffer alive")
		runtime.KeepAlive(p)
	})

	t.Run("metrics count each outcome on the buffer's own size", func(t *testing.T) {
		p, metrics := newPool(1024)
		_, putA := p.get(0)
		_, putB := p.get(0)
		_, putC := p.get(1)
		_, putD := p.get(2)
		putA()
		putB()
		_, put := p.get(0)
		put()
		putC()                              // evicts A and B
		p.tick(inMemoIdleTTL + time.Second) // decays C
		putD()                              // larger than the budget

		assert.Equal(t, []countingInMemoMetrics{
			{created: 2, got: 1, put: 3, evicted: 2},
			{created: 1, put: 1, cleanedUp: 1},
			{created: 1, discarded: 1},
		}, []countingInMemoMetrics{*metrics[0], *metrics[1], *metrics[2]})
	})

	t.Run("concurrent put, evict and decay", func(t *testing.T) {
		p := newInMemoBufPool(nil, testInMemoSizes, 4*1024, inMemoIdleTTL, inMemoDecayFraction)
		var wg sync.WaitGroup
		for w := range 8 {
			wg.Go(func() {
				for i := range 2000 {
					buf, put := p.get((w + i) % len(testInMemoSizes))
					marker := uint64(w)<<32 | uint64(i)
					buf = buf[:8]
					binary.LittleEndian.PutUint64(buf, marker)
					runtime.Gosched()
					if got := binary.LittleEndian.Uint64(buf); got != marker {
						t.Errorf("buffer handed out twice: marker %x overwritten by %x", marker, got)
						return
					}
					put()
				}
			})
		}
		wg.Go(func() {
			for i := range 2000 {
				p.tick(time.Duration(i) * inMemoIdleTTL)
				runtime.Gosched()
			}
		})
		wg.Wait()

		stacked := int64(0)
		for _, s := range p.stacks {
			stacked += int64(len(stackedBufIDs(s)) * s.size)
		}
		assert.Equal(t, stacked, p.idle.Load())
		assert.LessOrEqual(t, p.idle.Load(), p.budget)
	})
}

// BenchmarkInMemoBufPoolDecaySweep replays a seeded bursty trace for each TTL and
// decay fraction. It reports the hit rate, the idle share of the budget, and the
// buffers eviction and decay dropped.
func BenchmarkInMemoBufPoolDecaySweep(b *testing.B) {
	const budget = 8 * 1024
	for _, ttl := range []time.Duration{15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute} {
		for _, fraction := range []float64{0.25, 0.5, 1.0} {
			b.Run(fmt.Sprintf("ttl=%s/fraction=%.2f", ttl, fraction), func(b *testing.B) {
				var total countingInMemoMetrics
				idleShare := 0.0
				for b.Loop() {
					metrics, share := replayBurstyTrace(ttl, fraction, budget)
					total.add(&metrics)
					idleShare += share
				}
				b.ReportMetric(100*float64(total.got)/float64(total.got+total.created), "hit%")
				b.ReportMetric(idleShare/float64(b.N), "idle/budget")
				b.ReportMetric(float64(total.evicted)/float64(b.N), "evicted/op")
				b.ReportMetric(float64(total.cleanedUp)/float64(b.N), "decayed/op")
			})
		}
	}
}

// replayBurstyTrace replays an hour of virtual time. The 512 B size is busy every
// tick, 1 KiB bursts every 45 s and 2 KiB every 150 s, each burst holding one to
// four buffers.
func replayBurstyTrace(ttl time.Duration, fraction float64, budget int,
) (total countingInMemoMetrics, idleShare float64) {
	p, metrics := newCountingInMemoBufPool(budget, ttl, fraction)
	rng := rand.New(rand.NewPCG(1, 2))
	bursts := []struct {
		stack int
		every time.Duration
	}{{0, inMemoDecayTick}, {1, 45 * time.Second}, {2, 150 * time.Second}}
	const step = 5 * time.Second

	var held []func()
	samples := 0
	for now := time.Duration(0); now < time.Hour; now += step {
		if now%inMemoDecayTick == 0 {
			p.tick(now)
		}
		for _, burst := range bursts {
			if now%burst.every != 0 {
				continue
			}
			for range 1 + rng.IntN(4) {
				_, put := p.get(burst.stack)
				held = append(held, put)
			}
		}
		for _, put := range held {
			put()
		}
		held = held[:0]
		idleShare += float64(p.idle.Load()) / float64(budget)
		samples++
	}
	for _, m := range metrics {
		total.add(m)
	}
	return total, idleShare / float64(samples)
}

func TestBitmapBufPoolRanged(t *testing.T) {
	var metrics *monitoring.PrometheusMetrics = nil

	t.Run("pool returns buffers of next higher range", func(t *testing.T) {
		ranges := []int{32, 64, 128, 256, 512, 1024}

		testCases := []struct {
			cap         int
			expectedCap int
		}{
			{
				cap:         1,
				expectedCap: 32,
			},
			{
				cap:         16,
				expectedCap: 32,
			},
			{
				cap:         32,
				expectedCap: 32,
			},
			{
				cap:         33,
				expectedCap: 64,
			},
			{
				cap:         64,
				expectedCap: 64,
			},
			{
				cap:         65,
				expectedCap: 128,
			},
			{
				cap:         128,
				expectedCap: 128,
			},
			{
				cap:         129,
				expectedCap: 256,
			},
			{
				cap:         256,
				expectedCap: 256,
			},
			{
				cap:         257,
				expectedCap: 512,
			},
			{
				cap:         512,
				expectedCap: 512,
			},
			{
				cap:         513,
				expectedCap: 1024,
			},
			{
				cap:         1025,
				expectedCap: 1025,
			},
			{
				cap:         2345,
				expectedCap: 2345,
			},
		}

		t.Run("sync pools return buffers of sizes", func(t *testing.T) {
			syncMaxBufSize := 1024 // all sync pools
			pool := NewBitmapBufPoolRanged(metrics, syncMaxBufSize, testIdleBudget, ranges...)

			for i, tc := range testCases {
				t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
					buf, put := pool.Get(tc.cap)
					defer put()

					assert.Equal(t, 0, len(buf))
					assert.Equal(t, tc.expectedCap, cap(buf))
				})
			}
		})

		t.Run("sync + inmemo pools return buffers of sizes", func(t *testing.T) {
			syncMaxBufSize := 256 // sync pools + inmemo pools (512, 1024)
			pool := NewBitmapBufPoolRanged(metrics, syncMaxBufSize, testIdleBudget, ranges...)

			for i, tc := range testCases {
				t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
					buf, put := pool.Get(tc.cap)
					defer put()

					assert.Equal(t, 0, len(buf))
					assert.Equal(t, tc.expectedCap, cap(buf))
				})
			}
		})
	})
}

func TestCalculateSyncBufferRanges(t *testing.T) {
	testCases := []struct {
		minRangeP2     int
		maxRangeP2     int
		expectedRanges []int
	}{
		{
			minRangeP2:     1,
			maxRangeP2:     5,
			expectedRanges: []int{2, 4, 8, 16, 32},
		},
		{
			minRangeP2:     7,
			maxRangeP2:     10,
			expectedRanges: []int{128, 256, 512, 1024},
		},
		{
			minRangeP2:     9,
			maxRangeP2:     20,
			expectedRanges: []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576},
		},
		{
			minRangeP2:     7,
			maxRangeP2:     10,
			expectedRanges: []int{128, 256, 512, 1024},
		},
		{
			minRangeP2:     0,
			maxRangeP2:     0,
			expectedRanges: []int{1},
		},
		{
			minRangeP2:     0,
			maxRangeP2:     1,
			expectedRanges: []int{1, 2},
		},
		{
			minRangeP2:     -1,
			maxRangeP2:     0,
			expectedRanges: []int{},
		},
		{
			minRangeP2:     0,
			maxRangeP2:     -1,
			expectedRanges: []int{},
		},
		{
			minRangeP2:     9,
			maxRangeP2:     7,
			expectedRanges: []int{},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
			ranges := calculateSyncBufferRanges(tc.minRangeP2, tc.maxRangeP2)
			require.Equal(t, tc.expectedRanges, ranges)
		})
	}
}

func TestCalculateInMemoBufferRanges(t *testing.T) {
	MiB := 1 << 20

	testCases := []struct {
		name       string
		maxBufSize int
		idleBudget int
		want       []int
	}{
		{"default", 32 * MiB, 128 * MiB, []int{2 * MiB, 4 * MiB, 8 * MiB, 16 * MiB, 32 * MiB}},
		{"max size not a power of two", 24 * MiB, 128 * MiB, []int{2 * MiB, 4 * MiB, 8 * MiB, 16 * MiB, 24 * MiB}},
		{"max size not a power of two, over the budget", 24 * MiB, 20 * MiB, []int{2 * MiB, 4 * MiB, 8 * MiB, 16 * MiB}},
		{"max size within the sync.Pool tier", 768 << 10, 128 * MiB, nil},
		{"no budget", 32 * MiB, 0, nil},
		{"budget between two sizes", 32 * MiB, 10 * MiB, []int{2 * MiB, 4 * MiB, 8 * MiB}},
		{"budget equal to a size", 32 * MiB, 16 * MiB, []int{2 * MiB, 4 * MiB, 8 * MiB, 16 * MiB}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, calculateInMemoBufferRanges(1*MiB, 21, tc.maxBufSize, tc.idleBudget))
		})
	}

	t.Run("unlimited max size and budget", func(t *testing.T) {
		ranges := calculateInMemoBufferRanges(1*MiB, 21, math.MaxInt64, math.MaxInt64)
		require.Len(t, ranges, 43)
		assert.Equal(t, []int{1 << 62, math.MaxInt64}, ranges[41:])
	})
}

// TestNewBitmapBufPoolDefaultClose pins that the default pool's ticker decays
// idle buffers, and that close stops it while the pool stays usable.
func TestNewBitmapBufPoolDefaultClose(t *testing.T) {
	logger, _ := test.NewNullLogger()
	synctest.Test(t, func(t *testing.T) {
		pool, closePool := NewBitmapBufPoolDefault(logger, nil, 4<<20, 8<<20)
		first, put := pool.Get(2 << 20)
		put()
		kept, put := pool.Get(2 << 20)
		put()
		assert.Same(t, bufID(first), bufID(kept), "the budget must keep the buffer put back")

		time.Sleep(inMemoIdleTTL + 2*inMemoDecayTick)
		synctest.Wait()
		second, put := pool.Get(2 << 20)
		put()
		assert.NotSame(t, bufID(first), bufID(second), "the decay ticker must drop the expired buffer")

		closePool()
		_, put = pool.Get(2 << 20)
		put()
	})
}

func TestValidateBufferRanges(t *testing.T) {
	testCases := []struct {
		ranges         []int
		expectedRanges []int
	}{
		{
			ranges:         []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			ranges:         []int{1, 2, 3, 1, 2, 3, 1, 2, 3},
			expectedRanges: []int{1, 2, 3},
		},
		{
			ranges:         []int{-3, -2, -1, 0, 1, 2, 3},
			expectedRanges: []int{1, 2, 3},
		},
		{
			ranges:         []int{3, 2, 1, 0, -1, -2, -3},
			expectedRanges: []int{1, 2, 3},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
			ranges := validateBufferRanges(tc.ranges)
			require.Equal(t, tc.expectedRanges, ranges)
		})
	}
}

func TestBitmapBufPoolTrackingForTests_Get(t *testing.T) {
	t.Run("outstanding increments on Get and decrements on release", func(t *testing.T) {
		pool := NewBitmapBufPoolTrackingForTests()
		assert.Equal(t, int64(0), pool.Outstanding())

		_, rel1 := pool.Get(64)
		assert.Equal(t, int64(1), pool.Outstanding())

		_, rel2 := pool.Get(64)
		assert.Equal(t, int64(2), pool.Outstanding())

		rel1()
		assert.Equal(t, int64(1), pool.Outstanding())

		rel2()
		assert.Equal(t, int64(0), pool.Outstanding())
	})

	t.Run("release zeroes backing buffer", func(t *testing.T) {
		pool := NewBitmapBufPoolTrackingForTests()
		buf, release := pool.Get(64)
		// Write into the backing array through the cap slice.
		full := buf[:cap(buf)]
		full[0] = 0xFF
		full[cap(buf)-1] = 0xFF

		release()

		// Backing array must be zeroed after release.
		for i, b := range full {
			if b != 0 {
				t.Errorf("buf[%d] = %d after release, want 0", i, b)
			}
		}
	})

	t.Run("double release panics", func(t *testing.T) {
		pool := NewBitmapBufPoolTrackingForTests()
		_, release := pool.Get(64)
		release()
		assert.Panics(t, release)
	})
}

func TestBitmapBufPoolTrackingForTests_CloneToBuf(t *testing.T) {
	t.Run("outstanding increments on CloneToBuf and decrements on release", func(t *testing.T) {
		pool := NewBitmapBufPoolTrackingForTests()

		bm := sroar.NewBitmap()
		bm.SetMany([]uint64{1, 2, 3})

		cloned, release := pool.CloneToBuf(bm)
		assert.Equal(t, int64(1), pool.Outstanding())
		assert.Equal(t, bm.ToArray(), cloned.ToArray())

		release()
		assert.Equal(t, int64(0), pool.Outstanding())
	})

	t.Run("second put of a cloned bitmap panics", func(t *testing.T) {
		pool := NewBitmapBufPoolTrackingForTests()
		bm := sroar.NewBitmap()
		bm.Set(1)
		_, release := pool.CloneToBuf(bm)
		release()
		assert.PanicsWithValue(t, "bitmap released twice", release)
	})

	t.Run("release zeroes cloned bitmap backing buffer", func(t *testing.T) {
		pool := NewBitmapBufPoolTrackingForTests()

		bm := sroar.NewBitmap()
		bm.SetMany([]uint64{10, 20, 30})

		cloned, release := pool.CloneToBuf(bm)
		assert.False(t, cloned.IsEmpty())

		// ToBuffer returns a view into the backing memory while the bitmap is
		// non-empty. Capture it before release so we can inspect it afterward.
		full := cloned.ToBuffer()
		release()

		// Backing array must be zeroed after release.
		for i, b := range full {
			if b != 0 {
				t.Errorf("buf[%d] = %d after release, want 0", i, b)
			}
		}
	})
}

// TestPooledBitmapStructReuse pins that a struct still held is never handed out
// again, and that put leaves nothing of a grown bitmap in it.
func TestPooledBitmapStructReuse(t *testing.T) {
	// syncMaxBufSize 0 puts every range on the in-memory tier. Its stack returns
	// the buffer put last, the budget keeps it, and NewBitmapBufPoolRanged starts
	// no decay ticker, so buffers are reused deterministically.
	newRanged := func() *bitmapBufPoolRanged {
		return NewBitmapBufPoolRanged(nil, 0, testIdleBudget, 512, 1024)
	}

	src := sroar.NewBitmap()
	src.SetMany([]uint64{1, 2, 3, 100_000})
	serialized := slices.Clone(src.ToBuffer())

	t.Run("a held struct is not handed out again", func(t *testing.T) {
		pool := newRanged()
		bm1, put1 := pool.CloneBytesToBuf(serialized)
		bm2, put2 := pool.CloneToBuf(src)
		bm3, put3 := pool.AccumulatorToBuf(sroar.NewAccumulator())
		defer put1()
		defer put2()
		defer put3()
		assert.NotSame(t, bm1, bm2)
		assert.NotSame(t, bm1, bm3)
		assert.NotSame(t, bm2, bm3)
		assert.Equal(t, src.ToArray(), bm1.ToArray())
		assert.Equal(t, src.ToArray(), bm2.ToArray())
	})

	t.Run("growth past the buffer cannot corrupt the next checkout", func(t *testing.T) {
		pool := newRanged()
		bm1, put1 := pool.CloneBytesToBuf(serialized)
		for i := uint64(0); i < 100_000; i += 7 {
			bm1.Set(i)
		}
		put1()
		bm2, put2 := pool.CloneBytesToBuf(serialized)
		defer put2()
		require.Equal(t, src.ToArray(), bm2.ToArray(),
			"reused entry must reflect only the new clone, not the grown bitmap")
	})

	t.Run("put resets the struct", func(t *testing.T) {
		// A parked struct still pointing at a grown bitmap's memory would pin it
		// for as long as the struct sits in the pool.
		pools := []struct {
			name string
			pool BitmapBufPool
		}{
			{"idle-stack-backed", NewBitmapBufPoolRanged(nil, 0, testIdleBudget, 512, 1024)},
			{"sync.Pool-backed", NewBitmapBufPoolRanged(nil, 1<<20, testIdleBudget, 512, 1024)},
			{"oversized, on the heap", NewBitmapBufPoolRanged(nil, 0, testIdleBudget, 8)},
		}
		for _, tc := range pools {
			t.Run(tc.name, func(t *testing.T) {
				bm, put := tc.pool.CloneBytesToBuf(serialized)
				for i := uint64(0); i < 100_000; i += 7 {
					bm.Set(i)
				}
				require.NotZero(t, bm.GetCardinality())
				put()
				assert.Equal(t, sroar.Bitmap{}, *bm,
					"released struct must not retain the grown bitmap's memory")
			})
		}
	})

	t.Run("clone allocates nothing steady-state", func(t *testing.T) {
		pool := newRanged()
		_, put := pool.CloneBytesToBuf(serialized)
		put()
		// A sync.Pool miss allocates a struct, but never on every run.
		allocs := testing.AllocsPerRun(100, func() {
			bm, put := pool.CloneBytesToBuf(serialized)
			_ = bm.GetCardinality()
			put()
		})
		assert.Zero(t, allocs, "pooled clone must reuse both buffer and Bitmap struct")
	})
}

// dirtyTrackingPool hands out buffers filled with garbage, as reused pool
// buffers are, and tracks them like the pool it wraps.
type dirtyTrackingPool struct {
	*BitmapBufPoolTrackingForTests
	gets int
}

func (p *dirtyTrackingPool) Get(minCap int) ([]byte, func()) {
	p.gets++
	buf, put := p.BitmapBufPoolTrackingForTests.Get(minCap)
	full := buf[:cap(buf)]
	for i := range full {
		full[i] = 0xA5
	}
	return buf, put
}

func (p *dirtyTrackingPool) CloneToBuf(bm *sroar.Bitmap) (*sroar.Bitmap, func()) {
	return cloneToBuf(p, bm)
}

func (p *dirtyTrackingPool) CloneBytesToBuf(src []byte) (*sroar.Bitmap, func()) {
	return cloneBytesToBuf(p, src)
}

func (p *dirtyTrackingPool) AccumulatorToBuf(acc *sroar.Accumulator) (*sroar.Bitmap, func()) {
	return accumulatorToBuf(p, acc)
}

// TestPooledBitmapGrowsThroughThePool pins that growth takes dirty buffers from
// the pool, releases each outgrown one exactly once, and matches the heap result.
func TestPooledBitmapGrowsThroughThePool(t *testing.T) {
	base := []uint64{1, 2, 3, 70_000}
	baseBM := func() *sroar.Bitmap {
		bm := sroar.NewBitmap()
		bm.SetMany(base)
		return bm
	}
	dense := sroar.NewBitmap()
	for i := uint64(0); i < 300_000; i += 3 {
		dense.Set(i)
	}

	constructors := []struct {
		name string
		get  func(BitmapBufPool) (*sroar.Bitmap, func())
	}{
		{"CloneToBuf", func(p BitmapBufPool) (*sroar.Bitmap, func()) { return p.CloneToBuf(baseBM()) }},
		{"CloneBytesToBuf", func(p BitmapBufPool) (*sroar.Bitmap, func()) {
			return p.CloneBytesToBuf(slices.Clone(baseBM().ToBuffer()))
		}},
		{"AccumulatorToBuf", func(p BitmapBufPool) (*sroar.Bitmap, func()) {
			acc := sroar.NewAccumulator()
			acc.Or(baseBM())
			return p.AccumulatorToBuf(acc)
		}},
	}
	ops := []struct {
		name string
		grow func(bm *sroar.Bitmap)
	}{
		{"Set one by one", func(bm *sroar.Bitmap) {
			for i := uint64(0); i < 200_000; i += 5 {
				bm.Set(i)
			}
		}},
		{"Or", func(bm *sroar.Bitmap) { bm.Or(dense) }},
		{"OrConc", func(bm *sroar.Bitmap) { bm.OrConc(dense, 4) }},
		{"Or then AndNot", func(bm *sroar.Bitmap) {
			bm.Or(dense)
			bm.AndNot(baseBM())
		}},
	}

	for _, c := range constructors {
		for _, op := range ops {
			t.Run(c.name+"/"+op.name, func(t *testing.T) {
				want := baseBM()
				op.grow(want)

				pool := &dirtyTrackingPool{BitmapBufPoolTrackingForTests: NewBitmapBufPoolTrackingForTests()}
				bm, put := c.get(pool)
				getsBefore := pool.gets
				op.grow(bm)

				require.Greater(t, pool.gets, getsBefore, "the bitmap must have grown through the pool")
				require.Equal(t, int64(1), pool.Outstanding(), "every outgrown buffer must be released")
				require.Equal(t, want.ToArray(), bm.ToArray())
				put()
				require.Equal(t, int64(0), pool.Outstanding())
			})
		}
	}
}

// TestPooledBitmapGrowthAllocatesNothing pins that a warm pool serves every
// grown size inside its ranges, with no heap or disposable buffer.
func TestPooledBitmapGrowthAllocatesNothing(t *testing.T) {
	disposable := &countingDisposableMetrics{}
	pool := NewBitmapBufPoolRanged(nil, 0, testIdleBudget, calculateSyncBufferRanges(9, 20)...)
	pool.disposableMetrics = disposable
	src := sroar.NewBitmap()
	src.Set(1)

	// One value per key grows the buffer with every new container, and never
	// converts an array container, which sroar does on the heap.
	grow := func() {
		bm, put := pool.CloneToBuf(src)
		for i := uint64(1); i < 4000; i++ {
			bm.Set(i << 16)
		}
		put()
	}
	grow()
	allocs := testing.AllocsPerRun(100, grow)
	assert.Zero(t, allocs)
	assert.Zero(t, disposable.created)
}

type countingDisposableMetrics struct{ created int }

func (m *countingDisposableMetrics) bufCreated(int) { m.created++ }

func TestAccumulatorToBuf(t *testing.T) {
	ids := []uint64{1, 2, 3, 70_000, 200_000}
	newAcc := func() *sroar.Accumulator {
		acc := sroar.NewAccumulator()
		bm := sroar.NewBitmap()
		bm.SetMany(ids)
		acc.Or(bm)
		return acc
	}

	pools := []struct {
		name string
		pool BitmapBufPool
	}{
		{"noop", NewBitmapBufPoolNoop()},
		{"ranged idle-stack-backed", NewBitmapBufPoolRanged(nil, 0, testIdleBudget, 512, 1024)},
		{"ranged sync.Pool-backed", NewBitmapBufPoolRanged(nil, 1<<20, testIdleBudget, 512, 1024)},
		{"tracking", NewBitmapBufPoolTrackingForTests()},
	}
	for _, tc := range pools {
		t.Run(tc.name, func(t *testing.T) {
			acc := newAcc()
			bm, put := tc.pool.AccumulatorToBuf(acc)
			require.NotNil(t, put, "put must always be callable")
			require.Equal(t, acc.Bitmap().ToArray(), bm.ToArray())
			put()
		})
	}

	t.Run("empty accumulator yields an empty bitmap", func(t *testing.T) {
		pool := NewBitmapBufPoolRanged(nil, 0, testIdleBudget, 512, 1024)
		bm, put := pool.AccumulatorToBuf(sroar.NewAccumulator())
		require.NotNil(t, put, "put must always be callable")
		defer put()
		require.NotNil(t, bm)
		require.True(t, bm.IsEmpty())
	})

	t.Run("nil accumulator panics, as documented", func(t *testing.T) {
		pool := NewBitmapBufPoolRanged(nil, 0, testIdleBudget, 512, 1024)
		require.Panics(t, func() { pool.AccumulatorToBuf(nil) })
	})

	t.Run("allocates nothing warm", func(t *testing.T) {
		pool := NewBitmapBufPoolRanged(nil, 0, testIdleBudget, 512, 1024)
		acc := newAcc()
		_, put := pool.AccumulatorToBuf(acc)
		put()

		allocs := testing.AllocsPerRun(100, func() {
			bm, put := pool.AccumulatorToBuf(acc)
			_ = bm.GetCardinality()
			put()
		})
		assert.Zero(t, allocs, "warm materialization must reuse both buffer and Bitmap struct")
	})

	t.Run("growth past the buffer cannot corrupt the next checkout", func(t *testing.T) {
		pool := NewBitmapBufPoolRanged(nil, 0, testIdleBudget, 512, 1024)
		acc := newAcc()
		bm1, put1 := pool.AccumulatorToBuf(acc)
		for i := uint64(0); i < 100_000; i += 7 {
			bm1.Set(i)
		}
		put1()
		bm2, put2 := pool.AccumulatorToBuf(acc)
		defer put2()
		require.Equal(t, ids, bm2.ToArray(),
			"reused entry must reflect only the fresh union, not the grown bitmap")
	})
}

// TestCloneBytesToBuf_OddLengthPanics pins the corrupt-input guard: an
// odd-length src — always corruption, sroar serializations are []uint16-based
// — must fail with a recoverable panic naming the length. Without the guard
// the input reaches sroar's even-length assert, a log.Fatal that kills the
// whole process over one corrupt segment region.
func TestCloneBytesToBuf_OddLengthPanics(t *testing.T) {
	pool := NewBitmapBufPoolNoop()

	// call through a variable: the deliberately corrupt argument would
	// otherwise trip staticcheck's serialized-bitmap length analysis
	clone := pool.CloneBytesToBuf
	require.PanicsWithValue(t,
		"roaringset: corrupt serialized bitmap: odd length 9",
		func() { clone(make([]byte, 9)) })
}

// mirrors the real LSMBitmapBuffersUsage definition, but unregistered so each
// test observes only its own increments.
func newTestBitmapBuffersUsage() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lsm_bitmap_buffers_usage",
		Help: "Number of bitmap buffers used by size",
	}, []string{"size", "operation"})
}

func expectedDisposableCreated(sizeLabel string) string {
	return fmt.Sprintf(`
# HELP lsm_bitmap_buffers_usage Number of bitmap buffers used by size
# TYPE lsm_bitmap_buffers_usage counter
lsm_bitmap_buffers_usage{operation="disposable_created",size="%s"} 1
`, sizeLabel)
}

func TestPromBufDisposableMetricsSizeLabel(t *testing.T) {
	tests := []struct {
		name        string
		sizeInBytes int
		sizeLabel   string
	}{
		{
			name:        "exactly a power of two",
			sizeInBytes: 32 << 20,
			sizeLabel:   "32 MiB",
		},
		{
			name:        "one byte above a power of two",
			sizeInBytes: 32<<20 + 1,
			sizeLabel:   "64 MiB",
		},
		{
			name:        "one byte below a power of two",
			sizeInBytes: 64<<20 - 1,
			sizeLabel:   "64 MiB",
		},
		{
			name:        "between two powers of two",
			sizeInBytes: 100_000_000,
			sizeLabel:   "128 MiB",
		},
		{
			name:        "smallest buffer",
			sizeInBytes: 1,
			sizeLabel:   "1 B",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			counter := newTestBitmapBuffersUsage()
			metrics := newPromBufDisposableMetrics(&monitoring.PrometheusMetrics{
				LSMBitmapBuffersUsage: counter,
			})

			metrics.bufCreated(test.sizeInBytes)

			require.NoError(t, testutil.CollectAndCompare(counter,
				strings.NewReader(expectedDisposableCreated(test.sizeLabel))))
		})
	}
}

func TestBitmapBufPoolRangedDisposableMetrics(t *testing.T) {
	// 2048 is above the largest range, so it is served by an unpooled buffer
	// and labelled with the size class it would have needed.
	counter := newTestBitmapBuffersUsage()
	pool := NewBitmapBufPoolRanged(&monitoring.PrometheusMetrics{
		LSMBitmapBuffersUsage: counter,
	}, 1024, testIdleBudget, 512, 1024)

	_, put := pool.Get(2048)
	put()

	require.NoError(t, testutil.CollectAndCompare(counter,
		strings.NewReader(expectedDisposableCreated("2.0 KiB"))))
}

func TestBitmapBufPoolRangedInMemoMetrics(t *testing.T) {
	// Both ranges are on the in-memory tier, so the 1 KiB put evicts both 512 B buffers.
	counter := newTestBitmapBuffersUsage()
	pool := NewBitmapBufPoolRanged(&monitoring.PrometheusMetrics{
		LSMBitmapBuffersUsage: counter,
	}, 256, 1024, 512, 1024)

	_, putA := pool.Get(512)
	_, putB := pool.Get(512)
	_, putC := pool.Get(1024)
	putA()
	putB()
	putC()

	assert.Equal(t, 2.0, testutil.ToFloat64(counter.WithLabelValues("512 B", "inmemo_evicted")))
}
