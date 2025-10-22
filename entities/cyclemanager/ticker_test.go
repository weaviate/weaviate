//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cyclemanager

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_FixedIntervalTicker(t *testing.T) {
	t.Run("channel is empty before started", func(t *testing.T) {
		interval := time.Second

		synctest.Test(t, func(t *testing.T) {
			ticker := NewFixedTicker(interval)

			assertNoTick(t, ticker.C())

			ticker.Start()
			time.Sleep(interval)
			synctest.Wait()

			assertTick(t, ticker.C())

			ticker.Stop()
		})
	})

	t.Run("interval is fixed", func(t *testing.T) {
		interval := time.Second
		delta := 100 * time.Millisecond
		count := 4
		times := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewFixedTicker(interval)
			ticker.Start()

			times[0] = time.Now()
			for i := range count {
				time.Sleep(interval + delta)
				synctest.Wait()

				times[i+1] = <-ticker.C()
			}

			ticker.Stop()
		})

		for i := range count {
			assert.Equal(t, interval, times[i+1].Sub(times[i]))
		}
	})

	t.Run("interval does not change on CycleExecuted call", func(t *testing.T) {
		interval := time.Second
		delta := 100 * time.Millisecond
		count := 4
		times1 := make([]time.Time, count+1)
		times2 := make([]time.Time, count+1)
		times3 := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewFixedTicker(interval)
			ticker.Start()

			times1[0] = time.Now()
			for i := range count {
				time.Sleep(interval + delta)
				synctest.Wait()

				times1[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false)
			times2[0] = time.Now()
			for i := range count {
				time.Sleep(interval + delta)
				synctest.Wait()

				times2[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(true)
			times3[0] = time.Now()
			for i := range count {
				time.Sleep(interval + delta)
				synctest.Wait()

				times3[i+1] = <-ticker.C()
			}

			ticker.Stop()
		})

		for _, times := range [][]time.Time{times1, times2, times3} {
			for i := range count {
				assert.Equal(t, interval, times[i+1].Sub(times[i]))
			}
		}
	})

	t.Run("no ticks after stop", func(t *testing.T) {
		interval := time.Second
		delta := 100 * time.Millisecond
		count := 4
		times := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewFixedTicker(interval)
			ticker.Start()

			times[0] = time.Now()
			for i := range count {
				time.Sleep(interval + delta)
				synctest.Wait()

				times[i+1] = <-ticker.C()
			}

			ticker.Stop()

			time.Sleep(interval + delta)
			synctest.Wait()

			assertNoTick(t, ticker.C())
		})

		for i := range count {
			assert.Equal(t, interval, times[i+1].Sub(times[i]))
		}
	})

	t.Run("ticker starts again", func(t *testing.T) {
		interval := time.Second
		delta := 100 * time.Millisecond
		count := 4
		times1 := make([]time.Time, count+1)
		times2 := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewFixedTicker(interval)
			ticker.Start()

			times1[0] = time.Now()
			for i := range count {
				time.Sleep(interval + delta)
				synctest.Wait()

				times1[i+1] = <-ticker.C()
			}

			ticker.Stop()
			ticker.Start()

			times2[0] = time.Now()
			for i := range count {
				time.Sleep(interval + delta)
				synctest.Wait()

				times2[i+1] = <-ticker.C()
			}

			ticker.Stop()
		})

		for _, times := range [][]time.Time{times1, times2} {
			for i := range count {
				assert.Equal(t, interval, times[i+1].Sub(times[i]))
			}
		}
	})

	t.Run("ticker does not run with <= 0 interval", func(t *testing.T) {
		interval := time.Duration(0)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewFixedTicker(interval)
			ticker.Start()

			time.Sleep(time.Minute)
			synctest.Wait()

			assertNoTick(t, ticker.C())

			ticker.Stop()
		})
	})
}

func Test_SeriesTicker(t *testing.T) {
	t.Run("channel is empty before started", func(t *testing.T) {
		intervals := []time.Duration{time.Second, 2 * time.Second}

		synctest.Test(t, func(t *testing.T) {
			ticker := NewSeriesTicker(intervals)

			assertNoTick(t, ticker.C())

			ticker.Start()
			time.Sleep(intervals[1])
			synctest.Wait()

			assertTick(t, ticker.C())

			ticker.Stop()
		})
	})

	t.Run("interval is fixed between CycleExecuted calls, advances on false, resets on true", func(t *testing.T) {
		intervals := []time.Duration{1000 * time.Millisecond, 1500 * time.Millisecond, 2000 * time.Millisecond}
		delta := 100 * time.Millisecond
		count := 4
		times1 := make([]time.Time, count+1)
		times2 := make([]time.Time, count+1)
		times3 := make([]time.Time, count+1)
		times4 := make([]time.Time, count+1)
		times5 := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewSeriesTicker(intervals)
			ticker.Start()

			times1[0] = time.Now()
			for i := range count {
				time.Sleep(intervals[0] + delta)
				synctest.Wait()

				times1[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false) // moving to 2nd interval
			times2[0] = time.Now()
			for i := range count {
				time.Sleep(intervals[1] + delta)
				synctest.Wait()

				times2[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false) // moving to 3rd interval
			times3[0] = time.Now()
			for i := range count {
				time.Sleep(intervals[2] + delta)
				synctest.Wait()

				times3[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false) // using 3rd interval (as it is last)
			times4[0] = time.Now()
			for i := range count {
				time.Sleep(intervals[2] + delta)
				synctest.Wait()

				times4[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(true) // moving to 1st interval
			times5[0] = time.Now()
			for i := range count {
				time.Sleep(intervals[0] + delta)
				synctest.Wait()

				times5[i+1] = <-ticker.C()
			}

			ticker.Stop()
		})

		for _, times := range [][]time.Time{times1, times5} {
			for i := range count {
				assert.Equal(t, intervals[0], times[i+1].Sub(times[i]))
			}
		}
		for i := range count {
			assert.Equal(t, intervals[1], times2[i+1].Sub(times2[i]))
		}
		for _, times := range [][]time.Time{times3, times4} {
			for i := range count {
				assert.Equal(t, intervals[2], times[i+1].Sub(times[i]))
			}
		}
	})

	t.Run("no ticks after stop", func(t *testing.T) {
		intervals := []time.Duration{time.Second, 2 * time.Second}
		delta := 100 * time.Millisecond
		count := 4
		times := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewSeriesTicker(intervals)
			ticker.Start()

			times[0] = time.Now()
			for i := range count {
				time.Sleep(intervals[0] + delta)
				synctest.Wait()

				times[i+1] = <-ticker.C()
			}

			ticker.Stop()

			time.Sleep(intervals[1] + delta)
			synctest.Wait()

			assertNoTick(t, ticker.C())
		})

		for i := range count {
			assert.Equal(t, intervals[0], times[i+1].Sub(times[i]))
		}
	})

	t.Run("ticker starts again", func(t *testing.T) {
		intervals := []time.Duration{time.Second}
		delta := 100 * time.Millisecond
		count := 4
		times1 := make([]time.Time, count+1)
		times2 := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewSeriesTicker(intervals)
			ticker.Start()

			times1[0] = time.Now()
			for i := range count {
				time.Sleep(intervals[0] + delta)
				synctest.Wait()

				times1[i+1] = <-ticker.C()
			}

			ticker.Stop()
			ticker.Start()

			times2[0] = time.Now()
			for i := range count {
				time.Sleep(intervals[0] + delta)
				synctest.Wait()

				times2[i+1] = <-ticker.C()
			}

			ticker.Stop()
		})

		for _, times := range [][]time.Time{times1, times2} {
			for i := range count {
				assert.Equal(t, intervals[0], times[i+1].Sub(times[i]))
			}
		}
	})

	t.Run("ticker does not run with invalid params", func(t *testing.T) {
		run := func(t *testing.T, intervals []time.Duration) {
			t.Helper()

			synctest.Test(t, func(t *testing.T) {
				ticker := NewSeriesTicker(intervals)
				ticker.Start()

				time.Sleep(time.Minute)
				synctest.Wait()

				assertNoTick(t, ticker.C())

				ticker.Stop()
			})
		}

		t.Run("any interval <= 0", func(t *testing.T) {
			run(t, []time.Duration{time.Second, 0})
		})

		t.Run("no intervals", func(t *testing.T) {
			run(t, []time.Duration{})
		})
	})
}

func Test_LinearTicker(t *testing.T) {
	t.Run("channel is empty before started", func(t *testing.T) {
		minInterval := 1 * time.Second
		maxInterval := 2 * time.Second
		steps := uint(2)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewLinearTicker(minInterval, maxInterval, steps)

			assertNoTick(t, ticker.C())

			ticker.Start()
			time.Sleep(maxInterval)
			synctest.Wait()

			assertTick(t, ticker.C())

			ticker.Stop()
		})
	})

	t.Run("interval is fixed between CycleExecuted calls, advances on false, resets on true", func(t *testing.T) {
		minInterval := 1000 * time.Millisecond
		maxInterval := 2000 * time.Millisecond
		steps := uint(2)
		middleInterval := 1500 * time.Millisecond
		delta := 100 * time.Millisecond
		count := 4
		times1 := make([]time.Time, count+1)
		times2 := make([]time.Time, count+1)
		times3 := make([]time.Time, count+1)
		times4 := make([]time.Time, count+1)
		times5 := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewLinearTicker(minInterval, maxInterval, steps)
			ticker.Start()

			times1[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times1[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false) // moving to middle interval (1.5 s)
			times2[0] = time.Now()
			for i := range count {
				time.Sleep(middleInterval + delta)
				synctest.Wait()

				times2[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false) // moving to max interval
			times3[0] = time.Now()
			for i := range count {
				time.Sleep(maxInterval + delta)
				synctest.Wait()

				times3[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false) // using max interval
			times4[0] = time.Now()
			for i := range count {
				time.Sleep(maxInterval + delta)
				synctest.Wait()

				times4[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(true) // moving to min interval
			times5[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times5[i+1] = <-ticker.C()
			}

			ticker.Stop()
		})

		for _, times := range [][]time.Time{times1, times5} {
			for i := range count {
				assert.Equal(t, minInterval, times[i+1].Sub(times[i]))
			}
		}
		for i := range count {
			assert.Equal(t, middleInterval, times2[i+1].Sub(times2[i]))
		}
		for _, times := range [][]time.Time{times3, times4} {
			for i := range count {
				assert.Equal(t, maxInterval, times[i+1].Sub(times[i]))
			}
		}
	})

	t.Run("no ticks after stop", func(t *testing.T) {
		minInterval := 1 * time.Second
		maxInterval := 2 * time.Second
		steps := uint(2)
		delta := 100 * time.Millisecond
		count := 4
		times := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewLinearTicker(minInterval, maxInterval, steps)
			ticker.Start()

			times[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times[i+1] = <-ticker.C()
			}

			ticker.Stop()

			time.Sleep(maxInterval + delta)
			synctest.Wait()

			assertNoTick(t, ticker.C())
		})
	})

	t.Run("ticker starts again", func(t *testing.T) {
		minInterval := 1 * time.Second
		maxInterval := 2 * time.Second
		steps := uint(2)
		delta := 100 * time.Millisecond
		count := 4
		times1 := make([]time.Time, count+1)
		times2 := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewLinearTicker(minInterval, maxInterval, steps)
			ticker.Start()

			times1[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times1[i+1] = <-ticker.C()
			}

			ticker.Stop()
			ticker.Start()

			times2[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times2[i+1] = <-ticker.C()
			}

			ticker.Stop()
		})

		for _, times := range [][]time.Time{times1, times2} {
			for i := range count {
				assert.Equal(t, minInterval, times[i+1].Sub(times[i]))
			}
		}
	})

	t.Run("ticker does not run with invalid params", func(t *testing.T) {
		run := func(t *testing.T, minInterval, maxInterval time.Duration, steps uint) {
			t.Helper()

			synctest.Test(t, func(t *testing.T) {
				ticker := NewLinearTicker(minInterval, maxInterval, steps)
				ticker.Start()

				time.Sleep(time.Minute)
				synctest.Wait()

				assertNoTick(t, ticker.C())

				ticker.Stop()
			})
		}

		t.Run("minInterval <= 0", func(t *testing.T) {
			run(t, 0, time.Second, 1)
		})

		t.Run("maxInterval <= 0", func(t *testing.T) {
			run(t, time.Second, 0, 1)
		})

		t.Run("steps = 0", func(t *testing.T) {
			run(t, time.Second, 2*time.Second, 0)
		})

		t.Run("minInterval > maxInterval", func(t *testing.T) {
			run(t, 2*time.Second, time.Second, 1)
		})
	})
}

func Test_ExpTicker(t *testing.T) {
	t.Run("channel is empty before started", func(t *testing.T) {
		minInterval := 1 * time.Second
		maxInterval := 2 * time.Second
		base := uint(2)
		steps := uint(2)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewExpTicker(minInterval, maxInterval, base, steps)

			assertNoTick(t, ticker.C())

			ticker.Start()
			time.Sleep(maxInterval)
			synctest.Wait()

			assertTick(t, ticker.C())

			ticker.Stop()
		})
	})

	t.Run("interval is fixed between CycleExecuted calls, advances on false, resets on true", func(t *testing.T) {
		minInterval := 1250 * time.Millisecond
		maxInterval := 2000 * time.Millisecond
		base := uint(2)
		steps := uint(2)
		middleInterval := 1500 * time.Millisecond
		delta := 100 * time.Millisecond
		count := 4
		times1 := make([]time.Time, count+1)
		times2 := make([]time.Time, count+1)
		times3 := make([]time.Time, count+1)
		times4 := make([]time.Time, count+1)
		times5 := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewExpTicker(minInterval, maxInterval, base, steps)
			ticker.Start()

			times1[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times1[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false) // moving to middle interval (1.5 s)
			times2[0] = time.Now()
			for i := range count {
				time.Sleep(middleInterval + delta)
				synctest.Wait()

				times2[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false) // moving to max interval
			times3[0] = time.Now()
			for i := range count {
				time.Sleep(maxInterval + delta)
				synctest.Wait()

				times3[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(false) // using max interval
			times4[0] = time.Now()
			for i := range count {
				time.Sleep(maxInterval + delta)
				synctest.Wait()

				times4[i+1] = <-ticker.C()
			}

			ticker.CycleExecuted(true) // moving to min interval
			times5[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times5[i+1] = <-ticker.C()
			}

			ticker.Stop()
		})

		for _, times := range [][]time.Time{times1, times5} {
			for i := range count {
				assert.Equal(t, minInterval, times[i+1].Sub(times[i]))
			}
		}
		for i := range count {
			assert.Equal(t, middleInterval, times2[i+1].Sub(times2[i]))
		}
		for _, times := range [][]time.Time{times3, times4} {
			for i := range count {
				assert.Equal(t, maxInterval, times[i+1].Sub(times[i]))
			}
		}
	})

	t.Run("no ticks after stop", func(t *testing.T) {
		minInterval := 1 * time.Second
		maxInterval := 2 * time.Second
		base := uint(2)
		steps := uint(2)
		delta := 100 * time.Millisecond
		count := 4
		times := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewExpTicker(minInterval, maxInterval, base, steps)
			ticker.Start()

			times[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times[i+1] = <-ticker.C()
			}

			ticker.Stop()

			time.Sleep(maxInterval + delta)
			synctest.Wait()

			assertNoTick(t, ticker.C())
		})
	})

	t.Run("ticker starts again", func(t *testing.T) {
		minInterval := 1 * time.Second
		maxInterval := 2 * time.Second
		base := uint(2)
		steps := uint(2)
		delta := 100 * time.Millisecond
		count := 4
		times1 := make([]time.Time, count+1)
		times2 := make([]time.Time, count+1)

		synctest.Test(t, func(t *testing.T) {
			ticker := NewExpTicker(minInterval, maxInterval, base, steps)
			ticker.Start()

			times1[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times1[i+1] = <-ticker.C()
			}

			ticker.Stop()
			ticker.Start()

			times2[0] = time.Now()
			for i := range count {
				time.Sleep(minInterval + delta)
				synctest.Wait()

				times2[i+1] = <-ticker.C()
			}

			ticker.Stop()
		})

		for _, times := range [][]time.Time{times1, times2} {
			for i := range count {
				assert.Equal(t, minInterval, times[i+1].Sub(times[i]))
			}
		}
	})

	t.Run("ticker does not run with invalid params", func(t *testing.T) {
		run := func(t *testing.T, minInterval, maxInterval time.Duration, base, steps uint) {
			t.Helper()

			synctest.Test(t, func(t *testing.T) {
				ticker := NewExpTicker(minInterval, maxInterval, base, steps)
				ticker.Start()

				time.Sleep(time.Minute)
				synctest.Wait()

				assertNoTick(t, ticker.C())

				ticker.Stop()
			})
		}

		t.Run("minInterval <= 0", func(t *testing.T) {
			run(t, 0, time.Second, 2, 2)
		})

		t.Run("maxInterval <= 0", func(t *testing.T) {
			run(t, time.Second, 0, 2, 2)
		})

		t.Run("base == 0", func(t *testing.T) {
			run(t, time.Second, 2*time.Second, 0, 2)
		})

		t.Run("steps = 0", func(t *testing.T) {
			run(t, time.Second, 2*time.Second, 2, 0)
		})

		t.Run("minInterval > maxInterval", func(t *testing.T) {
			run(t, 2*time.Second, time.Second, 2, 2)
		})
	})
}

func Test_LinearToIntervals(t *testing.T) {
	type testCase struct {
		name        string
		minInterval time.Duration
		maxInterval time.Duration
		steps       uint
		expected    []time.Duration
	}

	testCases := []testCase{
		{
			name:        "100 => 5000; steps 2",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			steps:       2,
			expected: []time.Duration{
				100_000_000,
				2_550_000_000,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; steps 3",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			steps:       3,
			expected: []time.Duration{
				100_000_000,
				1_733_333_333,
				3_366_666_666,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; steps 4",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			steps:       4,
			expected: []time.Duration{
				100_000_000,
				1_325_000_000,
				2_550_000_000,
				3_775_000_000,
				5_000_000_000,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := linearToIntervals(tc.minInterval, tc.maxInterval, tc.steps)

			assert.ElementsMatch(t, res, tc.expected)
		})
	}
}

func Test_ExpToIntervals(t *testing.T) {
	type testCase struct {
		name        string
		minInterval time.Duration
		maxInterval time.Duration
		base        uint
		steps       uint
		expected    []time.Duration
	}

	testCases := []testCase{
		{
			name:        "100 => 5000; base 2; steps 2",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        2,
			steps:       2,
			expected: []time.Duration{
				100_000_000,
				1_733_333_333,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 2; steps 3",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        2,
			steps:       3,
			expected: []time.Duration{
				100_000_000,
				800_000_000,
				2_200_000_000,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 2; steps 4",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        2,
			steps:       4,
			expected: []time.Duration{
				100_000_000,
				426_666_666,
				1_080_000_000,
				2_386_666_666,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 3; steps 2",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        3,
			steps:       2,
			expected: []time.Duration{
				100_000_000,
				1_325_000_000,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 3; steps 3",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        3,
			steps:       3,
			expected: []time.Duration{
				100_000_000,
				476_923_076,
				1_607_692_307,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 3; steps 4",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        3,
			steps:       4,
			expected: []time.Duration{
				100_000_000,
				222_500_000,
				590_000_000,
				1_692_500_000,
				5_000_000_000,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := expToIntervals(tc.minInterval, tc.maxInterval, tc.base, tc.steps)

			assert.ElementsMatch(t, res, tc.expected)
		})
	}
}

func assertTick(t *testing.T, tickCh <-chan time.Time) {
	select {
	case <-tickCh:
	default:
		assert.Fail(t, "should have tick")
	}
}

func assertNoTick(t *testing.T, tickCh <-chan time.Time) {
	select {
	case <-tickCh:
		assert.Fail(t, "should not have tick")
	default:
	}
}
