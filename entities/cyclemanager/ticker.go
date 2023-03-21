//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cyclemanager

import (
	"time"
)

type CycleTicker interface {
	Start()
	Stop()
	C() <-chan time.Time
	// called with bool value whenever cycle function finished execution
	// true - indicates cycle function actually did some processing
	// false - cycle function returned without doing anything
	CycleExecuted(executed bool)
}

type FixedIntervalTicker struct {
	interval time.Duration
	ticker   *time.Ticker
}

// Creates ticker with fixed interval. Interval is not changed regardless
// of execution results reported by cycle function
//
// If interval <= 0 given, ticker will not fire
func NewFixedIntervalTicker(interval time.Duration) CycleTicker {
	// for testing purposes allow interval of 0,
	// meaning cycle manager not working
	if interval <= 0 {
		return NewNoopTicker()
	}

	return newFixedIntervalTicker(interval)
}

func newFixedIntervalTicker(interval time.Duration) CycleTicker {
	ticker := time.NewTicker(time.Second)
	ticker.Stop()
	return &FixedIntervalTicker{
		interval: interval,
		ticker:   ticker,
	}
}

func (t *FixedIntervalTicker) Start() {
	t.ticker.Reset(t.interval)
}

func (t *FixedIntervalTicker) Stop() {
	t.ticker.Stop()
}

func (t *FixedIntervalTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *FixedIntervalTicker) CycleExecuted(executed bool) {
	t.ticker.Reset(t.interval)
}

type SeriesTicker struct {
	intervals   []time.Duration
	intervalPos int
	ticker      *time.Ticker
}

// Creates ticker with set of interval values.
// Ticker starts with intervals[0] value and with every report of executed "false"
// changes interval value to next one in given array up until last one.
// Report of executed "true" resets interval to interval[0]
//
// If any of intervals given is <= 0 given, ticker will not fire
func NewSeriesTicker(intervals []time.Duration) CycleTicker {
	if len(intervals) == 0 {
		return NewNoopTicker()
	}

	allSame := true
	for i := range intervals {
		if intervals[i] <= 0 {
			return NewNoopTicker()
		}
		if intervals[i] != intervals[0] {
			allSame = false
		}
	}
	if allSame {
		return newFixedIntervalTicker(intervals[0])
	}

	return newSeriesTicker(intervals)
}

func newSeriesTicker(intervals []time.Duration) CycleTicker {
	ticker := time.NewTicker(time.Second)
	ticker.Stop()
	return &SeriesTicker{
		intervals:   intervals,
		intervalPos: 0,
		ticker:      ticker,
	}
}

func (t *SeriesTicker) Start() {
	t.intervalPos = 0
	t.ticker.Reset(t.intervals[t.intervalPos])
}

func (t *SeriesTicker) Stop() {
	t.ticker.Stop()
}

func (t *SeriesTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *SeriesTicker) CycleExecuted(executed bool) {
	if executed {
		t.intervalPos = 0
	} else if t.intervalPos < len(t.intervals)-1 {
		t.intervalPos++
	}
	t.ticker.Reset(t.intervals[t.intervalPos])
}

// Creates ticker with intervals between minInterval and maxInterval values.
// Number of intervals in-between is determined by steps value.
// Ticker starts with minInterval value and with every report of executed "false"
// changes interval value to next one, up until maxInterval.
// Report of executed "true" resets interval to minInterval
// Example: for minInterval = 100ms, maxInterval = 5s, steps = 4, intervals are
// 100ms . 1325ms . 2550ms . 3775ms . 5000ms
//
// If min- or maxInterval is <= 0 or steps = 0 or min > maxInterval, ticker will not fire
func NewLinearTicker(minInterval, maxInterval time.Duration, steps uint) CycleTicker {
	if minInterval <= 0 || maxInterval <= 0 || steps == 0 || minInterval > maxInterval {
		return NewNoopTicker()
	}
	if minInterval == maxInterval {
		return newFixedIntervalTicker(minInterval)
	}

	return newLinearTicker(minInterval, maxInterval, steps)
}

func newLinearTicker(minInterval, maxInterval time.Duration, steps uint) CycleTicker {
	return newSeriesTicker(linearToIntervals(minInterval, maxInterval, steps))
}

// Creates ticker with intervals between minInterval and maxInterval values.
// Number of intervals in-between is determined by steps value.
// Ticker starts with minInterval value and with every report of executed "false"
// changes interval value to next one, up until maxInterval.
// Report of executed "true" resets interval to minInterval
// Example: for minInterval = 100ms, maxInterval = 5s, base = 2, steps = 4, intervals are
// 100ms . 427ms .. 1080ms .... 2387ms ........ 5000ms
//
// If min- or maxInterval is <= 0 or base = 0 or steps = 0 or min > maxInterval, ticker will not fire
func NewExpTicker(minInterval, maxInterval time.Duration, base, steps uint) CycleTicker {
	if minInterval <= 0 || maxInterval <= 0 || base == 0 || steps == 0 || minInterval > maxInterval {
		return NewNoopTicker()
	}
	if minInterval == maxInterval {
		return newFixedIntervalTicker(minInterval)
	}
	if base == 1 {
		return newLinearTicker(minInterval, maxInterval, steps)
	}

	return newExpTicker(minInterval, maxInterval, base, steps)
}

func newExpTicker(minInterval, maxInterval time.Duration, base, steps uint) CycleTicker {
	return newSeriesTicker(expToIntervals(minInterval, maxInterval, base, steps))
}

type NoopTicker struct {
	ch chan time.Time
}

func NewNoopTicker() CycleTicker {
	return &NoopTicker{
		ch: make(chan time.Time),
	}
}

func (t *NoopTicker) Start() {
}

func (t *NoopTicker) Stop() {
}

func (t *NoopTicker) C() <-chan time.Time {
	return t.ch
}

func (t *NoopTicker) CycleExecuted(executed bool) {
}

func linearToIntervals(minInterval, maxInterval time.Duration, steps uint) []time.Duration {
	delta := float64(maxInterval-minInterval) / float64(steps)
	floatInterval := float64(minInterval)

	intervals := make([]time.Duration, steps+1)
	intervals[0] = minInterval
	for i := uint(1); i <= steps; i++ {
		floatInterval += delta
		intervals[i] = time.Duration(floatInterval)
	}
	return intervals
}

func expToIntervals(minInterval, maxInterval time.Duration, base, steps uint) []time.Duration {
	sum := uint(1)
	power := uint(1)
	for i := uint(1); i < steps; i++ {
		power *= base
		sum += power
	}
	delta := float64(maxInterval-minInterval) / float64(sum)
	floatInterval := float64(minInterval)
	floatBase := float64(base)

	intervals := make([]time.Duration, steps+1)
	intervals[0] = minInterval
	for i := uint(1); i <= steps; i++ {
		floatInterval += delta
		intervals[i] = time.Duration(floatInterval)
		if i < steps {
			delta *= floatBase
		}
	}
	return intervals
}
