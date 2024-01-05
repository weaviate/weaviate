//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cyclemanager

import (
	"time"
)

// ===== Tickers =====

type CycleTicker interface {
	Start()
	Stop()
	C() <-chan time.Time
	// called with bool value whenever cycle function finished execution
	// true - indicates cycle function actually did some processing
	// false - cycle function returned without doing anything
	CycleExecuted(executed bool)
}

type cycleTicker struct {
	intervals CycleIntervals
	ticker    *time.Ticker
}

func newCycleTicker(intervals CycleIntervals) CycleTicker {
	if intervals == nil {
		return NewNoopTicker()
	}
	ticker := time.NewTicker(time.Second)
	ticker.Stop()
	return &cycleTicker{ticker: ticker, intervals: intervals}
}

func (t *cycleTicker) Start() {
	t.ticker.Reset(t.intervals.Get())
}

func (t *cycleTicker) Stop() {
	t.ticker.Stop()
}

func (t *cycleTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *cycleTicker) CycleExecuted(executed bool) {
	if executed {
		t.intervals.Reset()
	} else {
		t.intervals.Advance()
	}
	t.ticker.Reset(t.intervals.Get())
}

// Creates ticker with fixed interval. Interval is not changed regardless
// of execution results reported by cycle function
//
// If interval <= 0 given, ticker will not fire
func NewFixedTicker(interval time.Duration) CycleTicker {
	return newCycleTicker(NewFixedIntervals(interval))
}

// Creates ticker with set of interval values.
// Ticker starts with intervals[0] value and with every report of executed "false"
// changes interval value to next one in given array up until last one.
// Report of executed "true" resets interval to interval[0]
//
// If any of intervals given is <= 0 given, ticker will not fire
func NewSeriesTicker(intervals []time.Duration) CycleTicker {
	return newCycleTicker(NewSeriesIntervals(intervals))
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
	return newCycleTicker(NewLinearIntervals(minInterval, maxInterval, steps))
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
	return newCycleTicker(NewExpIntervals(minInterval, maxInterval, base, steps))
}

type noopTicker struct {
	ch chan time.Time
}

func NewNoopTicker() CycleTicker {
	return &noopTicker{
		ch: make(chan time.Time),
	}
}

func (t *noopTicker) Start() {
}

func (t *noopTicker) Stop() {
}

func (t *noopTicker) C() <-chan time.Time {
	return t.ch
}

func (t *noopTicker) CycleExecuted(executed bool) {
}

// ===== Intervals =====

type CycleIntervals interface {
	Get() time.Duration
	Reset()
	Advance()
}

type fixedIntervals struct {
	interval time.Duration
}

func (i *fixedIntervals) Get() time.Duration {
	return i.interval
}

func (i *fixedIntervals) Reset() {
}

func (i *fixedIntervals) Advance() {
}

type seriesIntervals struct {
	intervals []time.Duration
	pos       int
}

func (i *seriesIntervals) Get() time.Duration {
	return i.intervals[i.pos]
}

func (i *seriesIntervals) Reset() {
	i.pos = 0
}

func (i *seriesIntervals) Advance() {
	if i.pos < len(i.intervals)-1 {
		i.pos++
	}
}

func NewFixedIntervals(interval time.Duration) CycleIntervals {
	if interval <= 0 {
		return nil
	}
	return &fixedIntervals{interval: interval}
}

func NewSeriesIntervals(intervals []time.Duration) CycleIntervals {
	if len(intervals) == 0 {
		return nil
	}
	allSame := true
	for i := range intervals {
		if intervals[i] <= 0 {
			return nil
		}
		if intervals[i] != intervals[0] {
			allSame = false
		}
	}
	if allSame {
		return &fixedIntervals{interval: intervals[0]}
	}
	return &seriesIntervals{intervals: intervals, pos: 0}
}

func NewLinearIntervals(minInterval, maxInterval time.Duration, steps uint) CycleIntervals {
	if minInterval <= 0 || maxInterval <= 0 || steps == 0 || minInterval > maxInterval {
		return nil
	}
	if minInterval == maxInterval {
		return &fixedIntervals{interval: minInterval}
	}
	return &seriesIntervals{intervals: linearToIntervals(minInterval, maxInterval, steps), pos: 0}
}

func NewExpIntervals(minInterval, maxInterval time.Duration, base, steps uint) CycleIntervals {
	if minInterval <= 0 || maxInterval <= 0 || base == 0 || steps == 0 || minInterval > maxInterval {
		return nil
	}
	if minInterval == maxInterval {
		return &fixedIntervals{interval: minInterval}
	}
	if base == 1 {
		return &seriesIntervals{intervals: linearToIntervals(minInterval, maxInterval, steps), pos: 0}
	}
	return &seriesIntervals{intervals: expToIntervals(minInterval, maxInterval, base, steps), pos: 0}
}

// ===== Helper funcs =====

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
