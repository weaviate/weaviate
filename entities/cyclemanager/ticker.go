package cyclemanager

import "time"

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

func NewFixedIntervalTicker(interval time.Duration) CycleTicker {
	// for testing purposes allow interval of 0,
	// meaning cycle manager not working
	if interval <= 0 {
		return NewNoopTicker()
	}

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
	// result of execution does not change ticker's interval
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
