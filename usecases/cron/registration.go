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

package cron

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/errors"
)

// cronsRegistrationConfig is everything a cron job supplies.
// newCronsRegistration checks it and builds the registration around it, which
// owns the channel, the locks and the value the loop last registered.
type cronsRegistrationConfig[T comparable] struct {
	name           string
	runtimeHookKey string

	// shouldRegister is read once, when start runs. nil means always.
	shouldRegister  func() bool
	configuredValue func() T
	// resolve turns a configured value into a cron spec, and reports false
	// for a value that means "do not run".
	resolve func(T) (spec string, register bool)
	// cancelOnChange cancels the tick in flight before the replacement, so
	// the drain does not wait out a full run. false lets it finish.
	cancelOnChange bool

	logger            logrus.FieldLogger
	gocronLogger      gocron.Logger
	serverShutdownCtx context.Context
}

// cronsRegistration holds a cron job's enable gate, the buffered channel its
// runtime-config hook pushes to, and the loop that registers the job and
// re-registers it as values arrive.
type cronsRegistration[T comparable] struct {
	// mu guards currentValue and the valueCh drain+push.
	mu           sync.Mutex
	currentValue T // guarded by mu
	valueCh      chan T

	// jobLogger is logger with the job field set to name, so every line the
	// loop and the tick write names the job the same way.
	jobLogger logrus.FieldLogger

	// registerWG lets shutdown await start's goroutine instead of returning
	// while it still runs.
	registerWG sync.WaitGroup

	// stopped reports that the loop goroutine has exited. GoWrapper recovers a
	// panic in it and returns normally, so without this RuntimeConfigHook would
	// push a value no loop ever reads. A registrant whose enable gate declined
	// never had a loop, and still reads as running.
	stopped atomic.Bool

	// runMu keeps the tick off the job that replaces it: the tick holds it
	// for its whole body, and the loop Lock/Unlocks it as a barrier before
	// swapping. DrainAndUpsertJob supplies that exclusion only while a named
	// entry survives, and the disable path removes the entry.
	runMu sync.Mutex

	cronsRegistrationConfig[T]
}

func (c *cronsRegistration[T]) jobName() string { return c.name }

func (c *cronsRegistration[T]) hookKey() string { return c.runtimeHookKey }

// newCronsRegistration checks the fields a registration cannot work without
// and opens the value channel, seeded with the current configured value. start
// refuses a registration built any other way, because a nil channel parks its
// loop silently.
func newCronsRegistration[T comparable](cfg cronsRegistrationConfig[T]) (*cronsRegistration[T], error) {
	if cfg.name == "" {
		return nil, fmt.Errorf("cron job has no name")
	}
	// Only usecases/config interprets this key, where an empty one
	// prefix-matches every runtime config field name.
	if cfg.runtimeHookKey == "" {
		return nil, fmt.Errorf("cron job %q has no runtime config hook key", cfg.name)
	}
	if cfg.configuredValue == nil {
		return nil, fmt.Errorf("cron job %q reads no configured value", cfg.name)
	}
	if cfg.resolve == nil {
		return nil, fmt.Errorf("cron job %q resolves no schedule", cfg.name)
	}
	if cfg.logger == nil {
		return nil, fmt.Errorf("cron job %q has no logger", cfg.name)
	}
	if cfg.gocronLogger == nil {
		return nil, fmt.Errorf("cron job %q has no cron logger", cfg.name)
	}
	if cfg.serverShutdownCtx == nil {
		return nil, fmt.Errorf("cron job %q has no shutdown context", cfg.name)
	}

	c := &cronsRegistration[T]{
		cronsRegistrationConfig: cfg,
		valueCh:                 make(chan T, 1),
	}
	c.jobLogger = cfg.logger.WithField("job", cfg.name)
	c.currentValue = c.configuredValue()
	c.valueCh <- c.currentValue
	return c, nil
}

// enabled reports whether this job registers. A nil shouldRegister means it
// does.
func (c *cronsRegistration[T]) enabled() bool {
	return c.shouldRegister == nil || c.shouldRegister()
}

// start launches the registration loop and reports whether it did. A job
// shouldRegister turns off gets no goroutine. tickGate is consulted once per
// tick, before tick runs.
func (c *cronsRegistration[T]) start(cr *gocron.Cron, tickGate func() bool,
	tick func(context.Context),
) (started bool, err error) {
	// Caught at startup rather than at the first fire: a nil gate or tick
	// panics into initGoCron's Recover chain on every tick, which logs a
	// stack and keeps firing, and a nil channel parks the loop with no output.
	if tickGate == nil {
		return false, fmt.Errorf("cron job %q has no tick gate", c.name)
	}
	if tick == nil {
		return false, fmt.Errorf("cron job %q has no tick", c.name)
	}
	if c.valueCh == nil {
		return false, fmt.Errorf("cron job %q was not built by newCronsRegistration", c.name)
	}
	if !c.enabled() {
		return false, nil
	}

	c.registerWG.Add(1)
	errors.GoWrapper(func() {
		defer c.registerWG.Done()
		defer c.stopped.Store(true)
		c.loop(cr, tickGate, tick)
	}, c.jobLogger)

	return true, nil
}

// loop registers the job for every value the channel carries, and returns once
// the server shuts down.
func (c *cronsRegistration[T]) loop(cr *gocron.Cron, tickGate func() bool,
	tick func(context.Context),
) {
	cancel := func() {} // noop until a cancelOnChange job replaces it

	for {
		select {
		case value := <-c.valueCh:
			spec, register := c.resolve(value)
			if !register {
				cancel()
				if cr.RemoveByName(c.name) {
					c.jobLogger.Info("cron job removed")
				}
				c.jobLogger.WithField("value", value).
					Info("cron job skipped, its configured value disables it")
				continue
			}

			// Parse before touching the entry, so a spec the library
			// refuses leaves the running job as it was, with its tick
			// context uncancelled.
			if err := cr.ValidateSpec(spec); err != nil {
				outcome := "no job is registered"
				if cr.EntryByName(c.name).Valid() {
					outcome = "keeping the previous registration"
				}
				c.jobLogger.WithField("schedule", spec).
					Errorf("cron job schedule refused, %s: %v", outcome, err)
				continue
			}
			cancel()

			// Wait out the tick in flight before swapping the job, and say so:
			// the wait can last a whole tick body. gocron drains for us only
			// while the named entry survives, and a disable removed it, so on
			// the re-enable DrainAndUpsertJob finds nothing to wait for.
			// Released before that upsert, which waits for a tick that takes
			// runMu first: holding runMu across the call deadlocks the two.
			if !c.runMu.TryLock() {
				c.jobLogger.Debug("cron job waiting for the tick in flight")
				c.runMu.Lock()
			}
			c.runMu.Unlock()
			if c.serverShutdownCtx.Err() != nil {
				c.jobLogger.Debug("server shutdown context cancelled")
				return
			}

			tickCtx := c.serverShutdownCtx
			if c.cancelOnChange {
				tickCtx, cancel = context.WithCancel(c.serverShutdownCtx)
			}
			entryId, err := cr.DrainAndUpsertJob(spec, c.tickJob(tickCtx, tickGate, tick),
				gocron.WithName(c.name))
			if err != nil {
				// The entry the upsert did not replace still holds the tick
				// cancel() ended above, so leaving it registered keeps a job
				// firing that can only skip itself.
				outcome := "no job is registered"
				if cr.RemoveByName(c.name) {
					outcome = "the previous registration was removed"
				}
				c.jobLogger.WithField("schedule", spec).
					Errorf("cron job not added, %s: %v", outcome, err)
				continue
			}
			c.jobLogger.WithFields(logrus.Fields{
				"entry_id": entryId,
				"schedule": spec,
			}).Info("cron job added")

		case <-c.serverShutdownCtx.Done():
			cancel()
			c.jobLogger.Debug("server shutdown context cancelled")
			return
		}
	}
}

// tickJob returns the per-tick callback. SkipIfStillRunning keeps a tick off
// itself. runMu keeps it off the job that replaces it.
func (c *cronsRegistration[T]) tickJob(ctx context.Context, tickGate func() bool,
	tick func(context.Context),
) gocron.Job {
	return gocron.NewChain(
		gocron.SkipIfStillRunning(c.gocronLogger),
	).Then(gocron.FuncJob(func() {
		c.runMu.Lock()
		defer c.runMu.Unlock()

		// A cancelOnChange job cancels its tick context before the barrier, so
		// a fire from the replaced generation must not run. The read sits under
		// runMu so a fire that waited for the lock still sees that cancel.
		if ctx.Err() != nil {
			c.jobLogger.Debug("cron tick skipped, its context has ended")
			return
		}

		if !tickGate() {
			c.jobLogger.Debug("cron tick skipped by its gate")
			return
		}
		tick(ctx)
	}))
}

// wait blocks until start's goroutine has exited. Returns at once when start
// never launched one.
func (c *cronsRegistration[T]) wait() {
	c.registerWG.Wait()
}

// RuntimeConfigHook re-reads the configured value and, on change, pushes it
// to the registration loop. The whole read-compare-store-and-push runs under
// mu so concurrent callers can't interleave and leave the channel holding a
// different value than currentValue.
func (c *cronsRegistration[T]) RuntimeConfigHook() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	newValue := c.configuredValue()
	if newValue == c.currentValue {
		return nil
	}
	// Below the no-change return, so a sibling field's change does not report
	// a failure this hook had nothing to apply. currentValue stays behind, so
	// the next push of the same value still reports.
	if c.stopped.Load() {
		return fmt.Errorf("cron job %q has no registration loop running", c.name)
	}
	c.currentValue = newValue

	// Drain the stale value (if any) before pushing. The buffer is size 1,
	// so the send stays non-blocking while we hold the lock.
	select {
	case <-c.valueCh:
	default:
	}
	c.valueCh <- newValue
	return nil
}
