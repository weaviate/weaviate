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

package errors

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	entcfg "github.com/weaviate/weaviate/entities/config"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"golang.org/x/sync/errgroup"
)

// ErrorGroupWrapper is a custom type that embeds errgroup.Group.
type ErrorGroupWrapper struct {
	*errgroup.Group
	variables      []interface{}
	logger         logrus.FieldLogger
	recoverPanic   func(err *error, localVars ...interface{})
	panicsLock     sync.Mutex
	panics         []recoveredPanic
	routineCounter atomic.Int64
	includeStack   bool
	limitSet       int
}

// NewErrorGroupWrapper creates a new ErrorGroupWrapper.
func NewErrorGroupWrapper(logger logrus.FieldLogger, vars ...interface{}) *ErrorGroupWrapper {
	egw := &ErrorGroupWrapper{
		Group:     new(errgroup.Group),
		variables: vars,
		logger:    logger,
	}
	egw.setRecoverPanic()

	if entcfg.Enabled(os.Getenv("LOG_STACK_TRACE_ON_ERROR_GROUP")) {
		egw.includeStack = true
	}
	return egw
}

// NewErrorGroupWithContextWrapper creates a new ErrorGroupWrapper
func NewErrorGroupWithContextWrapper(logger logrus.FieldLogger, ctx context.Context, vars ...interface{}) (*ErrorGroupWrapper, context.Context) {
	eg, ctx := errgroup.WithContext(ctx)
	egw := &ErrorGroupWrapper{
		Group:     eg,
		variables: vars,
		logger:    logger,
	}
	egw.setRecoverPanic()

	if entcfg.Enabled(os.Getenv("LOG_STACK_TRACE_ON_ERROR_GROUP")) {
		egw.includeStack = true
	}

	return egw, ctx
}

// setRecoverPanic builds the recovery every method defers around its callback.
// DISABLE_RECOVERY_ON_PANIC=true makes it a no-op, so a panic reaches the runtime.
func (egw *ErrorGroupWrapper) setRecoverPanic() {
	if entcfg.Enabled(os.Getenv("DISABLE_RECOVERY_ON_PANIC")) {
		egw.recoverPanic = func(*error, ...interface{}) {}
		return
	}
	egw.recoverPanic = func(err *error, localVars ...interface{}) {
		// recover() only works when called directly by the deferred func, so it
		// can't move into reportPanic.
		if r := recover(); r != nil {
			reportPanic(err, r, egw.logger, localVars, egw.variables)

			// Recorded as well as returned: Wait carries one goroutine's error, so
			// WaitAndCollect is the only route out for every other panic.
			egw.panicsLock.Lock()
			egw.panics = append(egw.panics, recoveredPanic{err: *err, localVars: localVars})
			egw.panicsLock.Unlock()
		}
	}
}

// reportPanic logs a recovered panic and makes it the caller's error.
func reportPanic(err *error, r any, logger logrus.FieldLogger, localVars, groupVars []interface{}) {
	entsentry.Recover(r)
	logger.WithField("panic", r).Errorf("Recovered from panic: %v, local variables %v, additional localVars %v", r, localVars, groupVars)
	PrintStack(logger)

	// The panic becomes the goroutine's error, so errgroup's errOnce records it
	// and cancels with it. It therefore outranks every error a sibling returns
	// afterwards, including the context.Canceled that cancellation produces.
	*err = fmt.Errorf("panic occurred: %v", r)
}

// RunRecovered runs f on the calling goroutine, converting a panic into the
// same error ErrorGroupWrapper.Go produces, so inline and grouped work report
// panics identically. DISABLE_RECOVERY_ON_PANIC lets the panic reach the runtime.
//
// The environment is read per call, not cached: a test that sets the variable
// for itself needs the next call to see it, and caching the first read would
// silently ignore the setting depending on what ran before.
func RunRecovered(logger logrus.FieldLogger, f func() error) (err error) {
	if entcfg.Enabled(os.Getenv("DISABLE_RECOVERY_ON_PANIC")) {
		return f()
	}
	defer func() {
		if r := recover(); r != nil {
			reportPanic(&err, r, logger, nil, nil)
		}
	}()
	return f()
}

// recoveredPanic is a panic the group recovered, with the localVars of the call
// that raised it.
type recoveredPanic struct {
	err       error
	localVars []interface{}
}

// recoveredPanics returns every panic the group recovered, including ones Wait
// doesn't report: Wait returns one goroutine's error, never an inline call's.
// Call it after Wait, once every goroutine has recorded its panic.
func (egw *ErrorGroupWrapper) recoveredPanics() []recoveredPanic {
	egw.panicsLock.Lock()
	defer egw.panicsLock.Unlock()
	return append([]recoveredPanic(nil), egw.panics...)
}

// varStrings renders each localVar as a string for the collector. A collector
// that files by group turns each into a path segment, so pass identifying
// values, not log labels.
func varStrings(vars ...interface{}) []string {
	rendered := make([]string, 0, len(vars))
	for _, v := range vars {
		rendered = append(rendered, fmt.Sprint(v))
	}
	return rendered
}

// WaitAndCollect waits for the group, hands each recovered panic to collect
// with the raising call's local variables, and returns what Wait reported. A
// recovered panic reaches both, so recording both double-counts it. A nil
// collect makes this Wait. Call it once: panics are kept, not drained.
func (egw *ErrorGroupWrapper) WaitAndCollect(collect func(err error, localVars ...string)) error {
	err := egw.Wait()
	if collect == nil {
		return err
	}
	for _, p := range egw.recoveredPanics() {
		collect(p.err, varStrings(p.localVars...)...)
	}
	return err
}

// withRecovery wraps f so every method that runs a callback shares one recovery.
func (egw *ErrorGroupWrapper) withRecovery(f func() error, localVars ...interface{}) func() error {
	return func() (err error) {
		defer egw.recoverPanic(&err, localVars...)
		return f()
	}
}

// Go runs f in a new goroutine. A panic in f becomes f's error, so Wait
// reports it unless another goroutine errored first, and WaitAndCollect
// hands it to collect either way.
func (egw *ErrorGroupWrapper) Go(f func() error, localVars ...interface{}) {
	egw.Group.Go(egw.withRecovery(f, localVars...))
	egw.routineCounter.Add(1)
}

// RunRecovered calls f on the calling goroutine, not a new one, and returns what
// f returned or the panic it recovered. Unlike the package-level RunRecovered, a
// panic is also recorded for WaitAndCollect. The return is the only route out for
// an error f returned: neither Wait nor WaitAndCollect carries it.
func (egw *ErrorGroupWrapper) RunRecovered(f func() error, localVars ...interface{}) error {
	return egw.withRecovery(f, localVars...)()
}

// TryGo runs f in a new goroutine when the group's limit allows it, reporting
// whether it started. A panic in f is recovered and returned as f's error, just
// as Go does.
func (egw *ErrorGroupWrapper) TryGo(f func() error, localVars ...interface{}) bool {
	started := egw.Group.TryGo(egw.withRecovery(f, localVars...))
	if started {
		egw.routineCounter.Add(1)
	}
	return started
}

// SetLimit sets the group's goroutine limit and records it in limitSet.
func (egw *ErrorGroupWrapper) SetLimit(limit int) {
	egw.Group.SetLimit(limit)
	egw.limitSet = limit
}

// Wait waits for all goroutines to finish and returns the first non-nil error,
// which includes a recovered panic.
func (egw *ErrorGroupWrapper) Wait() error {
	count := egw.routineCounter.Load()
	logBase := egw.logger.WithFields(logrus.Fields{
		"action":     "error_group_wait_initiated",
		"jobs_count": count,
		"limit":      egw.limitSet,
	})

	if egw.includeStack {
		stackBuf := make([]byte, 4096)
		n := runtime.Stack(stackBuf, false)
		stackBuf = stackBuf[:n]

		logBase = logBase.WithField("stack", string(stackBuf))
	}

	logBase.Debugf("Waiting for %d jobs to finish with limit %d", count, egw.limitSet)

	return egw.Group.Wait()
}
