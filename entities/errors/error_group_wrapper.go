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
	"github.com/weaviate/weaviate/entities/errorcompounder"
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
	panics         []RecoveredPanic
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
		// the no-op never calls recover, so the panic reaches the runtime
		egw.recoverPanic = func(*error, ...interface{}) {}
		return
	}
	egw.recoverPanic = func(err *error, localVars ...interface{}) {
		r := recover()
		if r == nil {
			return
		}
		entsentry.Recover(r)
		egw.logger.WithField("panic", r).Errorf("Recovered from panic: %v, local variables %v, additional localVars %v", r, localVars, egw.variables)
		PrintStack(egw.logger)

		// *err surfaces however the caller ran f (Go/TryGo via Wait, RunInline via
		// return); Panics keeps it either way.
		*err = fmt.Errorf("panic occurred: %v", r)

		egw.panicsLock.Lock()
		egw.panics = append(egw.panics, RecoveredPanic{Err: *err, LocalVars: localVars})
		egw.panicsLock.Unlock()
	}
}

// RecoveredPanic is a panic the group recovered, with the localVars of the call
// that raised it.
type RecoveredPanic struct {
	Err       error
	LocalVars []interface{}
}

// Panics returns every panic the group recovered, including the ones Wait does
// not report: Wait returns one goroutine's error, and never a RunInline call's.
// Call it after Wait, once every goroutine has finished appending.
func (egw *ErrorGroupWrapper) Panics() []RecoveredPanic {
	egw.panicsLock.Lock()
	defer egw.panicsLock.Unlock()
	return append([]RecoveredPanic(nil), egw.panics...)
}

// WaitAndCollect waits for the group, then files every recovered panic under
// its localVars into ec. It discards what Wait returned, so use it only where
// callbacks file their own errors and return nil. Call it once: panics are
// kept, not drained, so a repeat call re-files them.
func (egw *ErrorGroupWrapper) WaitAndCollect(ec errorcompounder.ErrorCompounder) {
	_ = egw.Wait()
	for _, p := range egw.Panics() {
		ec.AddGroups(p.Err, errorcompounder.GroupNames(p.LocalVars...)...)
	}
}

// recovered wraps f so every method that runs a callback shares one recovery.
func (egw *ErrorGroupWrapper) recovered(f func() error, localVars ...interface{}) func() error {
	return func() (err error) {
		defer egw.recoverPanic(&err, localVars...)
		return f()
	}
}

// Go runs f in a new goroutine. A panic in f becomes f's error: Wait reports
// it, and a context-bound group cancels with it. DISABLE_RECOVERY_ON_PANIC lets
// the panic reach the runtime instead.
func (egw *ErrorGroupWrapper) Go(f func() error, localVars ...interface{}) {
	egw.Group.Go(egw.recovered(f, localVars...))
	egw.routineCounter.Add(1)
}

// RunInline calls f on the calling goroutine and returns what f returned, or the
// panic it recovered. Wait reports neither, so an error f returned is the caller's
// to handle; a recovered panic also reaches Panics and WaitAndCollect.
func (egw *ErrorGroupWrapper) RunInline(f func() error, localVars ...interface{}) error {
	return egw.recovered(f, localVars...)()
}

// TryGo runs f in a new goroutine when the group's limit allows it, reporting
// whether it started. A panic in f is recovered and returned as f's error, just
// as Go does.
func (egw *ErrorGroupWrapper) TryGo(f func() error, localVars ...interface{}) bool {
	started := egw.Group.TryGo(egw.recovered(f, localVars...))
	if started {
		egw.routineCounter.Add(1)
	}
	return started
}

// SetLimit overrides the SetLimit method to set a limit on the number of
// goroutines and track what's set.
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
