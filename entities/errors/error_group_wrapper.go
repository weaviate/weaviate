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

// setRecoverPanic builds the recovery that Go defers in each goroutine.
// DISABLE_RECOVERY_ON_PANIC=true makes it a no-op instead, so a panic reaches
// the runtime.
func (egw *ErrorGroupWrapper) setRecoverPanic() {
	if entcfg.Enabled(os.Getenv("DISABLE_RECOVERY_ON_PANIC")) {
		// the no-op never calls recover, so the panic reaches the runtime
		egw.recoverPanic = func(*error, ...interface{}) {}
		return
	}
	egw.recoverPanic = func(err *error, localVars ...interface{}) {
		// recover only works when the deferred function calls it itself, so it
		// stays here rather than moving into reportPanic.
		if r := recover(); r != nil {
			reportPanic(err, r, egw.logger, localVars, egw.variables)
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

// RunRecovered runs f on the calling goroutine and turns a panic in f into the
// same error ErrorGroupWrapper.Go turns it into, so a caller that runs its work
// inline in some cases and on the group in others reports a panic the same way
// either way. DISABLE_RECOVERY_ON_PANIC lets the panic reach the runtime.
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

// Go runs f in a new goroutine. A panic in f is recovered and returned as f's
// error, so Wait reports it as "panic occurred: <value>" and the group's
// context is cancelled with it. DISABLE_RECOVERY_ON_PANIC lets the panic reach
// the runtime instead.
func (egw *ErrorGroupWrapper) Go(f func() error, localVars ...interface{}) {
	egw.Group.Go(func() (err error) {
		defer egw.recoverPanic(&err, localVars...)
		return f()
	})
	egw.routineCounter.Add(1)
}

// SetLimit overrides the SetLimit method to set a limit on the number of
// goroutines and track what's set.
func (egw *ErrorGroupWrapper) SetLimit(limit int) {
	egw.Group.SetLimit(limit)
	egw.limitSet = limit
}

// Wait waits for all goroutines to finish and returns the first non-nil error,
// which includes a panic Go recovered.
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
