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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/errorcompounder"
)

func TestErrorGroupWrapper(t *testing.T) {
	// start dispatches f, so one set of assertions covers TryGo and Go.
	dispatchers := []struct {
		name  string
		start func(egw *ErrorGroupWrapper, f func() error)
	}{
		{
			name:  "Go",
			start: func(egw *ErrorGroupWrapper, f func() error) { egw.Go(f) },
		},
		{
			name: "TryGo",
			start: func(egw *ErrorGroupWrapper, f func() error) {
				require.True(t, egw.TryGo(f), "the group has no limit, so it must start")
			},
		},
	}
	cases := []struct {
		name string
		env  string
	}{
		{name: "non-boolean", env: "something"},
		{name: "empty or unset", env: ""},
		{name: "false", env: "false"},
	}
	for _, d := range dispatchers {
		for _, tt := range cases {
			t.Run(d.name+"/"+tt.name, func(t *testing.T) {
				var buf bytes.Buffer
				log := logrus.New()
				log.SetOutput(&buf)
				defer func() {
					log.SetOutput(os.Stderr)
				}()

				// the constructor reads the environment, so set it first
				t.Setenv("DISABLE_RECOVERY_ON_PANIC", tt.env)
				eg := NewErrorGroupWrapper(log)
				d.start(eg, func() error {
					slice := make([]string, 0)
					slice[0] = "test"
					return nil
				})
				err := eg.Wait()
				assert.Contains(t, buf.String(), "Recovered from panic")
				require.ErrorContains(t, err, "index out of range")
			})
		}
	}
}

func TestErrorGroupWrapperTryGo_Limit(t *testing.T) {
	cases := []struct {
		name string
		// limit caps the group. One goroutine is blocked in it for the whole
		// test, so a limit of 1 leaves no slot for TryGo and 2 leaves one.
		limit     int
		wantStart bool
		wantCount int64
	}{
		{name: "slot free", limit: 2, wantStart: true, wantCount: 2},
		{name: "limit saturated", limit: 1, wantStart: false, wantCount: 1},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetOutput(io.Discard)

			eg := NewErrorGroupWrapper(log)
			eg.SetLimit(tt.limit)

			block := make(chan struct{})
			eg.Go(func() error {
				<-block
				return nil
			})
			// a cleanup release keeps a failed assertion from blocking the group
			t.Cleanup(func() {
				close(block)
				assert.NoError(t, eg.Wait())
			})

			var ran atomic.Int64
			started := eg.TryGo(func() error { ran.Add(1); return nil })
			require.Equal(t, tt.wantStart, started)
			require.Equal(t, tt.wantCount, eg.routineCounter.Load(),
				"only a TryGo that started is counted into Wait's jobs_count")
			if !started {
				require.Zero(t, ran.Load(),
					"a refused TryGo must not run f; index_objects_ttl.go runs it inline instead")
			}
		})
	}
}

// The assumption is that the context returned by the group will be cancelled as
// soon as one goroutine panics. Wait then reports the panic rather than the
// cancellation its siblings return because of it.
func TestErrorGroupWrapperWithContext_Panics(t *testing.T) {
	cases := []struct {
		name string
		// limit, when positive, caps the group so the sibling only starts once the
		// panicking goroutine has finished.
		limit           int
		sibling         func(ctx context.Context) error
		wantErrContains string
	}{
		{name: "no sibling"},
		{
			name: "sibling waiting on the group context",
			sibling: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		},
		{
			name:  "sibling starting after the panic",
			limit: 1,
			sibling: func(ctx context.Context) error {
				return ctx.Err()
			},
		},
		{
			name: "sibling finishing cleanly",
			sibling: func(context.Context) error {
				return nil
			},
		},
		{
			name: "sibling panicking too",
			sibling: func(context.Context) error {
				panic("sibling")
			},
			wantErrContains: "panic occurred",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// the panic this asserts on only becomes an error where it is recovered
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			var buf bytes.Buffer
			log := logrus.New()
			log.SetOutput(&buf)
			defer func() {
				log.SetOutput(os.Stderr)
			}()

			ctx := context.Background()
			eg, ctx := NewErrorGroupWithContextWrapper(log, ctx)
			if tt.limit > 0 {
				eg.SetLimit(tt.limit)
			}

			eg.Go(func() error {
				slice := make([]string, 0)
				slice[0] = "test"
				return nil
			})
			if tt.sibling != nil {
				eg.Go(func() error {
					return tt.sibling(ctx)
				})
			}

			// if the wrapper wouldn't cancel the context this line would block forever
			<-ctx.Done()
			assert.NotNil(t, ctx.Err())

			err := eg.Wait()
			assert.Contains(t, buf.String(), "Recovered from panic")
			wantErrContains := tt.wantErrContains
			if wantErrContains == "" {
				wantErrContains = "index out of range"
			}
			require.ErrorContains(t, err, wantErrContains)
			require.NotErrorIs(t, err, context.Canceled)
		})
	}
}

// TestErrorGroupWrapperReturnsGoroutineError pins that the deferred recovery
// leaves the error a goroutine returned by itself, with recovery enabled and
// with DISABLE_RECOVERY_ON_PANIC turning it into a no-op.
func TestErrorGroupWrapperReturnsGoroutineError(t *testing.T) {
	jobErr := errors.New("job failed")
	cases := []struct {
		name            string
		disableRecovery string
	}{
		{name: "recovery enabled", disableRecovery: "false"},
		{name: "recovery disabled", disableRecovery: "true"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := logrus.New()
			log.SetOutput(&buf)
			defer func() {
				log.SetOutput(os.Stderr)
			}()

			t.Setenv("DISABLE_RECOVERY_ON_PANIC", tt.disableRecovery)
			eg, _ := NewErrorGroupWithContextWrapper(log, context.Background())
			eg.Go(func() error {
				return jobErr
			})

			require.ErrorIs(t, eg.Wait(), jobErr)
			assert.NotContains(t, buf.String(), "Recovered from panic")
		})
	}
}

// The assumption is that when the goroutine doesn't panic, the context
// does not get canceled
func TestErrorGroupWrapperWithContext_DoesNotPanic(t *testing.T) {
	var buf bytes.Buffer
	log := logrus.New()
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctx := context.Background()
	eg, ctx := NewErrorGroupWithContextWrapper(log, ctx)

	eg.Go(func() error {
		slice := make([]string, 1)
		slice[0] = "test"
		return nil
	})

	assert.Nil(t, ctx.Err())
	err := eg.Wait()
	assert.Nil(t, err)
	assert.NotContains(t, buf.String(), "Recovered from panic")
}

// TestRunRecovered covers the inline runner's three outcomes, including the
// DISABLE_RECOVERY_ON_PANIC branch, which is the half of the parity claim that
// nothing else exercises.
func TestRunRecovered(t *testing.T) {
	boom := errors.New("boom")

	t.Run("f's error passes through", func(t *testing.T) {
		t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
		require.ErrorIs(t, RunRecovered(newTestLogger(t), func() error { return boom }), boom)
	})

	t.Run("no error and no panic", func(t *testing.T) {
		t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
		require.NoError(t, RunRecovered(newTestLogger(t), func() error { return nil }))
	})

	t.Run("a panic becomes the error", func(t *testing.T) {
		t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
		var buf bytes.Buffer
		log := logrus.New()
		log.SetOutput(&buf)

		err := RunRecovered(log, func() error { panic("blew up") })

		require.ErrorContains(t, err, "panic occurred: blew up")
		require.Contains(t, buf.String(), "Recovered from panic")
	})

	t.Run("DISABLE_RECOVERY_ON_PANIC lets the panic reach the runtime", func(t *testing.T) {
		// Read per call rather than cached, so this takes effect for every
		// RunRecovered in the process from here on.
		t.Setenv("DISABLE_RECOVERY_ON_PANIC", "true")
		require.Panics(t, func() {
			_ = RunRecovered(newTestLogger(t), func() error { panic("blew up") })
		})
	})
}

func newTestLogger(t *testing.T) logrus.FieldLogger {
	t.Helper()
	log := logrus.New()
	log.SetOutput(io.Discard)
	return log
}

// TestErrorGroupWrapperPanics pins that every recovered panic is collected with
// its own localVars, where Wait reports one of them.
func TestErrorGroupWrapperPanics(t *testing.T) {
	dispatchers := []struct {
		name string
		// feedsWait is false for the inline runner, which returns the panic to its caller.
		feedsWait bool
		start     func(egw *ErrorGroupWrapper, f func() error, localVars ...interface{})
	}{
		{
			name:      "Go",
			feedsWait: true,
			start: func(egw *ErrorGroupWrapper, f func() error, localVars ...interface{}) {
				egw.Go(f, localVars...)
			},
		},
		{
			name:      "TryGo",
			feedsWait: true,
			start: func(egw *ErrorGroupWrapper, f func() error, localVars ...interface{}) {
				require.True(t, egw.TryGo(f, localVars...), "the group has no limit, so it must start")
			},
		},
		{
			name: "RunRecovered",
			start: func(egw *ErrorGroupWrapper, f func() error, localVars ...interface{}) {
				_ = egw.RunRecovered(f, localVars...)
			},
		},
	}
	for _, d := range dispatchers {
		t.Run(d.name, func(t *testing.T) {
			// a panic is only collected where it is recovered
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			log := logrus.New()
			log.SetOutput(io.Discard)

			eg := NewErrorGroupWrapper(log)

			const panicking = 3
			for i := 0; i < panicking; i++ {
				shard := fmt.Sprintf("shard-%d", i)
				d.start(eg, func() error { panic("boom on " + shard) }, shard)
			}
			d.start(eg, func() error { return nil }, "shard-clean")

			waitErr := eg.Wait()
			if d.feedsWait {
				require.ErrorContains(t, waitErr, "panic occurred")
			} else {
				require.NoError(t, waitErr)
			}

			collected := eg.recoveredPanics()
			require.Len(t, collected, panicking, "every panic is collected, not just the one Wait returns")
			if d.feedsWait {
				reported := 0
				for i := 0; i < panicking; i++ {
					if strings.Contains(waitErr.Error(), fmt.Sprintf("boom on shard-%d", i)) {
						reported++
					}
				}
				require.Equal(t, 1, reported,
					"Wait reports one panic; collecting is what makes the other two reachable")
			}
			filed := map[string]error{}
			for _, p := range collected {
				require.Len(t, p.localVars, 1, "a panic carries the localVars of the call that raised it")
				shard, ok := p.localVars[0].(string)
				require.True(t, ok, "localVars are collected unchanged, got %T", p.localVars[0])
				filed[shard] = p.err
			}
			for i := 0; i < panicking; i++ {
				shard := fmt.Sprintf("shard-%d", i)
				require.ErrorContains(t, filed[shard], "boom on "+shard)
			}
			require.NotContains(t, filed, "shard-clean", "a call that returned cleanly collects nothing")
		})
	}
}

// TestRecoveredPanicKeepsAnErrorValue guards that a panic carrying an error
// stays matchable by errors.Is. A nil deref panics with a runtime.Error.
func TestRecoveredPanicKeepsAnErrorValue(t *testing.T) {
	sentinel := errors.New("the panic value")

	cases := []struct {
		name string
		run  func(egw *ErrorGroupWrapper) error
	}{
		{
			name: "Go",
			run: func(egw *ErrorGroupWrapper) error {
				egw.Go(func() error { panic(sentinel) }, "Books")
				return egw.Wait()
			},
		},
		{
			name: "TryGo",
			run: func(egw *ErrorGroupWrapper) error {
				require.True(t, egw.TryGo(func() error { panic(sentinel) }, "Books"))
				return egw.Wait()
			},
		},
		{
			name: "RunRecovered",
			run: func(egw *ErrorGroupWrapper) error {
				return egw.RunRecovered(func() error { panic(sentinel) }, "Books")
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// these assertions read a recovered panic, so the recovery must run
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			logger := logrus.New()
			logger.SetOutput(io.Discard)
			egw := NewErrorGroupWrapper(logger)

			err := tt.run(egw)
			require.ErrorIs(t, err, sentinel, "the panic value stays matchable")
			require.ErrorContains(t, err, "panic occurred: the panic value",
				"and the message reads as it did before the value was wrapped")

			collected := egw.recoveredPanics()
			require.Len(t, collected, 1)
			require.ErrorIs(t, collected[0].err, sentinel,
				"the ledger keeps the value matchable too, for whoever collects it")
		})
	}

	t.Run("a runtime error", func(t *testing.T) {
		t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

		logger := logrus.New()
		logger.SetOutput(io.Discard)

		var counts map[string]int
		err := NewErrorGroupWrapper(logger).RunRecovered(func() error {
			counts["boom"] = 1
			return nil
		})

		var runtimeErr runtime.Error
		require.ErrorAs(t, err, &runtimeErr,
			"writing to a nil map panics with a runtime.Error, so callers can match on it")
	})
}

// TestErrorGroupWrapperRunRecovered pins that the method runs f on the calling
// goroutine and leaves Wait's jobs_count alone.
func TestErrorGroupWrapperRunRecovered(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	log := logrus.New()
	log.SetOutput(io.Discard)
	eg := NewErrorGroupWrapper(log)

	caller := currentGoroutineID()
	var ran uint64
	require.NoError(t, eg.RunRecovered(func() error {
		ran = currentGoroutineID()
		return nil
	}))
	require.Equal(t, caller, ran, "RunRecovered must not start a goroutine")
	require.Zero(t, eg.routineCounter.Load(), "a call that starts nothing is not counted")

	jobErr := errors.New("job failed")
	require.ErrorIs(t, eg.RunRecovered(func() error { return jobErr }), jobErr,
		"Wait never sees an inline error, so the return is its only route out")

	err := eg.RunRecovered(func() error { panic("boom") }, "Books", "shard-1")
	require.ErrorContains(t, err, "panic occurred: boom", "the panic is returned, as it is from Go")
	require.Len(t, eg.recoveredPanics(), 1)
	require.Equal(t, []interface{}{"Books", "shard-1"}, eg.recoveredPanics()[0].localVars)
	require.NoError(t, eg.Wait())
}

// currentGoroutineID reads the id off the runtime's own stack header.
func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	var id uint64
	fmt.Sscanf(string(buf[:n]), "goroutine %d ", &id)
	return id
}

func TestWaitAndCollectFilesEveryPanic(t *testing.T) {
	cases := []struct {
		name  string
		run   func(egw *ErrorGroupWrapper)
		want  []string
		count int
	}{
		{
			name:  "a goroutine panic",
			run:   func(egw *ErrorGroupWrapper) { egw.Go(func() error { panic("in goroutine") }, "Books") },
			want:  []string{"in goroutine", "Books"},
			count: 1,
		},
		{
			name:  "an inline panic, which Wait never reports",
			run:   func(egw *ErrorGroupWrapper) { _ = egw.RunRecovered(func() error { panic("inline") }, "Books", 7) },
			want:  []string{"inline", "Books", "7"},
			count: 1,
		},
		{
			name: "both, so the second is not shadowed by the first",
			run: func(egw *ErrorGroupWrapper) {
				egw.Go(func() error { panic("in goroutine") }, "Books")
				egw.Wait()
				_ = egw.RunRecovered(func() error { panic("inline") }, "Movies")
			},
			want:  []string{"in goroutine", "inline", "Books", "Movies"},
			count: 2,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// these assertions read a recovered panic, so the recovery must run
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			logger := logrus.New()
			logger.SetOutput(io.Discard)
			egw := NewErrorGroupWrapper(logger)
			ec := errorcompounder.New()

			tt.run(egw)
			_ = egw.WaitAndCollect(ec.AddGroups)

			require.Equal(t, tt.count, ec.Len(), "every recovered panic is filed once")
			err := ec.ToError()
			require.Error(t, err)
			for _, want := range tt.want {
				require.ErrorContains(t, err, want)
			}
		})
	}
}

// TestWaitAndCollectKeepsPanicsAcrossRounds pins what a reused group does:
// hands back a copy, reports round one's error again, re-collects it.
func TestWaitAndCollectKeepsPanicsAcrossRounds(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	egw := NewErrorGroupWrapper(logger)

	egw.Go(func() error { panic("round one") }, "Books")
	require.ErrorContains(t, egw.Wait(), "round one")

	snapshot := egw.recoveredPanics()
	require.Len(t, snapshot, 1)
	snapshot[0] = recoveredPanic{err: errors.New("overwritten")}

	egw.Go(func() error { panic("round two") }, "Movies")
	require.ErrorContains(t, egw.Wait(), "round one",
		"errgroup keeps the first error, so a reused group reports round one again")

	ec := errorcompounder.New()
	_ = egw.WaitAndCollect(ec.AddGroups)
	require.Equal(t, 2, ec.Len(),
		"panics are kept, not drained, so a reused group re-collects round one")
	filed := ec.ToError()
	require.ErrorContains(t, filed, "round one",
		"replacing an element of the returned slice cannot reach egw.panics")
	require.ErrorContains(t, filed, "round two")
	require.NotContains(t, filed.Error(), "overwritten")
}

// TestVarStrings guards that every var reaches the collector.
func TestVarStrings(t *testing.T) {
	cases := []struct {
		name string
		vars []interface{}
		want []string
	}{
		{name: "no vars", want: []string{}},
		{name: "collection and shard", vars: []interface{}{"Books", "shard-1"}, want: []string{"Books", "shard-1"}},
		{name: "a non-string is rendered", vars: []interface{}{"Books", 7}, want: []string{"Books", "7"}},
		{name: "nil is rendered", vars: []interface{}{"Books", nil}, want: []string{"Books", "<nil>"}},
		{name: "no strings at all", vars: []interface{}{7, nil}, want: []string{"7", "<nil>"}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, varStrings(tt.vars...))
		})
	}
}

// TestWaitAndCollectReturnsWaitsError pins the half the collected panics do not
// carry: an error a callback returned reaches the caller only through the return.
func TestWaitAndCollectReturnsWaitsError(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	failed := errors.New("callback failed")

	t.Run("an error a callback returned", func(t *testing.T) {
		eg := NewErrorGroupWrapper(logger)
		eg.Go(func() error { return failed })

		var collected []error
		err := eg.WaitAndCollect(func(err error, groups ...string) {
			collected = append(collected, err)
		})

		require.ErrorIs(t, err, failed, "the caller decides what to do with it")
		require.Empty(t, collected, "a returned error is not a recovered panic")
	})

	t.Run("no error and no panic returns nil", func(t *testing.T) {
		eg := NewErrorGroupWrapper(logger)
		eg.Go(func() error { return nil })

		require.NoError(t, eg.WaitAndCollect(func(error, ...string) {}))
	})

	t.Run("a nil collector makes it Wait", func(t *testing.T) {
		t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
		eg := NewErrorGroupWrapper(logger)
		eg.Go(func() error { panic("boom") }, "Books")

		require.ErrorContains(t, eg.WaitAndCollect(nil), "panic occurred: boom",
			"a nil collect still waits and still reports, it just files nothing")
	})

	t.Run("a recovered panic reaches both", func(t *testing.T) {
		t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
		eg := NewErrorGroupWrapper(logger)
		eg.Go(func() error { panic("boom") }, "Books", "shard-1")

		var groups [][]string
		err := eg.WaitAndCollect(func(_ error, g ...string) { groups = append(groups, g) })

		require.ErrorContains(t, err, "panic occurred",
			"the group reports the panic as the goroutine's error")
		require.Equal(t, [][]string{{"Books", "shard-1"}}, groups,
			"and the callback is handed the names of the call that raised it")
	})
}
