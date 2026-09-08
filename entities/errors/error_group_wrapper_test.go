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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorGroupWrapper(t *testing.T) {
	cases := []struct {
		env string
		set bool
	}{
		{env: "something", set: true},
		{env: "something", set: false},
		{env: "", set: true},
		{env: "false", set: true},
		// {env: "true", set: true}, this will NOT recover the panic, but we cannot recover on a higher level and there
		// is no way to have the test succeed
	}
	for _, tt := range cases {
		t.Run(tt.env, func(t *testing.T) {
			var buf bytes.Buffer
			log := logrus.New()
			log.SetOutput(&buf)
			defer func() {
				log.SetOutput(os.Stderr)
			}()

			// the constructor reads the environment, so the value has to be in
			// place before it runs
			if tt.set {
				t.Setenv("DISABLE_RECOVERY_ON_PANIC", tt.env)
			}
			eg := NewErrorGroupWrapper(log)
			eg.Go(func() error {
				slice := make([]string, 0)
				slice[0] = "test"
				return nil
			})
			err := eg.Wait()
			assert.Contains(t, buf.String(), "Recovered from panic")
			assert.Contains(t, err.Error(), "index out of range")
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

// TestErrorGroupWrapperPanics pins that every recovered panic is drained with
// its own localVars, where Wait reports one of them.
func TestErrorGroupWrapperPanics(t *testing.T) {
	dispatchers := []struct {
		name string
		// feedsWait is false for RunInline, which returns the panic to its
		// caller instead of routing it through the group.
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
			name: "RunInline",
			start: func(egw *ErrorGroupWrapper, f func() error, localVars ...interface{}) {
				_ = egw.RunInline(f, localVars...)
			},
		},
	}
	for _, d := range dispatchers {
		t.Run(d.name, func(t *testing.T) {
			// a panic is only drained where it is recovered
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

			drained := eg.Panics()
			require.Len(t, drained, panicking, "every panic is drained, not just the one Wait returns")
			if d.feedsWait {
				reported := 0
				for i := 0; i < panicking; i++ {
					if strings.Contains(waitErr.Error(), fmt.Sprintf("boom on shard-%d", i)) {
						reported++
					}
				}
				require.Equal(t, 1, reported,
					"Wait reports one panic; the drain is what makes the other two reachable")
			}
			filed := map[string]error{}
			for _, p := range drained {
				require.Len(t, p.LocalVars, 1, "a panic carries the localVars of the call that raised it")
				shard, ok := p.LocalVars[0].(string)
				require.True(t, ok, "localVars are drained unchanged, got %T", p.LocalVars[0])
				filed[shard] = p.Err
			}
			for i := 0; i < panicking; i++ {
				shard := fmt.Sprintf("shard-%d", i)
				require.ErrorContains(t, filed[shard], "boom on "+shard)
			}
			require.NotContains(t, filed, "shard-clean", "a call that returned cleanly drains nothing")
		})
	}
}

// TestErrorGroupWrapperRunInline pins that RunInline runs f on the calling
// goroutine and leaves Wait's jobs_count alone.
func TestErrorGroupWrapperRunInline(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	log := logrus.New()
	log.SetOutput(io.Discard)
	eg := NewErrorGroupWrapper(log)

	caller := currentGoroutineID()
	var ran uint64
	require.NoError(t, eg.RunInline(func() error {
		ran = currentGoroutineID()
		return nil
	}))
	require.Equal(t, caller, ran, "RunInline must not start a goroutine")
	require.Zero(t, eg.routineCounter.Load(), "a call that starts nothing is not counted")

	err := eg.RunInline(func() error { panic("boom") }, "Books", "shard-1")
	require.ErrorContains(t, err, "panic occurred: boom", "the panic is returned, as it is from Go")
	require.Len(t, eg.Panics(), 1)
	require.Equal(t, []interface{}{"Books", "shard-1"}, eg.Panics()[0].LocalVars)
	require.NoError(t, eg.Wait())
}

// currentGoroutineID reads the id off the runtime's own stack header, so a test
// can tell whether a call stayed on the goroutine that made it.
func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	var id uint64
	fmt.Sscanf(string(buf[:n]), "goroutine %d ", &id)
	return id
}
