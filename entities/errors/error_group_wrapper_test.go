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
	"os"
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
