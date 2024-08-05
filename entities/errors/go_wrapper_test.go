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

package errors

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestGoWrapper(t *testing.T) {
	cases := []struct {
		env string
		set bool
	}{
		{env: "something", set: true},
		{env: "something", set: false},
		{env: "", set: true},
		{env: "false", set: true},
		// {env: "true", set: true}, // this will NOT recover the panic, but we cannot recover on a higher level and
		// there is no way to have the test succeed
	}
	for _, tt := range cases {
		t.Run(tt.env, func(t *testing.T) {
			var buf bytes.Buffer
			log := logrus.New()
			log.SetOutput(&buf)

			if tt.set {
				t.Setenv("DISABLE_RECOVERY_ON_PANIC", tt.env)
			}
			wg := sync.WaitGroup{}
			wg.Add(1)
			GoWrapper(func() {
				defer wg.Done()
				panic("test")
			}, log)
			wg.Wait()

			// wait for the recover function in the wrapper to write to the log. This is done after the defer function
			// in the function we pass to the wrapper has been called and we have no way to block until it is done.
			// Note that this does not matter in normal operation as we do not depend on the log being written to
			time.Sleep(100 * time.Millisecond)
			log.SetOutput(os.Stderr)
			assert.Contains(t, buf.String(), "Recovered from panic")
		})
	}
}

func TestGoWrapperWithBlock(t *testing.T) {
	cases := []struct {
		env string
		set bool
	}{
		{env: "something", set: true},
		{env: "something", set: false},
		{env: "", set: true},
		{env: "false", set: true},
	}
	for _, tt := range cases {
		t.Run(tt.env, func(t *testing.T) {
			var buf bytes.Buffer
			log := logrus.New()
			log.SetOutput(&buf)

			if tt.set {
				t.Setenv("DISABLE_RECOVERY_ON_PANIC", tt.env)
			}
			err := GoWrapperWithBlock(func() {
				panic("test panic")
			}, log)
			assert.NotNil(t, err)

			// wait for the recover function in the wrapper to write to the log. This is done after the defer function
			// in the function we pass to the wrapper has been called and we have no way to block until it is done.
			// Note that this does not matter in normal operation as we do not depend on the log being written to
			time.Sleep(100 * time.Millisecond)
			log.SetOutput(os.Stderr)
			assert.Contains(t, buf.String(), "Recovered from panic")
			assert.Contains(t, buf.String(), "test panic")
		})
	}
}

func TestGoWrapperWithErrorCh(t *testing.T) {
	cases := []struct {
		env string
		set bool
	}{
		{env: "something", set: true},
		{env: "something", set: false},
		{env: "", set: true},
		{env: "false", set: true},
	}
	for _, tt := range cases {
		t.Run(tt.env, func(t *testing.T) {
			var buf bytes.Buffer
			log := logrus.New()
			log.SetOutput(&buf)

			if tt.set {
				t.Setenv("DISABLE_RECOVERY_ON_PANIC", tt.env)
			}

			var a atomic.Bool
			a.Store(true)

			errCh := GoWrapperWithErrorCh(func() {
				time.Sleep(50 * time.Millisecond)
				a.Store(false)
				panic("test panic")
			}, log)

			assert.True(t, a.Load())
			assert.NotNil(t, errCh)
			assert.NotNil(t, <-errCh)
			assert.False(t, a.Load())

			log.SetOutput(os.Stderr)
			assert.Contains(t, buf.String(), "Recovered from panic")
			assert.Contains(t, buf.String(), "test panic")
		})
	}
}
