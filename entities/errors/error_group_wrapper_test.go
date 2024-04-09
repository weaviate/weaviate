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
	"context"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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

			eg := NewErrorGroupWrapper(log)
			if tt.set {
				t.Setenv("DISABLE_RECOVERY_ON_PANIC", tt.env)
			}
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

// The assumption is that the context returned by the group will be cancelled
// as soon as one goroutine panics
func TestErrorGroupWrapperWithContext_Panics(t *testing.T) {
	var buf bytes.Buffer
	log := logrus.New()
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctx := context.Background()
	eg, ctx := NewErrorGroupWithContextWrapper(log, ctx)

	eg.Go(func() error {
		slice := make([]string, 0)
		slice[0] = "test"
		return nil
	})

	// if the wrapper wouldn't cancel the context this line would block forever
	<-ctx.Done()
	assert.NotNil(t, ctx.Err())

	err := eg.Wait()
	assert.Contains(t, buf.String(), "Recovered from panic")
	assert.Contains(t, err.Error(), "index out of range")
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
