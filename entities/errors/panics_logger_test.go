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
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrintStack(t *testing.T) {
	t.Run("logs stack trace with correct level and action field", func(t *testing.T) {
		logger, hook := test.NewNullLogger()

		PrintStack(logger)

		require.Len(t, hook.Entries, 1)

		entry := hook.Entries[0]

		assert.Equal(t, logrus.ErrorLevel, entry.Level)
		assert.Equal(t, "print_stack", entry.Data["action"])

		message := entry.Message
		assert.NotEmpty(t, message, "expected non-empty log message")

		// Stack trace should contain typical stack trace elements
		assert.Contains(t, message, "goroutine", "expected stack trace to contain 'goroutine'")
		assert.Contains(t, message, "panics_logger_test.go", "expected stack trace to contain the test file name")
		assert.Contains(t, message, "TestPrintStack", "expected stack trace to contain the test function name")
	})

	t.Run("stack trace contains file paths and line numbers", func(t *testing.T) {
		logger, hook := test.NewNullLogger()

		PrintStack(logger)

		require.Len(t, hook.Entries, 1)
		message := hook.Entries[0].Message

		assert.True(t, strings.Contains(message, ".go:"), "expected stack trace to contain file:line patterns")
	})

	t.Run("captures stack from calling function", func(t *testing.T) {
		logger, hook := test.NewNullLogger()

		// Call from a nested function to ensure stack capture works
		nestedFunction := func() {
			PrintStack(logger)
		}
		nestedFunction()

		require.Len(t, hook.Entries, 1)
		message := hook.Entries[0].Message

		// Should contain the nested function in the stack
		assert.Contains(t, message, "TestPrintStack", "expected stack trace to contain calling test function")
	})

	t.Run("logs stack trace when recovering from actual panic", func(t *testing.T) {
		logger, hook := test.NewNullLogger()

		// Function that will panic
		panickyFunction := func() {
			defer func() {
				if r := recover(); r != nil {
					// This is where we'd use PrintStack in real code
					PrintStack(logger)
				}
			}()

			// Trigger an actual panic
			panic("something went wrong!")
		}

		// Execute the panicky function (should not crash the test due to recover)
		panickyFunction()

		require.Len(t, hook.Entries, 1, "expected exactly one log entry from panic recovery")

		entry := hook.Entries[0]

		assert.Equal(t, logrus.ErrorLevel, entry.Level)
		assert.Equal(t, "print_stack", entry.Data["action"])

		message := entry.Message

		// Verify stack trace contains panic-related information
		assert.Contains(t, message, "goroutine")
		assert.Contains(t, message, "panics_logger_test.go")
		assert.Contains(t, message, "panic", "expected stack trace to show panic call")

		assert.True(t, strings.Contains(message, ".go:"), "expected file:line patterns in stack trace")
	})
}
