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

package distributedtask

import (
	"fmt"
	"sync"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

// scriptedCompletionProvider returns swappingErr(n) from the n-th
// SWAPPING-status OnTaskCompleted call (1-based) and nil on every other status.
type scriptedCompletionProvider struct {
	*testTaskProvider

	swappingErr func(n int) error

	mu            sync.Mutex
	swappingCalls int
}

func (p *scriptedCompletionProvider) OnGroupCompleted(_ *Task, _ string, _ []string) error {
	return nil
}

func (p *scriptedCompletionProvider) OnSwapRequested(_ *Task, _ string, _ []string) error {
	return nil
}

func (p *scriptedCompletionProvider) OnTaskCompleted(task *Task) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if task.Status != TaskStatusSwapping {
		return nil
	}
	p.swappingCalls++
	return p.swappingErr(p.swappingCalls)
}

func (p *scriptedCompletionProvider) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.swappingCalls
}

func TestOnTaskCompletedAwaitingIsBudgetExempt(t *testing.T) {
	t.Run("awaiting errors never fail the task", func(t *testing.T) {
		defer leaktest.Check(t)()

		const awaitingTicks = 2 * maxCompletedCallbackAttempts
		prov := &scriptedCompletionProvider{
			testTaskProvider: newTestTaskProvider(t, nil),
			swappingErr: func(n int) error {
				if n <= awaitingTicks {
					return fmt.Errorf("leader writes the descriptor: %w", ErrTaskCompletionAwaiting)
				}
				return nil
			},
		}
		h := submitTaskToSwapping(t, prov, prov.testTaskProvider, "awaiting-task")
		defer h.Close()

		// The harness finalizer applies MarkDistributedTaskFailed directly, so
		// a proposal while waiting would show as FAILED here.
		for i := 1; i <= awaitingTicks; i++ {
			h.advanceClock(h.schedulerTickInterval)
			require.Equal(t, TaskStatusSwapping, statusOf(t, h),
				"task must stay SWAPPING while awaiting (tick %d)", i)
		}
		require.Greater(t, prov.calls(), maxCompletedCallbackAttempts,
			"awaiting errors must not consume the callback budget")

		h.advanceClock(h.schedulerTickInterval)
		require.Equal(t, TaskStatusFinished, statusOf(t, h))
		require.Equal(t, awaitingTicks+1, prov.calls())
	})

	t.Run("plain errors still exhaust the budget", func(t *testing.T) {
		defer leaktest.Check(t)()

		prov := &scriptedCompletionProvider{
			testTaskProvider: newTestTaskProvider(t, nil),
			swappingErr: func(int) error {
				return fmt.Errorf("simulated transient completion failure")
			},
		}
		h := submitTaskToSwapping(t, prov, prov.testTaskProvider, "plain-error-task")
		defer h.Close()

		for i := 0; i < maxCompletedCallbackAttempts; i++ {
			h.advanceClock(h.schedulerTickInterval)
		}
		require.Equal(t, TaskStatusFailed, statusOf(t, h))
		require.Equal(t, maxCompletedCallbackAttempts, prov.calls())
	})

	t.Run("awaiting errors re-fire on a terminal status", func(t *testing.T) {
		defer leaktest.Check(t)()

		prov := &terminalAwaitingProvider{testTaskProvider: newTestTaskProvider(t, nil)}
		h := submitTaskToSwapping(t, prov, prov.testTaskProvider, "terminal-awaiting-task")
		defer h.Close()

		h.advanceClock(h.schedulerTickInterval)
		require.Equal(t, TaskStatusFailed, statusOf(t, h))

		const awaitingTicks = 3
		for i := 1; i <= awaitingTicks; i++ {
			h.advanceClock(h.schedulerTickInterval)
			require.Equal(t, TaskStatusFailed, statusOf(t, h),
				"task must stay FAILED while awaiting (tick %d)", i)
		}
		require.Equal(t, awaitingTicks, prov.calls(),
			"the callback must re-fire every tick although the provider is no TerminalCleanupProvider")
	})
}

// terminalAwaitingProvider fails the task permanently on SWAPPING and returns
// ErrTaskCompletionAwaiting on every later call. It is not a
// TerminalCleanupProvider, so only the awaiting branch re-fires its callback
// on a terminal status.
type terminalAwaitingProvider struct {
	*testTaskProvider

	mu            sync.Mutex
	terminalCalls int
}

func (p *terminalAwaitingProvider) OnGroupCompleted(_ *Task, _ string, _ []string) error {
	return nil
}

func (p *terminalAwaitingProvider) OnSwapRequested(_ *Task, _ string, _ []string) error {
	return nil
}

func (p *terminalAwaitingProvider) OnTaskCompleted(task *Task) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if task.Status == TaskStatusSwapping {
		return fmt.Errorf("fail the task: %w", ErrTaskCompletionPermanent)
	}
	p.terminalCalls++
	return fmt.Errorf("leader writes the descriptor: %w", ErrTaskCompletionAwaiting)
}

func (p *terminalAwaitingProvider) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.terminalCalls
}
