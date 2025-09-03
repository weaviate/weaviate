package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestWorkerDo_RetryOnTransient_PausesThenResumes_PreservesOrder(t *testing.T) {
	logger, hook := test.NewNullLogger()
	w := &Worker{
		logger:        logger,
		retryInterval: 5 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Global step counter to verify strict in-order execution across tasks.
	var globalSeq int32

	// Task1: fail transiently 10x, succeed on 11th execution.
	t1 := newFakeTask("t1", 10, &globalSeq)

	// Task2: no failures, should run only after t1 has fully succeeded.
	t2 := newFakeTask("t2", 0, &globalSeq)

	batch := &Batch{
		Ctx:   ctx,
		Tasks: []Task{t1, t2},
	}

	err := w.do(batch)
	require.NoError(t, err, "batch should complete successfully")

	// t1 should have 11 executes: 10 transient OOM + 1 success.
	require.Equal(t, int32(11), atomic.LoadInt32(&t1.execCnt), "t1 execution count mismatch")
	// Logger printed 10 warnings (1 per transient OOM)
	assert.Len(t, hook.Entries, 10, "expected 10 log entries for t1 transient failures")
	// t2 should run exactly once, and only after t1 finishes.
	require.Equal(t, int32(1), atomic.LoadInt32(&t2.execCnt), "t2 should execute once")

	// Order guarantee: all t1 steps must come BEFORE any t2 step.
	require.NotEmpty(t, t1.steps)
	require.NotEmpty(t, t2.steps)

	lastT1 := t1.steps[len(t1.steps)-1]
	firstT2 := t2.steps[0]
	require.Greater(t, firstT2, lastT1, "t2 must not start until t1 has succeeded (queue paused then resumed)")

	// Extra sanity: t1 had exactly 4 steps (seq numbers) and they’re strictly increasing.
	require.Len(t, t1.steps, 11)
	for i := 1; i < len(t1.steps); i++ {
		require.Greater(t, t1.steps[i], t1.steps[i-1], "t1 steps must be strictly increasing")
	}
}

// fakeTask fails with a transient error `failures` times, then succeeds.
// It also records the global "step" at each Execute call, so we can verify ordering.
type fakeTask struct {
	name      string
	failures  int32 // remaining failures before success
	execCnt   int32
	steps     []int32
	stepLogCh chan int32 // append-only via channel to avoid races if needed
	globalSeq *int32
}

func (f *fakeTask) Key() uint64 {
	return 0
}

func (f *fakeTask) Op() uint8 {
	return 0
}

func newFakeTask(name string, transientFailures int, globalSeq *int32) *fakeTask {
	return &fakeTask{
		name:      name,
		failures:  int32(transientFailures),
		globalSeq: globalSeq,
		steps:     make([]int32, 0, transientFailures+1),
	}
}

func (t *fakeTask) Execute(ctx context.Context) error {
	// record a global, monotonically increasing step for strict ordering checks
	step := atomic.AddInt32(t.globalSeq, 1)
	t.steps = append(t.steps, step)
	atomic.AddInt32(&t.execCnt, 1)

	// Simulate transient errors first, then success
	if atomic.LoadInt32(&t.failures) > 0 {
		atomic.AddInt32(&t.failures, -1)
		return enterrors.NewOutOfMemory("simulated transient OOM")
	}
	return nil
}
