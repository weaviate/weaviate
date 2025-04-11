package distributedtask

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// 1. Let's say we have two active tasks (#1.1 and #2.1) in the raft snapshot.
// 2. In the raft log there is a task to cancel task #1.1 and then start #1.2 again.
//
// Before restart, we were doing #1.2 already and initialized some local state.

// # Even based task scheduler
// After restart, we handle these events:
// 1. We notice that snapshot had #1.1 and #2.1, so we start a task for them.
// 	* 1.1. notices that the version on disk is newer and aborts.
//  * 1.2. continues with the task.
// 2. We notice that the log has a task to cancel #1.1. There is no task to cancel so all good. (TODO: make sure that the channel has at least 1 size)
// 3. Another even comes in to start #1.2 again. We notice that there is some state on disk, pick it up and continue.

// # Goal state task scheduler
// After restart we pool the tasks and see that there is as started #1.2 and #2.1.
// Scheduler pools the tasks list and makes sure that:
// 1. There is a running task for each STARTED task.
// 2, Invokes clean ups for all tasks in a non

type TaskScheduler struct {
}

type testTask struct {
	id      string
	version uint64
	payload string

	finishCh chan struct{}
	cancelCh chan struct{}

	p *testTaskProvider
}

func newTestTask(id string, version uint64, payload string, p *testTaskProvider) *testTask {
	return &testTask{
		id:      id,
		version: version,
		payload: payload,
		p:       p,

		finishCh: make(chan struct{}),
		cancelCh: make(chan struct{}),
	}
}

func (t *testTask) Launch() {
	go func() {
		t.p.startedCh <- t

		select {
		case <-t.finishCh:
			return
		case <-t.cancelCh:
			return
		}
	}()
}

func (t *testTask) Cancel() {
	close(t.cancelCh)
}

type testTaskProvider struct {
	startedCh   chan *testTask
	finishedCh  chan *testTask
	cancelledCh chan *testTask
}

func (p *testTaskProvider) RegisterTaskNodeCompletionRecorder(recorder TaskNodeCompletionRecorder) {
	//TODO implement me
	panic("implement me")
}

func newTestTaskProvider() *testTaskProvider {
	return &testTaskProvider{
		startedCh:   make(chan *testTask),
		finishedCh:  make(chan *testTask),
		cancelledCh: make(chan *testTask),
	}
}

func (p *testTaskProvider) PrepareTask(taskID string, taskVersion uint64, payload []byte) (TaskHandle, error) {
	return newTestTask(taskID, taskVersion, string(payload), p), nil
}

func TestLaunchTaskAndFinish(t *testing.T) {
	var (
		manager  = NewManager(nil)
		provider = newTestTaskProvider()
	)

	err := manager.RegisterProvider("test", provider)
	require.NoError(t, err)

	err = manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  "test",
		Id:                    "1234",
		Payload:               []byte("payload"),
		SubmittedAtUnixMillis: time.Now().UnixMilli(),
	}), 10)
	require.NoError(t, err)

	startedTask := recvWithTimeout(t, provider.startedCh)
	require.Equal(t, "1234", startedTask.id)

	close(startedTask.finishCh)

	require.Equal(t, "1234", recvWithTimeout(t, provider.finishedCh))
}

func recvWithTimeout[T any](t *testing.T, ch <-chan T) T {
	select {
	case el := <-ch:
		return el
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
	panic("unreachable")
}

func toCmd[T any](t *testing.T, subCommand T) *cmd.ApplyRequest {
	bytes, err := json.Marshal(subCommand)
	require.NoError(t, err)

	return &cmd.ApplyRequest{
		SubCommand: bytes,
	}
}
