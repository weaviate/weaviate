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

package objectttl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/namespaces"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// fixedNodeResolver resolves every node name to the same host.
type fixedNodeResolver string

func (r fixedNodeResolver) NodeHostname(string) (string, bool) { return string(r), true }

// The sweep skips a collection whose namespace is not active. Which states are
// not active is namespaces.RequireActive's own table, so suspended stands for
// all of them here and the rows vary what the sweep does with the verdict.
func TestCoordinatorStartSkipsClassesWithoutActiveNamespace(t *testing.T) {
	suspended := []cmd.NamespaceState{cmd.NamespaceStateSuspended}

	type namespaceSetup struct {
		name string
		// steps are the state changes applied after creation, in order.
		steps []cmd.NamespaceState
	}

	tests := []struct {
		name       string
		namespaces []namespaceSetup
		classes    []string
		wantSwept  []string
	}{
		{
			name:       "active namespace is swept",
			namespaces: []namespaceSetup{{name: "customer1"}},
			classes:    []string{"customer1:Foo"},
			wantSwept:  []string{"customer1:Foo"},
		},
		{
			name:      "class outside any namespace is swept",
			classes:   []string{"Foo"},
			wantSwept: []string{"Foo"},
		},
		{
			name:       "suspended namespace is skipped",
			namespaces: []namespaceSetup{{name: "customer1", steps: suspended}},
			classes:    []string{"customer1:Foo"},
		},
		{
			name:    "namespace the node does not know is skipped",
			classes: []string{"customer1:Foo"},
		},
		{
			name: "a skipped namespace does not stop an active one",
			namespaces: []namespaceSetup{
				{name: "customer1", steps: suspended},
				{name: "customer2"},
			},
			classes:   []string{"customer1:Foo", "customer2:Bar"},
			wantSwept: []string{"customer2:Bar"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger := logrus.New()

			controller := namespaces.NewController(logger)
			raftIndex := uint64(1)
			for _, ns := range test.namespaces {
				require.NoError(t, controller.Create(cmd.Namespace{Name: ns.name, HomeNodes: []string{"node1"}}, raftIndex))
				raftIndex++
				for _, state := range ns.steps {
					require.NoError(t, controller.ChangeState(ns.name, state, namespaces.StateChange{AppliedIndex: raftIndex}))
					raftIndex++
				}
			}

			var sweptLock sync.Mutex
			var swept []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload []ObjectsExpiredPayload
				require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))

				sweptLock.Lock()
				defer sweptLock.Unlock()
				for _, collection := range payload {
					swept = append(swept, collection.Class)
				}
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()

			reader := schemaUC.NewMockSchemaReader(t)
			reader.EXPECT().ReadSchema(mock.Anything).RunAndReturn(func(read func(models.Class, uint64)) error {
				for _, class := range test.classes {
					read(models.Class{
						Class:           class,
						ObjectTTLConfig: &models.ObjectTTLConfig{Enabled: true, DeleteOn: "_creationTimeUnix"},
					}, 1)
				}
				return nil
			})

			// Not reached when every class is skipped, since there is nothing to dispatch.
			getter := schemaUC.NewMockSchemaGetter(t)
			getter.EXPECT().NodeName().Return("node1").Maybe()
			getter.EXPECT().Nodes().Return([]string{"node1", "node2"}).Maybe()

			// A nil db is safe because two nodes always route the sweep to the remote
			// node; the local branch would use it.
			c := NewCoordinator(reader, getter, controller, nil,
				configRuntime.NewDynamicValue[float64](1), logger, server.Client(),
				fixedNodeResolver(strings.TrimPrefix(server.URL, "http://")), NewLocalStatus())

			now := time.Now()
			require.NoError(t, c.Start(context.Background(), false, now, now))

			sweptLock.Lock()
			defer sweptLock.Unlock()
			assert.ElementsMatch(t, test.wantSwept, swept)
		})
	}
}

func TestLocalState(t *testing.T) {
	t.Run("initial state is not running", func(t *testing.T) {
		s := NewLocalStatus()
		assert.False(t, s.IsRunning())
	})

	t.Run("SetRunning succeeds when not running", func(t *testing.T) {
		s := NewLocalStatus()

		ok, ctx := s.SetRunning()

		require.True(t, ok)
		require.NotNil(t, ctx)
		assert.True(t, s.IsRunning())
		assert.NoError(t, ctx.Err(), "context should not be cancelled yet")
	})

	t.Run("SetRunning returns valid non-cancelled context", func(t *testing.T) {
		s := NewLocalStatus()

		ok, ctx := s.SetRunning()

		require.True(t, ok)
		require.NotNil(t, ctx)

		select {
		case <-ctx.Done():
			t.Fatal("context should not be done yet")
		default:
			// expected: context is still active
		}
	})

	t.Run("SetRunning fails when already running", func(t *testing.T) {
		s := NewLocalStatus()
		ok, _ := s.SetRunning()
		require.True(t, ok, "first SetRunning should succeed")

		ok2, ctx2 := s.SetRunning()

		assert.False(t, ok2)
		assert.Nil(t, ctx2)
		assert.True(t, s.IsRunning(), "should still be running after failed SetRunning")
	})

	t.Run("Abort cancels the context and keeps the slot", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx := s.SetRunning()
		require.True(t, ok)
		require.NotNil(t, ctx)

		aborted := s.Abort()

		assert.True(t, aborted)
		assert.True(t, s.IsRunning(),
			"the deletion observes the cancellation only between batches, so it is still draining")

		select {
		case <-ctx.Done():
			// expected
		default:
			t.Fatal("context should be done after Abort")
		}
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrAborted)
	})

	t.Run("Finish cancels the context and releases the slot", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx := s.SetRunning()
		require.True(t, ok)

		s.Finish()

		assert.False(t, s.IsRunning())
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.ErrorIs(t, context.Cause(ctx), ErrFinished)
	})

	t.Run("an aborted deletion keeps the cause it was aborted with", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx := s.SetRunning()
		require.True(t, ok)

		require.True(t, s.Abort())
		s.Finish()

		assert.ErrorIs(t, context.Cause(ctx), ErrAborted,
			"a deletion that returned because it was aborted must not report itself as finished")
		assert.NotErrorIs(t, context.Cause(ctx), ErrFinished)
	})

	t.Run("an aborted deletion's cleanup cannot cancel its successor", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx1 := s.SetRunning()
		require.True(t, ok)

		require.True(t, s.Abort())

		// while the aborted deletion drains, the slot is not up for grabs
		taken, ctx2 := s.SetRunning()
		require.False(t, taken, "a successor must not start on top of a draining deletion")
		require.Nil(t, ctx2)

		s.Finish()

		taken, ctx2 = s.SetRunning()
		require.True(t, taken)
		require.NotNil(t, ctx2)
		assert.NoError(t, ctx2.Err(),
			"the successor's context is its own, not one its predecessor's cleanup reaches")
		assert.ErrorIs(t, ctx1.Err(), context.Canceled)
	})

	t.Run("Abort reports false when nothing is running", func(t *testing.T) {
		s := NewLocalStatus()

		aborted := s.Abort()

		assert.False(t, aborted)
		assert.False(t, s.IsRunning())
	})

	t.Run("a repeated Abort still reports the draining deletion", func(t *testing.T) {
		s := NewLocalStatus()
		ok, _ := s.SetRunning()
		require.True(t, ok)

		assert.True(t, s.Abort())
		assert.True(t, s.Abort(), "the deletion has not returned yet, so there is still one to report")

		s.Finish()

		assert.False(t, s.Abort())
	})

	t.Run("Finish on a released slot is a no-op", func(t *testing.T) {
		s := NewLocalStatus()
		ok, _ := s.SetRunning()
		require.True(t, ok)

		s.Finish()
		s.Finish()

		assert.False(t, s.IsRunning())
	})

	t.Run("SetRunning can be called again after Finish", func(t *testing.T) {
		s := NewLocalStatus()

		ok1, ctx1 := s.SetRunning()
		require.True(t, ok1)
		s.Finish()

		ok2, ctx2 := s.SetRunning()

		assert.True(t, ok2)
		require.NotNil(t, ctx2)
		assert.True(t, s.IsRunning())
		assert.NoError(t, ctx2.Err(), "new context should not be cancelled")

		// old context should still be cancelled
		assert.ErrorIs(t, ctx1.Err(), context.Canceled)
	})

	t.Run("each SetRunning produces an independent context", func(t *testing.T) {
		s := NewLocalStatus()

		ok1, ctx1 := s.SetRunning()
		require.True(t, ok1)
		s.Finish()

		ok2, ctx2 := s.SetRunning()
		require.True(t, ok2)

		// ctx1 is cancelled, ctx2 is not
		assert.ErrorIs(t, ctx1.Err(), context.Canceled)
		assert.NoError(t, ctx2.Err())

		s.Finish()

		assert.ErrorIs(t, ctx2.Err(), context.Canceled)
	})

	t.Run("concurrent SetRunning calls: only one succeeds", func(t *testing.T) {
		s := NewLocalStatus()

		const goroutines = 50
		var wg sync.WaitGroup
		var successCount atomic.Int32

		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				ok, _ := s.SetRunning()
				if ok {
					successCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(1), successCount.Load(), "exactly one goroutine should win SetRunning")
		assert.True(t, s.IsRunning())
	})

	t.Run("concurrent Abort calls all report the one draining deletion", func(t *testing.T) {
		s := NewLocalStatus()
		ok, _ := s.SetRunning()
		require.True(t, ok)

		const goroutines = 50
		var wg sync.WaitGroup
		var abortedCount atomic.Int32

		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				if s.Abort() {
					abortedCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(goroutines), abortedCount.Load(),
			"every abort reaches the same running deletion")
		assert.True(t, s.IsRunning(), "and none of them releases the slot")
	})

	t.Run("concurrent SetRunning and Abort: consistent state", func(t *testing.T) {
		s := NewLocalStatus()
		// prime with a running state
		ok, _ := s.SetRunning()
		require.True(t, ok)

		var wg sync.WaitGroup
		const goroutines = 20

		// half try to abort, half try to set running again
		wg.Add(goroutines * 2)
		for range goroutines {
			go func() {
				defer wg.Done()
				s.Abort()
			}()
			go func() {
				defer wg.Done()
				s.SetRunning()
			}()
		}
		wg.Wait()

		// an abort never releases the slot, so the deletion primed above still holds it
		assert.True(t, s.IsRunning())
		taken, _ := s.SetRunning()
		assert.False(t, taken, "IsRunning and the slot must agree")
	})

	t.Run("context cancelled by Abort is propagated to child contexts", func(t *testing.T) {
		s := NewLocalStatus()
		ok, parentCtx := s.SetRunning()
		require.True(t, ok)

		childCtx, cancel := context.WithCancel(parentCtx)
		defer cancel()

		s.Abort()

		select {
		case <-childCtx.Done():
			assert.ErrorIs(t, context.Cause(childCtx), context.Canceled)
			assert.ErrorIs(t, context.Cause(childCtx), ErrAborted)
		default:
			t.Fatal("child context should be done after parent is cancelled")
		}
	})

	t.Run("IsRunning reflects state changes correctly across lifecycle", func(t *testing.T) {
		s := NewLocalStatus()

		assert.False(t, s.IsRunning(), "initially not running")

		ok, _ := s.SetRunning()
		require.True(t, ok)
		assert.True(t, s.IsRunning(), "running after SetRunning")

		s.Finish()
		assert.False(t, s.IsRunning(), "not running after Finish")

		ok2, _ := s.SetRunning()
		require.True(t, ok2)
		assert.True(t, s.IsRunning(), "running again after second SetRunning")
	})

	t.Run("multiple full cycles work correctly", func(t *testing.T) {
		s := NewLocalStatus()

		for i := range 5 {
			ok, ctx := s.SetRunning()
			require.True(t, ok, "cycle %d: SetRunning should succeed", i)
			require.NotNil(t, ctx)
			assert.NoError(t, ctx.Err())

			s.Finish()
			assert.ErrorIs(t, ctx.Err(), context.Canceled, "cycle %d", i)
			assert.False(t, s.IsRunning())
		}
	})
}

// ttlProbeTimeout bounds every wait in the probe, so a window that never opens
// fails the test instead of hanging the package.
const ttlProbeTimeout = 10 * time.Second

// ttlCounterProbe keeps the first collection's delete goroutine calling
// countDeleted across the loop's next write to the counter map, unordered.
type ttlCounterProbe struct {
	dispatched int
	firstClass string
	reading    chan struct{}
	stop       chan struct{}
	overlapped bool
	counted    int32
	timedOut   bool
}

func newTTLCounterProbe() *ttlCounterProbe {
	return &ttlCounterProbe{reading: make(chan struct{}), stop: make(chan struct{})}
}

func (p *ttlCounterProbe) DeleteExpiredObjects(_ context.Context, eg *enterrors.ErrorGroupWrapper,
	_ errorcompounder.ErrorCompounder, className, _ string, _, _ time.Time,
	countDeleted func(int32), _ uint64,
) {
	p.dispatched++
	switch p.dispatched {
	case 1:
		p.firstClass = className
		eg.Go(func() error {
			deadline := time.After(ttlProbeTimeout)
			close(p.reading)
			var count int32
			for {
				countDeleted(1)
				count++
				select {
				case <-p.stop:
					p.counted = count
					return nil
				case <-deadline:
					p.counted = count
					p.timedOut = true
					return errors.New("the dispatch loop never reached the next collection")
				default:
				}
			}
		})
		// hold the loop here until the reader is running, so the write that
		// follows lands inside the reader's window
		select {
		case <-p.reading:
			p.overlapped = true
		case <-time.After(ttlProbeTimeout):
		}
	case 2:
		// the second collection's entry is written, so the overlap under test has happened
		close(p.stop)
	}
}

// A delete goroutine must not read the counter map the dispatch loop writes to.
// The access is fatal rather than recoverable, and -race is what reports it.
func TestLocalSweepKeepsTheCounterMapOffTheDeleteGoroutines(t *testing.T) {
	// two collections are enough. The loop writes the second entry while the
	// first collection's deletes are still running
	classes := []string{"Collection0", "Collection1"}

	probe := newTTLCounterProbe()
	hook, err := runLocalSweep(t, classes, probe)
	require.NoError(t, err)

	require.True(t, probe.overlapped,
		"the reader must be running before the loop writes the next entry")
	require.False(t, probe.timedOut,
		"the reader hit its deadline, so the loop never reached the next collection")

	report := logEntry(hook, "ttl deletion on local node finished")
	require.NotNil(t, report, "the sweep must report what it deleted")
	assert.Equal(t, probe.counted, report.Data["c_"+probe.firstClass],
		"the collection is credited with what its own closure counted")
}

// logEntry returns the first entry logged with msg, which is how a test reads a
// value the code under test reports to an operator and nowhere else.
func logEntry(hook *logrustest.Hook, msg string) *logrus.Entry {
	for _, entry := range hook.AllEntries() {
		if entry.Message == msg {
			return entry
		}
	}
	return nil
}

// runLocalSweep sweeps classes through deleter on a single-node cluster, so the
// sweep takes the local path rather than handing the round to a remote node.
func runLocalSweep(t *testing.T, classes []string, deleter expiredObjectsDeleter) (*logrustest.Hook, error) {
	t.Helper()

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	reader := schemaUC.NewMockSchemaReader(t)
	reader.EXPECT().ReadSchema(mock.Anything).RunAndReturn(func(read func(models.Class, uint64)) error {
		for _, class := range classes {
			read(models.Class{
				Class:           class,
				ObjectTTLConfig: &models.ObjectTTLConfig{Enabled: true, DeleteOn: "_creationTimeUnix"},
			}, 1)
		}
		return nil
	})

	getter := schemaUC.NewMockSchemaGetter(t)
	getter.EXPECT().NodeName().Return("node1")
	getter.EXPECT().Nodes().Return([]string{"node1"})

	c := NewCoordinator(reader, getter, namespaces.NewController(logger), deleter,
		configRuntime.NewDynamicValue[float64](1), logger, nil, nil, NewLocalStatus())

	now := time.Now()
	return hook, c.Start(context.Background(), false, now, now)
}

// deleterFunc dispatches a collection's deletes however the caller wants them
// to run, in place of the store the sweep hands each collection to.
type deleterFunc func(eg *enterrors.ErrorGroupWrapper, ec errorcompounder.ErrorCompounder, className string)

func (f deleterFunc) DeleteExpiredObjects(_ context.Context, eg *enterrors.ErrorGroupWrapper,
	ec errorcompounder.ErrorCompounder, className, _ string, _, _ time.Time, _ func(int32), _ uint64,
) {
	f(eg, ec, className)
}

// deletePanic is what a delete goroutine panics with. It names no collection, so
// a row cannot pass on the panic's own text where the report owes it a group.
const deletePanic = "delete goroutine panicked"

var errDeleteFailed = errors.New("delete failed")

// A recovered panic files no error in the compounder and deletes no objects, so
// a sweep that only reads it reports a round that never ran as a clean one.
func TestLocalSweepReportsRecoveredPanics(t *testing.T) {
	tests := []struct {
		name       string
		classes    []string
		dispatch   deleterFunc
		wantErr    []string
		wantPanics int
	}{
		{
			name:    "every panicking collection is reported, not only the one Wait returns",
			classes: []string{"Collection0", "Collection1", "Collection2"},
			dispatch: func(eg *enterrors.ErrorGroupWrapper, _ errorcompounder.ErrorCompounder, _ string) {
				eg.Go(func() error { panic(deletePanic) })
			},
			wantErr:    []string{deletePanic},
			wantPanics: 3,
		},
		{
			name:    "a panic does not displace an error a sibling filed itself",
			classes: []string{"Collection0", "Collection1"},
			dispatch: func(eg *enterrors.ErrorGroupWrapper, ec errorcompounder.ErrorCompounder, className string) {
				if className == "Collection0" {
					eg.Go(func() error { panic(deletePanic) })
					return
				}
				eg.Go(func() error {
					ec.AddGroups(errDeleteFailed, className)
					return nil
				})
			},
			wantErr:    []string{deletePanic, "\"Collection1\": {" + errDeleteFailed.Error()},
			wantPanics: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// the integration job disables recovery, under which a panic takes the
			// test binary down instead of reaching the group
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			_, err := runLocalSweep(t, test.classes, test.dispatch)

			require.Error(t, err, "a sweep that lost a collection's deletes must not report success")
			for _, want := range test.wantErr {
				assert.ErrorContains(t, err, want)
			}
			assert.Equal(t, test.wantPanics, strings.Count(err.Error(), "panic occurred"),
				"one entry per panicking collection, since Wait reports only the first")
		})
	}
}

// scriptedNodeResolver answers for every node but two, panicking for one and
// failing to resolve the other, so one abort round carries all three outcomes.
type scriptedNodeResolver struct {
	host         string
	panicking    string
	unresolvable string
}

func (r scriptedNodeResolver) NodeHostname(nodeName string) (string, bool) {
	switch nodeName {
	case r.panicking:
		panic("resolving " + nodeName)
	case r.unresolvable:
		return "", false
	}
	return r.host, true
}

// An abort reports per node, so a node whose goroutine panicked must reach the
// caller as a node that was not aborted rather than as one that was.
func TestCoordinatorAbortReportsRecoveredPanics(t *testing.T) {
	// the integration job disables recovery, under which a panic takes the
	// test binary down instead of reaching the group
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	logger, hook := logrustest.NewNullLogger()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := ObjectsExpiredAbortResponse{Aborted: true}
		response.SetContentTypeHeader(w)
		w.WriteHeader(http.StatusOK)
		assert.NoError(t, json.NewEncoder(w).Encode(response))
	}))
	defer server.Close()

	getter := schemaUC.NewMockSchemaGetter(t)
	getter.EXPECT().NodeName().Return("node1")
	getter.EXPECT().Nodes().Return([]string{"node1", "node2", "node3", "node4"})

	resolver := scriptedNodeResolver{
		host:         strings.TrimPrefix(server.URL, "http://"),
		panicking:    "node3",
		unresolvable: "node4",
	}
	c := NewCoordinator(nil, getter, nil, nil, configRuntime.NewDynamicValue[float64](1),
		logger, server.Client(), resolver, NewLocalStatus())

	aborted, err := c.Abort(context.Background(), false)

	assert.True(t, aborted, "node2 aborted")

	report := logEntry(hook, "abort ttl deletion on all nodes")
	require.NotNil(t, report, "the abort must report what it reached")
	assert.Equal(t, map[string]bool{"node1": false, "node2": true, "node3": false, "node4": false},
		report.Data["nodes"], "a node the abort asked must be named whether or not it answered")

	require.Error(t, err, "a node the abort never reached must not read as one it did")
	assert.ErrorContains(t, err, "\"node3\": {panic occurred")
	assert.ErrorContains(t, err, "resolving node3")
	assert.ErrorContains(t, err, "\"node4\": {unable to resolve hostname for node4")
}
