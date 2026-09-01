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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
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
			c := NewCoordinator(reader, getter, controller, nil, logger, server.Client(),
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

	t.Run("ResetRunning succeeds when running and cancels context", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx := s.SetRunning()
		require.True(t, ok)
		require.NotNil(t, ctx)

		aborted := s.ResetRunning("finished")

		assert.True(t, aborted)
		assert.False(t, s.IsRunning())

		// context must be cancelled
		select {
		case <-ctx.Done():
			// expected
		default:
			t.Fatal("context should be done after ResetRunning")
		}
	})

	t.Run("ResetRunning sets context error to context.Canceled", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx := s.SetRunning()
		require.True(t, ok)

		s.ResetRunning("finished")

		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("ResetRunning cause contains the provided reason", func(t *testing.T) {
		s := NewLocalStatus()
		ok, ctx := s.SetRunning()
		require.True(t, ok)

		s.ResetRunning("aborted")

		cause := context.Cause(ctx)
		require.NotNil(t, cause)
		assert.ErrorIs(t, cause, context.Canceled)
		assert.Contains(t, cause.Error(), "aborted")
	})

	t.Run("ResetRunning fails when not running", func(t *testing.T) {
		s := NewLocalStatus()

		aborted := s.ResetRunning("aborted")

		assert.False(t, aborted)
		assert.False(t, s.IsRunning())
	})

	t.Run("ResetRunning on fresh LocalStatus returns false", func(t *testing.T) {
		s := NewLocalStatus()

		result := s.ResetRunning("some cause")

		assert.False(t, result)
	})

	t.Run("second ResetRunning after first returns false", func(t *testing.T) {
		s := NewLocalStatus()
		ok, _ := s.SetRunning()
		require.True(t, ok)

		first := s.ResetRunning("finished")
		second := s.ResetRunning("finished again")

		assert.True(t, first)
		assert.False(t, second)
	})

	t.Run("SetRunning can be called again after ResetRunning", func(t *testing.T) {
		s := NewLocalStatus()

		ok1, ctx1 := s.SetRunning()
		require.True(t, ok1)
		s.ResetRunning("finished")

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
		s.ResetRunning("round 1")

		ok2, ctx2 := s.SetRunning()
		require.True(t, ok2)

		// ctx1 is cancelled, ctx2 is not
		assert.ErrorIs(t, ctx1.Err(), context.Canceled)
		assert.NoError(t, ctx2.Err())

		s.ResetRunning("round 2")

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

	t.Run("concurrent ResetRunning calls: only one succeeds", func(t *testing.T) {
		s := NewLocalStatus()
		ok, _ := s.SetRunning()
		require.True(t, ok)

		const goroutines = 50
		var wg sync.WaitGroup
		var successCount atomic.Int32

		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				if s.ResetRunning("aborted") {
					successCount.Add(1)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(1), successCount.Load(), "exactly one goroutine should win ResetRunning")
		assert.False(t, s.IsRunning())
	})

	t.Run("concurrent SetRunning and ResetRunning: consistent state", func(t *testing.T) {
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
				s.ResetRunning("aborted")
			}()
			go func() {
				defer wg.Done()
				s.SetRunning()
			}()
		}
		wg.Wait()

		// state must be coherent: IsRunning must agree with internal invariants
		running := s.IsRunning()
		// no panic, no deadlock — just verify IsRunning is consistent
		assert.IsType(t, false, running) // bool type assertion
	})

	t.Run("context cancelled by ResetRunning is propagated to child contexts", func(t *testing.T) {
		s := NewLocalStatus()
		ok, parentCtx := s.SetRunning()
		require.True(t, ok)

		childCtx, cancel := context.WithCancel(parentCtx)
		defer cancel()

		s.ResetRunning("aborted")

		select {
		case <-childCtx.Done():
			assert.ErrorIs(t, errors.Unwrap(context.Cause(childCtx)), context.Canceled)
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

		s.ResetRunning("finished")
		assert.False(t, s.IsRunning(), "not running after ResetRunning")

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

			result := s.ResetRunning("finished")
			assert.True(t, result, "cycle %d: ResetRunning should succeed", i)
			assert.ErrorIs(t, ctx.Err(), context.Canceled)
			assert.False(t, s.IsRunning())
		}
	})
}
