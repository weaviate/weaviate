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

package cluster

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

func TestStoreExecute_CollectionCap(t *testing.T) {
	newStore := func(t *testing.T, namespaces ...string) (*Store, *MockStore) {
		t.Helper()
		srv, m := newBarrierTestStore(t)
		for i, ns := range namespaces {
			require.NoError(t, m.cfg.NamespacesController.Create(
				command.Namespace{Name: ns, HomeNodes: []string{m.cfg.NodeID}}, uint64(i+1)))
		}
		return srv.store, m
	}

	executeAsync := func(st *Store, cmd *command.ApplyRequest) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := st.Execute(cmd)
			done <- err
		}()
		return done
	}

	t.Run("a concurrent burst admits exactly the free capacity", func(t *testing.T) {
		const (
			limit   = 3
			seeded  = 1
			callers = 8
		)
		st, m := newStore(t, "ns1")
		st.schemaManager.SetCollectionLimit(func() int { return limit }, nil)
		for i := 0; i < seeded; i++ {
			_, err := st.Execute(addClassCmd(t, fmt.Sprintf("ns1:Seed%d", i)))
			require.NoError(t, err)
		}

		// A slow apply makes every caller read the count before the first
		// apply adds to it, unless Execute runs them one at a time.
		m.indexer.ExpectedCalls = nil
		m.indexer.On("Open", mock.Anything).Return(nil)
		m.indexer.On("Close", mock.Anything).Return(nil)
		m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
		m.indexer.On("AddClass", mock.Anything).
			Run(func(mock.Arguments) { time.Sleep(50 * time.Millisecond) }).
			Return(nil)

		cmds := make([]*command.ApplyRequest, callers)
		for i := range cmds {
			cmds[i] = addClassCmd(t, fmt.Sprintf("ns1:Burst%d", i))
		}

		var admitted, capped atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for _, cmd := range cmds {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := st.Execute(cmd)
				if err == nil {
					admitted.Add(1)
					return
				}
				le, ok := usagelimits.AsLimitExceeded(err)
				if ok && le.Limit == usagelimits.LimitCollections && le.Value == limit {
					capped.Add(1)
					return
				}
				t.Errorf("unexpected Execute error: %v", err)
			}()
		}
		close(start)
		wg.Wait()

		require.Equal(t, int32(limit-seeded), admitted.Load(), "admitted")
		require.Equal(t, int32(callers-(limit-seeded)), capped.Load(), "capped")
	})

	t.Run("creates in different namespaces do not serialize", func(t *testing.T) {
		st, _ := newStore(t, "ns1", "ns2")
		st.schemaManager.SetCollectionLimit(func() int { return 100 }, nil)

		st.collectionAddLocks.Lock("ns1")
		blocked := executeAsync(st, addClassCmd(t, "ns1:Blocked"))

		select {
		case err := <-executeAsync(st, addClassCmd(t, "ns2:Free")):
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("an ns2 create waited on ns1's lock")
		}

		select {
		case <-blocked:
			t.Fatal("an ns1 create ran while ns1's lock was held")
		case <-time.After(300 * time.Millisecond):
		}

		st.collectionAddLocks.Unlock("ns1")
		select {
		case err := <-blocked:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("the ns1 create never returned after unlock")
		}
	})

	// No limit is installed, so NewFSM saw a nil MaxCollections.
	t.Run("uncapped takes no lock", func(t *testing.T) {
		st, _ := newStore(t, "ns1")
		require.False(t, st.schemaManager.CollectionLimitEnforced())

		st.collectionAddLocks.Lock("ns1")
		defer st.collectionAddLocks.Unlock("ns1")
		select {
		case err := <-executeAsync(st, addClassCmd(t, "ns1:Free")):
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("an uncapped create waited on the namespace lock")
		}
	})
}
