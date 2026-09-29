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
	"context"
	"fmt"
	"sync"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// TestNotifyConcurrentCandidates hammers Notify the way concurrent NotifyPeer
// RPC handlers do during cluster bootstrap; run with -race.
func TestNotifyConcurrentCandidates(t *testing.T) {
	ms := NewMockStore(t, "N1", 9526)
	st := ms.Store(nil)
	st.cfg.BootstrapExpect = 1_000_000 // never reach the bootstrap threshold
	st.open.Store(true)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				require.NoError(t, st.Notify(fmt.Sprintf("node-%d-%d", g, j), "10.0.0.1:8300"))
			}
		}(g)
	}
	wg.Wait()
}

// TestNotifyConcurrentBootstrapOnce races notifies across the bootstrap threshold; run with -race.
func TestNotifyConcurrentBootstrapOnce(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "N1", utils.MustGetFreeTCPPort())
	hook := logrustest.NewLocal(m.logger)
	st := m.Store(nil)
	st.cfg.BootstrapExpect = 3
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	srv := NewRaft(mocks.NewMockNodeSelector(), st, nil)
	require.NoError(t, srv.Open(ctx, m.indexer))
	defer srv.Close(ctx)

	require.NoError(t, st.Notify(m.cfg.NodeID, fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)))
	require.NoError(t, st.Notify("seed", "10.1.0.1:8300"))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				require.NoError(t, st.Notify(fmt.Sprintf("node-%d-%d", g, j), fmt.Sprintf("10.0.%d.%d:8300", g, j)))
			}
		}(g)
	}
	close(start)
	wg.Wait()

	bootstraps := 0
	for _, e := range hook.AllEntries() {
		if e.Message == "starting cluster bootstrapping" {
			bootstraps++
		}
	}
	require.Equal(t, 1, bootstraps)
	require.True(t, st.bootstrapped.Load())
	require.Zero(t, st.candidatesLen())
}
