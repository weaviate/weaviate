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
	"testing"

	"github.com/stretchr/testify/require"
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
