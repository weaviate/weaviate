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
	"sync"
	"testing"
)

// TestStoreLeaderConcurrentWithRaftAssignment races Open's raft-field publication against an RPC-served Leader read.
func TestStoreLeaderConcurrentWithRaftAssignment(t *testing.T) {
	st := &Store{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = st.Leader()
			_, _ = st.LeaderWithID()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			st.raft.Store(nil)
		}
	}()
	wg.Wait()
}
