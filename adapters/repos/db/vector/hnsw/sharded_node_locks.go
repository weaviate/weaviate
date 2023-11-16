//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import "sync"

const nodeLockStripe = uint64(512)

type shardedNodeLocks []*sync.RWMutex

func newShardedNodeLocks() shardedNodeLocks {
	snl := make([]*sync.RWMutex, nodeLockStripe)
	for i := range snl {
		snl[i] = new(sync.RWMutex)
	}
	return snl
}

func (snl shardedNodeLocks) LockAll() {
	for i := range snl {
		snl[i].Lock()
	}
}

func (snl shardedNodeLocks) UnlockAll() {
	for i := range snl {
		snl[i].Unlock()
	}
}

func (snl shardedNodeLocks) Lock(id uint64) {
	snl[id%nodeLockStripe].Lock()
}

func (snl shardedNodeLocks) Unlock(id uint64) {
	snl[id%nodeLockStripe].Unlock()
}

func (snl shardedNodeLocks) RLockAll() {
	for i := range snl {
		snl[i].RLock()
	}
}

func (snl shardedNodeLocks) RUnlockAll() {
	for i := range snl {
		snl[i].RUnlock()
	}
}

func (snl shardedNodeLocks) RLock(id uint64) {
	snl[id%nodeLockStripe].RLock()
}

func (snl shardedNodeLocks) RUnlock(id uint64) {
	snl[id%nodeLockStripe].RUnlock()
}
