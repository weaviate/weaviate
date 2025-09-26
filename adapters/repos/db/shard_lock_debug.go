//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import "fmt"

// DEBUG METHOD: don't use in any real production use-case
// - This method is designed to simulate document ID locks in the shard
func (s *Shard) DebugLockDocIds() error {
	if s.docIdLock == nil {
		return fmt.Errorf("docIdLock is nil")
	}
	for i := range s.docIdLock {
		l := &s.docIdLock[i]
		l.Lock()
	}
	return nil
}

// DEBUG METHOD: don't use in any real production use-case
// - This method is designed to simulate document ID unlocks in the shard
func (s *Shard) DebugUnlockDocIds() bool {
	output := false
	for i := range s.docIdLock {
		l := &s.docIdLock[i]
		if l.TryLock() {
			l.Unlock()
		} else {
			l.Unlock()
			output = true
		}
	}
	return output
}

// DEBUG METHOD: don't use in any real production use-case
// - This method gets the lock status of the document IDs in the shard
func (s *Shard) DebugGetDocIdLockStatus() (bool, error) {
	if s.docIdLock == nil {
		return false, fmt.Errorf("docIdLock is nil")
	}
	output := false
	for i := range s.docIdLock {
		l := &s.docIdLock[i]
		if l.TryLock() {
			l.Unlock()
		} else {
			output = true
		}
	}
	return output, nil
}

func (s *LazyLoadShard) DebugLockDocIds() error {
	if s.shard == nil {
		return fmt.Errorf("shard is nil")
	}
	return s.shard.DebugLockDocIds()
}

func (s *LazyLoadShard) DebugUnlockDocIds() bool {
	if s.shard == nil {
		return false
	}
	return s.shard.DebugUnlockDocIds()
}

func (s *LazyLoadShard) DebugGetDocIdLockStatus() (bool, error) {
	if s.shard == nil {
		return false, fmt.Errorf("shard is nil")
	}
	return s.shard.DebugGetDocIdLockStatus()
}

func (s *MockShardLike) DebugLockDocIds() error {
	return nil
}

func (s *MockShardLike) DebugUnlockDocIds() bool {
	return false
}

func (s *MockShardLike) DebugGetDocIdLockStatus() (bool, error) {
	return false, nil
}
