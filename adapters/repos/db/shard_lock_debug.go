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

package db

import "fmt"

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
			break
		}
	}
	return output, nil
}

func (s *LazyLoadShard) DebugGetDocIdLockStatus() (bool, error) {
	shard := s.currentShard()
	if shard == nil {
		return false, fmt.Errorf("shard is nil")
	}
	return shard.DebugGetDocIdLockStatus()
}
