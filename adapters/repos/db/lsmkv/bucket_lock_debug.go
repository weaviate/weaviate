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

package lsmkv

import "fmt"

// DEBUG METHOD: don't use in any real production use-case
// - This method is designed to simulate a segment group maintenance lock in the bucket
func (b *Bucket) DebugLockSegmentGroup() error {
	if b.disk == nil {
		return fmt.Errorf("disk is nil")
	}
	b.disk.maintenanceLock.Lock()
	return nil
}

// DEBUG METHOD: don't use in any real production use-case
// - This method is designed to simulate a segment group maintenance unlock in the bucket
func (b *Bucket) DebugUnlockSegmentGroup() bool {
	output := false
	if b.disk != nil {
		if b.disk.maintenanceLock.TryLock() {
			b.disk.maintenanceLock.Unlock()
			output = false
		} else {
			b.disk.maintenanceLock.Unlock()
			output = true
		}
	}
	return output
}

// DEBUG METHOD: don't use in any real production use-case
// - This method gets the lock status of the segment group in the bucket
func (b *Bucket) DebugGetSegmentGroupLockStatus() (bool, error) {
	if b.disk == nil {
		return false, fmt.Errorf("disk is nil")
	}
	if b.disk.maintenanceLock.TryLock() {
		b.disk.maintenanceLock.Unlock()
		return false, nil
	}
	return true, nil
}
