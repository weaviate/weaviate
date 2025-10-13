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
