//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package helpers

// AllowList groups a list of possible indexIDs to be passed to a secondary
// index. The secondary index must make sure that it only returns result
// present on the AllowList
type AllowList map[uint32]struct{}

// Inserting and reading is not thread-safe. However, if inserting has
// completed, and the list can be considered read-only, it is safe to read from
// it concurrently
func (al AllowList) Insert(id uint32) {
	// no need to check if it's already present, simply overwrite
	al[id] = struct{}{}
}

// Contains is not thread-safe if the list is still being filled. However, if
// you can guarantee that the list is no longer being inserted into and it
// effectively becomes read-only, you can safely read concurrently
func (al AllowList) Contains(id uint32) bool {
	_, ok := al[id]
	return ok
}

func (al AllowList) DeepCopy() AllowList {
	out := AllowList{}

	for id := range al {
		out[id] = struct{}{}
	}

	return out
}
