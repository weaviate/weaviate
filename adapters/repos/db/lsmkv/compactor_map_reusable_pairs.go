//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

// reusableMapPairs is not thread-safe and intended for usage from a single
// thread. The caller is resoponsible for initializing each element themselves,
// the Resize functions will only set the size. If the size is reduced, this
// will only truncate elements, but will not reset values.
type reusableMapPairs struct {
	left  []MapPair
	right []MapPair
}

func newReusableMapPairs() *reusableMapPairs {
	return &reusableMapPairs{}
}

func (rmp *reusableMapPairs) ResizeLeft(size int) {
	if cap(rmp.left) >= size {
		rmp.left = rmp.left[:size]
	} else {
		// The 25% overhead for the capacity was chosen because we saw a lot
		// re-allocations during testing with just a few elements more than before.
		// This is something that really depends on the user's usage pattern, but
		// in the test scenarios based on the
		// weaviate-chaos-engineering/apps/importer-no-vector-index test script a
		// simple 25% overhead reduced the resizing needs to almost zero.
		rmp.left = make([]MapPair, size, int(float64(size)*1.25))
	}
}

func (rmp *reusableMapPairs) ResizeRight(size int) {
	if cap(rmp.right) >= size {
		rmp.right = rmp.right[:size]
	} else {
		// The 25% overhead for the capacity was chosen because we saw a lot
		// re-allocations during testing with just a few elements more than before.
		// This is something that really depends on the user's usage pattern, but
		// in the test scenarios based on the
		// weaviate-chaos-engineering/apps/importer-no-vector-index test script a
		// simple 25% overhead reduced the resizing needs to almost zero.
		rmp.right = make([]MapPair, size, int(float64(size)*1.25))
	}
}
