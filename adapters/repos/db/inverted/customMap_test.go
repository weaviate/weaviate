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

package inverted

import "testing"

func TestCustomMap(t *testing.T) {
	customMap := NewCustomMap(1000000)
	for i := 0; i < 1000000; i++ {
		customMap.Set(uint64(i), &docPointerWithScore{Score: float64(i)})
	}
	for i := 0; i < 1000000; i++ {
		_, ok := customMap.Get(uint64(i))
		if !ok {
			t.Errorf("Could not find %v", i)
		}
	}
}
