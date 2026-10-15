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

package filters

import (
	"sync"

	"github.com/weaviate/weaviate/entities/inverted"
)

// containsKeysCache holds the batched Contains keys built from one Value.
type containsKeysCache struct {
	once sync.Once
	keys inverted.SortedKeys
	err  error
}

// containsKeysInit guards lazily attaching a cache to a Value. A Value holds
// only a pointer, so copying one is not a vet copylocks error.
var containsKeysInit sync.Mutex

// ContainsKeys returns the keys build makes, calling it once per Value and
// sharing its keys or error with every caller, who wait out a build in progress.
// A Value reaches only shards of one class, so every caller would build the same.
func (v *Value) ContainsKeys(build func() (inverted.SortedKeys, error)) (inverted.SortedKeys, error) {
	containsKeysInit.Lock()
	if v.containsKeys == nil {
		v.containsKeys = &containsKeysCache{}
	}
	c := v.containsKeys
	containsKeysInit.Unlock()

	c.once.Do(func() { c.keys, c.err = build() })
	return c.keys, c.err
}
