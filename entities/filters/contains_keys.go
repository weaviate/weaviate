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

// ContainsKeys returns the keys build makes, calling it once per Value and
// sharing its keys or error with every caller, who wait out a build in progress.
// A Value reaches only shards of one class, so every caller would build the same.
func (v *Value) ContainsKeys(build func() (inverted.SortedKeys, error)) (inverted.SortedKeys, error) {
	c := &v.containsKeys
	c.once.Do(func() { c.keys, c.err = build() })
	return c.keys, c.err
}
