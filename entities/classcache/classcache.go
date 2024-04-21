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

package classcache

import (
	"sync"

	"github.com/weaviate/weaviate/entities/models"
)

type classCache sync.Map

type classCacheEntry struct {
	class   *models.Class
	version uint64
}

func (cc *classCache) Load(name string) (*classCacheEntry, bool) {
	if e, ok := (*sync.Map)(cc).Load(name); ok {
		return e.(*classCacheEntry), true
	}
	return nil, false
}

func (cc *classCache) LoadOrStore(name string, entry *classCacheEntry) (*classCacheEntry, bool) {
	e, ok := (*sync.Map)(cc).LoadOrStore(name, entry)
	return e.(*classCacheEntry), ok
}

// func (cc *classCache) Store(name string, entry *classCacheEntry) {
// 	(*sync.Map)(cc).Store(name, entry)
// }

func (cc *classCache) Delete(name string) {
	(*sync.Map)(cc).Delete(name)
}
