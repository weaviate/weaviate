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

func (cc *classCache) Load(name string) (*models.Class, bool) {
	if c, ok := (*sync.Map)(cc).Load(name); ok {
		return c.(*models.Class), true
	}
	return nil, false
}

func (cc *classCache) Store(name string, class *models.Class) {
	(*sync.Map)(cc).Store(name, class)
}
