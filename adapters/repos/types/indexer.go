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

package types

import (
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/schema"
)

type Indexer interface {
	GetIndex(name schema.ClassName) *db.Index
}
