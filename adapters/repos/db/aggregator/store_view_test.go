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

package aggregator

import (
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
)

// storeView serves every bucket from the store with the schema's settings.
type storeView struct{ lsmkv.BucketLookup }

func (storeView) Tokenization(_, schemaTokenization string) string { return schemaTokenization }

func (storeView) RangeableReady(prop *models.Property) bool { return inverted.HasRangeableIndex(prop) }

func (storeView) FallbackToSearchable() bool { return false }
