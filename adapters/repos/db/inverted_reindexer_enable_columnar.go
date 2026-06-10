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

package db

import (
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/schema"
)

// NewRuntimeEnableColumnarTask creates a ShardReindexTaskGeneric configured
// for runtime (live) enable-columnar migration. It builds columnar buckets
// from existing data, enabling indexColumnar on numeric/date properties.
func NewRuntimeEnableColumnarTask(
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
	propNames []string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	strategy := &EnableColumnarStrategy{
		schemaManager: schemaManager,
		propNames:     propNames,
		generation:    generation,
	}
	return newRuntimePerPropertyTask("EnableColumnar", logger, strategy, propNames, collectionName)
}
