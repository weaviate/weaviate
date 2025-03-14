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

package checks

import "github.com/weaviate/weaviate/entities/models"

// HasLegacyVectorIndex checks whether there is a configured legacy vector index on a class.
func HasLegacyVectorIndex(class *models.Class) bool {
	return class.Vectorizer != "" || class.VectorIndexConfig != nil || class.VectorIndexType != ""
}
