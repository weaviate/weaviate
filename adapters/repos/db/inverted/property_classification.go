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

package inverted

import (
	"strings"

	"github.com/weaviate/weaviate/entities/schema"
)

// IsMetaCountProperty reports whether the property is one of the
// synthetic "__meta_count" entries the analyzer emits for arrays. The
// classifier is shared between the regular shard-write path and the
// runtime-reindex strategies — keeping the rule in one place avoids
// the two diverging on a future suffix tweak.
func IsMetaCountProperty(property Property) bool {
	return len(property.Name) > len(schema.InternalMetaCountSuffix) &&
		strings.HasSuffix(property.Name, schema.InternalMetaCountSuffix)
}

// IsInternalProperty reports whether the property name starts with `_`
// (the convention for synthetic internal indices like `_id` /
// `_creationTimeUnix`).
func IsInternalProperty(property Property) bool {
	return property.Name[0] == '_'
}
