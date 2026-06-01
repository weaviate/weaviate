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

import "github.com/weaviate/weaviate/entities/schema"

const (
	InternalPropBackwardsCompatID = "id"
	InternalPropID                = "_id"
	// InternalNullIndex and InternalPropertyLength are aliases for the
	// canonical property-name suffix constants defined in entities/schema.
	// They share a single source of truth so validation and bucket naming
	// cannot drift out of sync.
	InternalNullIndex              = schema.InternalNullStateSuffix
	InternalPropertyLength         = schema.InternalPropertyLengthSuffix
	InternalPropCreationTimeUnix   = "_creationTimeUnix"
	InternalPropLastUpdateTimeUnix = "_lastUpdateTimeUnix"
)

// NotNullState is encoded as 0, so it can be read with the IsNull operator and value false.
const (
	InternalNotNullState = iota
	InternalNullState
)
