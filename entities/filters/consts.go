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

package filters

const (
	InternalPropBackwardsCompatID  = "id"
	InternalPropID                 = "_id"
	InternalNullIndex              = "_nullState"
	InternalPropertyLength         = "_propertyLength"
	InternalPropCreationTimeUnix   = "_creationTimeUnix"
	InternalPropLastUpdateTimeUnix = "_lastUpdateTimeUnix"
)

// NotNullState is encoded as 0, so it can be read with the IsNull operator and value false.
const (
	InternalNotNullState = iota
	InternalNullState
)
