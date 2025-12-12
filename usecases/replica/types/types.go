//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package types

import "github.com/weaviate/weaviate/cluster/router/types"

// tuple is a container for the data received from a replica
type tuple[T any] struct {
	Sender string
	UTime  int64
	O      T
	ack    int
	err    error
}
type BoolTuple tuple[types.RepairResponse]
