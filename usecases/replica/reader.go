//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"github.com/weaviate/weaviate/usecases/objects"
)

type result[T any] struct {
	data T
	err  error
}

type tuple[T any] struct {
	sender string
	UTime  int64
	o      T
	ack    int
	err    error
}

type (
	objTuple  tuple[objects.Replica]
	boolTuple tuple[RepairResponse]
)
