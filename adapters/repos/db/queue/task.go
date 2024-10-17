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

package queue

import "context"

type Task interface {
	Op() uint8
	Key() uint64
	Execute(ctx context.Context) error
}

type TaskGrouper interface {
	NewGroup(op uint8, tasks ...Task) Task
}

type Batch struct {
	Tasks []Task
	Ctx   context.Context
	Done  func()
}

type TaskDecoder interface {
	DecodeTask(*Decoder) (Task, error)
}
