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
	"context"
)

// Hand-written supplement that brings MockShardLike up to date with
// the interface methods added during the reindex extraction. Once
// `make mocks` regenerates with mockery on a host that satisfies
// go.mod's toolchain requirement, this file becomes redundant and
// should be deleted.

func (_m *MockShardLike) CleanStalePartialReindexState(ctx context.Context, propName, indexType string) error {
	ret := _m.Called(ctx, propName, indexType)
	if rf, ok := ret.Get(0).(func(context.Context, string, string) error); ok {
		return rf(ctx, propName, indexType)
	}
	if ret.Get(0) == nil {
		return nil
	}
	return ret.Get(0).(error)
}
