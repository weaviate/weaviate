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

package telemetry

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type fakeNodesStatusGetter struct {
	mock.Mock
}

func (n *fakeNodesStatusGetter) LocalNodeStatus(ctx context.Context,
	className, shardName, verbosity string,
) *models.NodeStatus {
	args := n.Called(ctx, className, shardName, verbosity)
	if args.Get(0) != nil {
		return args.Get(0).(*models.NodeStatus)
	}
	return nil
}

type fakeSchemaManager struct {
	mock.Mock
}

func (f *fakeSchemaManager) GetSchemaSkipAuth() schema.Schema {
	if len(f.ExpectedCalls) > 0 {
		args := f.Called()
		if args.Get(0) != nil {
			return args.Get(0).(schema.Schema)
		}
	}
	return schema.Schema{}
}
