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
)

type fakeNodesStatusGetter struct {
	mock.Mock
}

func (n *fakeNodesStatusGetter) LocalNodeStatus(ctx context.Context,
	className, verbosity string,
) *models.NodeStatus {
	args := n.Called(ctx, className, verbosity)
	if args.Get(0) != nil {
		return args.Get(0).(*models.NodeStatus)
	}
	return nil
}

type fakeModulesProvider struct {
	mock.Mock
}

func (m *fakeModulesProvider) GetMeta() (map[string]interface{}, error) {
	args := m.Called()
	if args.Get(0) != nil {
		return args.Get(0).(map[string]interface{}), nil
	}
	return nil, args.Error(1)
}
