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

package nodes

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/schema"
)

func TestNewManagerWithCustomTimeout(t *testing.T) {
	logger := logrus.New()
	authorizer := authorization.NewMockAuthorizer(t)
	db := NewMockdb(t)
	schemaManager := &schema.Manager{}
	rbacConfig := rbacconf.Config{}

	customTimeout := 60 * time.Second
	manager := NewManager(logger, authorizer, db, schemaManager, rbacConfig, customTimeout)

	assert.Equal(t, customTimeout, manager.timeout)
}

func TestNewManagerDefaultTimeout(t *testing.T) {
	logger := logrus.New()
	authorizer := authorization.NewMockAuthorizer(t)
	db := NewMockdb(t)
	schemaManager := &schema.Manager{}
	rbacConfig := rbacconf.Config{}

	manager := NewManager(logger, authorizer, db, schemaManager, rbacConfig, DefaultGetNodeStatusTimeout)

	assert.Equal(t, DefaultGetNodeStatusTimeout, manager.timeout)
}

func TestGetNodeStatusTimeoutBehavior(t *testing.T) {
	logger := logrus.New()
	schemaManager := &schema.Manager{}
	rbacConfig := rbacconf.Config{}

	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Create a mock DB that will delay longer than our timeout
	db := NewMockdb(t)
	db.EXPECT().GetNodeStatus(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, className, verbosity string) ([]*models.NodeStatus, error) {
			// Simulate a slow operation that takes longer than our timeout
			select {
			case <-time.After(100 * time.Millisecond):
				return []*models.NodeStatus{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	)

	manager := NewManager(logger, authorizer, db, schemaManager, rbacConfig, 10*time.Millisecond)

	ctx := context.Background()
	principal := &models.Principal{}

	// This should timeout due to the configured timeout being shorter than the delay
	_, err := manager.GetNodeStatus(ctx, principal, "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}
