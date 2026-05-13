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

package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespacedCollectionCountLimit checks that MAXIMUM_ALLOWED_COLLECTIONS_COUNT
// is enforced per namespace on NAMESPACES_ENABLED clusters. With a cap of 1,
// each namespace gets its own budget — a second create in the same namespace
// is rejected with 429 / USAGE_LIMIT_EXCEEDED, but a parallel namespace stays
// unaffected.
func TestNamespacedCollectionCountLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const (
		adminUser = "admin-user"
		adminKey  = "admin-key"
	)

	compose, err := docker.New().
		WithWeaviate().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithDbUsers().
		WithRBAC().
		WithRbacRoots(adminUser).
		WithNamespaces().
		WithWeaviateEnv("MAXIMUM_ALLOWED_COLLECTIONS_COUNT", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	// Operator path: only root can create namespaces and namespaced DB users
	// on an NS-enabled cluster. Namespaced users then create the collections.
	helper.CreateNamespace(t, "customer1", adminKey)
	defer helper.DeleteNamespace(t, "customer1", adminKey)

	const u1Subject = "u1"
	u1Key := helper.CreateUserWithNamespace(t, u1Subject, "customer1", adminKey)
	defer helper.DeleteUser(t, "customer1:"+u1Subject, adminKey)
	helper.AssignRoleToUser(t, adminKey, "admin", "customer1:"+u1Subject)
	defer helper.RevokeRoleFromUser(t, adminKey, "admin", "customer1:"+u1Subject)

	// First class in customer1 uses up that namespace's budget.
	helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, u1Key)
	defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)

	// Second class in the same namespace hits the per-namespace cap.
	_, err = helper.CreateClassAuthWithReturn(t, &models.Class{Class: "Films"}, u1Key)
	require.Error(t, err)
	var tooMany *clschema.SchemaObjectsCreateTooManyRequests
	require.True(t, errors.As(err, &tooMany),
		"expected SchemaObjectsCreateTooManyRequests, got %T: %v", err, err)
	require.NotNil(t, tooMany.Payload)
	assert.Equal(t, "USAGE_LIMIT_EXCEEDED", tooMany.Payload.ErrorCode)
	assert.Equal(t, "collections", tooMany.Payload.Limit)
	assert.Equal(t, int64(1), tooMany.Payload.Value)

	// A different namespace has its own budget and can still create a class.
	helper.CreateNamespace(t, "customer2", adminKey)
	defer helper.DeleteNamespace(t, "customer2", adminKey)

	const u2Subject = "u2"
	u2Key := helper.CreateUserWithNamespace(t, u2Subject, "customer2", adminKey)
	defer helper.DeleteUser(t, "customer2:"+u2Subject, adminKey)
	helper.AssignRoleToUser(t, adminKey, "admin", "customer2:"+u2Subject)
	defer helper.RevokeRoleFromUser(t, adminKey, "admin", "customer2:"+u2Subject)

	helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, u2Key)
	defer helper.DeleteClassAuth(t, "customer2:Movies", adminKey)
}
