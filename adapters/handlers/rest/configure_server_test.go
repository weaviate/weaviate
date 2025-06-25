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

package rest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_DummyAuthorizer(t *testing.T) {
	t.Run("when no authz is configured", func(t *testing.T) {
		authorizer := authorization.DummyAuthorizer{}

		t.Run("any request is allowed", func(t *testing.T) {
			err := authorizer.Authorize(context.Background(), nil, "delete", "the/world")
			assert.Nil(t, err)
		})
	})
}

func Test_AdminListAuthorizer(t *testing.T) {
	t.Run("when adminlist is configured", func(t *testing.T) {
		cfg := config.Config{
			Authorization: config.Authorization{
				AdminList: adminlist.Config{
					Enabled: true,
					Users:   []string{"user1"},
				},
			},
		}

		authorizer := adminlist.New(cfg.Authorization.AdminList)
		t.Run("admin requests are allowed", func(t *testing.T) {
			err := authorizer.Authorize(context.Background(), &models.Principal{Username: "user1"}, "delete", "the/world")
			assert.Nil(t, err)
		})

		t.Run("non admin requests are allowed", func(t *testing.T) {
			err := authorizer.Authorize(context.Background(), &models.Principal{Username: "user2"}, "delete", "the/world")
			assert.NotNil(t, err)
		})
	})
}
