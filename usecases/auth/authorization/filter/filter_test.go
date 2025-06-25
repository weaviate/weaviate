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

package filter

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		Name   string
		Config rbacconf.Config
		Items  []*models.Object
	}{
		{
			Name:   "rbac enabled, no objects",
			Items:  []*models.Object{},
			Config: rbacconf.Config{Enabled: true},
		},
		{
			Name:   "rbac disenabled, no objects",
			Items:  []*models.Object{},
			Config: rbacconf.Config{Enabled: false},
		},
	}

	l, _ := test.NewNullLogger()

	authorizer := mocks.NewMockAuthorizer()
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			resourceFilter := New[*models.Object](authorizer, tt.Config)
			filteredObjects := resourceFilter.Filter(
				context.Background(),
				l,
				&models.Principal{Username: "user"},
				tt.Items,
				authorization.READ,
				func(obj *models.Object) string {
					return ""
				},
			)

			require.Equal(t, len(tt.Items), len(filteredObjects))
		})
	}
}
