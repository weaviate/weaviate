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

package auth_tests

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/fault"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"

	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
)

const (
	userOnAdminlist    = "admin-user"
	userNotOnAdminlist = "custom-user"
	schemaQuery        = "{__schema {queryType {fields {name}}}}"
)

func newClient(t *testing.T, oidcSubject string) *client.Client {
	t.Helper()
	conf := client.Config{Scheme: "http", Host: os.Getenv(weaviateEndpoint)}
	if oidcSubject != "" {
		token, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, os.Getenv(mockOIDCHelperEndpoint), oidcSubject)
		conf.Headers = map[string]string{"Authorization": "Bearer " + token}
	}
	c, err := client.NewClient(conf)
	require.NoError(t, err)
	return c
}

func TestAuthGraphQL(t *testing.T) {
	ctx := context.Background()

	admin := newClient(t, userOnAdminlist)
	// add a class so schema is not empty
	require.NoError(t, admin.Schema().AllDeleter().Do(ctx))
	require.NoError(t, admin.Schema().ClassCreator().WithClass(&models.Class{Class: "Pizza"}).Do(ctx))

	tests := []struct {
		name        string
		oidcSubject string
		wantErr     string
	}{
		{name: "anonymous user", wantErr: "forbidden"},
		{name: "valid user not on adminlist", oidcSubject: userNotOnAdminlist, wantErr: "forbidden"},
		{name: "valid user on adminlist", oidcSubject: userOnAdminlist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newClient(t, tt.oidcSubject).GraphQL().Raw().WithQuery(schemaQuery).Do(ctx)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			var wErr *fault.WeaviateClientError
			require.True(t, errors.As(err, &wErr), "unexpected error: %v", err)
			require.Contains(t, wErr.DerivedFromError.Error(), tt.wantErr)
		})
	}
}
