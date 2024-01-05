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

package auth_tests

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/auth"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate/entities/models"

	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
)

const (
	wcsUserOnAdmin    = "ms_2d0e007e7136de11d5f29fce7a53dae219a51458@existiert.net"
	wcsUserNotOnAdmin = "ms_f0559e7899681721a44d9ca1f7418ff3cc6321c3@muellmail.com"
)

func TestAuthGraphQLUnauthenticated(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: os.Getenv(weaviateEndpoint)})
	_, err := c.GraphQL().Raw().WithQuery("{__schema {queryType {fields {name}}}}").Do(ctx)
	require.NotNil(t, err)
}

func TestAuthGraphQLValidUserNotOnAdminlist(t *testing.T) {
	pw := os.Getenv("WCS_DUMMY_CI_PW_2")
	if pw == "" {
		t.Skip("No password supplied")
	}

	ctx := context.Background()

	conf := client.Config{
		Scheme:     "http",
		Host:       os.Getenv(weaviateEndpoint),
		AuthConfig: auth.ResourceOwnerPasswordFlow{Username: wcsUserNotOnAdmin, Password: pw},
	}
	c, err := client.NewClient(conf)
	require.Nil(t, err)

	_, err = c.GraphQL().Raw().WithQuery("{__schema {queryType {fields {name}}}}").Do(ctx)
	require.NotNil(t, err)
}

func TestAuthGraphQLValidUser(t *testing.T) {
	pwAdminUser := os.Getenv("WCS_DUMMY_CI_PW")
	pwNoAdminUser := os.Getenv("WCS_DUMMY_CI_PW_2")
	if pwAdminUser == "" || pwNoAdminUser == "" {
		t.Skip("No password supplied")
	}

	ctx := context.Background()
	conf := client.Config{
		Scheme:     "http",
		Host:       os.Getenv(weaviateEndpoint),
		AuthConfig: auth.ResourceOwnerPasswordFlow{Username: wcsUserOnAdmin, Password: pwAdminUser},
	}
	c, err := client.NewClient(conf)
	require.Nil(t, err)

	// add a class so schema is not empty
	require.Nil(t, c.Schema().AllDeleter().Do(ctx))
	require.Nil(t, c.Schema().ClassCreator().WithClass(&models.Class{Class: "Pizza"}).Do(ctx))

	t.Run("returns schema without error for admin", func(t *testing.T) {
		_, err = c.GraphQL().Raw().WithQuery("{__schema {queryType {fields {name}}}}").Do(ctx)
		require.Nil(t, err)
	})

	t.Run("returns auth error for non-admin", func(t *testing.T) {
		conf2 := client.Config{
			Scheme:     "http",
			Host:       os.Getenv(weaviateEndpoint),
			AuthConfig: auth.ResourceOwnerPasswordFlow{Username: wcsUserNotOnAdmin, Password: pwNoAdminUser},
		}
		cNoAdmin, err := client.NewClient(conf2)
		require.Nil(t, err)

		_, err = cNoAdmin.GraphQL().Raw().WithQuery("{__schema {queryType {fields {name}}}}").Do(ctx)
		require.NotNil(t, err)
		wErr, ok := err.(*fault.WeaviateClientError)
		require.True(t, ok)

		require.Contains(t, wErr.DerivedFromError.Error(), "forbidden")
	})
}
