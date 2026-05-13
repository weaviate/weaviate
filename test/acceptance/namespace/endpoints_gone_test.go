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

package namespace

import (
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/classifications"
	gql "github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_EndpointsGone locks in that endpoints incompatible with
// namespace-enabled clusters return HTTP 410 Gone with an ErrorResponse body.
func TestNamespaces_EndpointsGone(t *testing.T) {
	t.Run("GET /v1/graphql returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Graphql.GraphqlPost(
			gql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{Query: "{ Get { Foo { _additional { id } } } }"}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *gql.GraphqlPostGone
		require.True(t, errors.As(err, &gone), "expected GraphqlPostGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	t.Run("POST /v1/graphql/batch returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Graphql.GraphqlBatch(
			gql.NewGraphqlBatchParams().WithBody(models.GraphQLQueries{
				{Query: "{ Get { Foo { _additional { id } } } }"},
			}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *gql.GraphqlBatchGone
		require.True(t, errors.As(err, &gone), "expected GraphqlBatchGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	t.Run("GET /v1/objects without ?class= returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsList(
			objects.NewObjectsListParams(),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *objects.ObjectsListGone
		require.True(t, errors.As(err, &gone), "expected ObjectsListGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	t.Run("GET /v1/objects/{id} (deprecated, no class) returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsGet(
			objects.NewObjectsGetParams().WithID(strfmt.UUID("11111111-1111-1111-1111-111111111111")),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *objects.ObjectsGetGone
		require.True(t, errors.As(err, &gone), "expected ObjectsGetGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	id := strfmt.UUID("11111111-1111-1111-1111-111111111111")

	t.Run("HEAD /v1/objects/{id} (deprecated, no class) returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsHead(
			objects.NewObjectsHeadParams().WithID(id),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *objects.ObjectsHeadGone
		require.True(t, errors.As(err, &gone), "expected ObjectsHeadGone, got %T: %v", err, err)
	})

	t.Run("DELETE /v1/objects/{id} (deprecated, no class) returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsDelete(
			objects.NewObjectsDeleteParams().WithID(id),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *objects.ObjectsDeleteGone
		require.True(t, errors.As(err, &gone), "expected ObjectsDeleteGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	t.Run("PATCH /v1/objects/{id} (deprecated, no class) returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsPatch(
			objects.NewObjectsPatchParams().WithID(id).WithBody(&models.Object{ID: id}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *objects.ObjectsPatchGone
		require.True(t, errors.As(err, &gone), "expected ObjectsPatchGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	t.Run("PUT /v1/objects/{id} (deprecated, no class) returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsUpdate(
			objects.NewObjectsUpdateParams().WithID(id).WithBody(&models.Object{ID: id}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *objects.ObjectsUpdateGone
		require.True(t, errors.As(err, &gone), "expected ObjectsUpdateGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	beacon := strfmt.URI("weaviate://localhost/Foo/22222222-2222-2222-2222-222222222222")

	t.Run("POST /v1/objects/{id}/references/{propertyName} (deprecated, no class) returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsReferencesCreate(
			objects.NewObjectsReferencesCreateParams().
				WithID(id).WithPropertyName("ref").
				WithBody(&models.SingleRef{Beacon: beacon}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *objects.ObjectsReferencesCreateGone
		require.True(t, errors.As(err, &gone), "expected ObjectsReferencesCreateGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	t.Run("PUT /v1/objects/{id}/references/{propertyName} (deprecated, no class) returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsReferencesUpdate(
			objects.NewObjectsReferencesUpdateParams().
				WithID(id).WithPropertyName("ref").
				WithBody(models.MultipleRef{{Beacon: beacon}}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *objects.ObjectsReferencesUpdateGone
		require.True(t, errors.As(err, &gone), "expected ObjectsReferencesUpdateGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	t.Run("DELETE /v1/objects/{id}/references/{propertyName} (deprecated, no class) returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Objects.ObjectsReferencesDelete(
			objects.NewObjectsReferencesDeleteParams().
				WithID(id).WithPropertyName("ref").
				WithBody(&models.SingleRef{Beacon: beacon}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *objects.ObjectsReferencesDeleteGone
		require.True(t, errors.As(err, &gone), "expected ObjectsReferencesDeleteGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	t.Run("POST /v1/classifications returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Classifications.ClassificationsPost(
			classifications.NewClassificationsPostParams().WithParams(&models.Classification{}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *classifications.ClassificationsPostGone
		require.True(t, errors.As(err, &gone), "expected ClassificationsPostGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})

	t.Run("GET /v1/classifications/{id} returns 410", func(t *testing.T) {
		_, err := helper.Client(t).Classifications.ClassificationsGet(
			classifications.NewClassificationsGetParams().WithID("11111111-1111-1111-1111-111111111111"),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var gone *classifications.ClassificationsGetGone
		require.True(t, errors.As(err, &gone), "expected ClassificationsGetGone, got %T: %v", err, err)
		require.NotNil(t, gone.Payload)
		require.NotEmpty(t, gone.Payload.Error)
		assert.NotEmpty(t, gone.Payload.Error[0].Message)
	})
}
