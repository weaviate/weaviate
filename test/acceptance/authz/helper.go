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

package test

import (
	"testing"

	"github.com/stretchr/testify/require"
	gql "github.com/weaviate/weaviate/client/graphql"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/objects"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/go-openapi/runtime"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	UUID1 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241")
	UUID2 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168242")
	UUID3 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168243")
	UUID4 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168244")
	UUID5 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168245")
	UUID6 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168246")
)

func deleteObjectClass(t *testing.T, class string, auth runtime.ClientAuthInfoWriter) {
	delParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, auth)
	helper.AssertRequestOk(t, delRes, err, nil)
}

func createClass(t *testing.T, class *models.Class, auth runtime.ClientAuthInfoWriter) error {
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, auth)
	return err
}

func generateMissingLists(permissions []*models.Permission) [][]*models.Permission {
	var result [][]*models.Permission

	for i := range permissions {
		missingList := append(permissions[:i], permissions[i+1:]...)
		result = append(result, missingList)
	}

	return result
}

func createObject(t *testing.T, object *models.Object, key string) (*objects.ObjectsCreateOK, error) {
	params := objects.NewObjectsCreateParams().WithBody(object)
	return helper.Client(t).Objects.ObjectsCreate(params, helper.CreateAuth(key))
}

func String(s string) *string {
	return &s
}

func queryGQL(t *testing.T, query, key string) (*gql.GraphqlPostOK, error) {
	params := gql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: "", Query: query, Variables: nil})
	return helper.Client(t).Graphql.GraphqlPost(params, helper.CreateAuth(key))
}

func assertGQL(t *testing.T, query, key string) *models.GraphQLResponse {
	params := gql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: "", Query: query, Variables: nil})
	resp, err := helper.Client(t).Graphql.GraphqlPost(params, helper.CreateAuth(key))
	require.Nil(t, err)
	if len(resp.Payload.Errors) > 0 {
		t.Logf("Error: %s", resp.Payload.Errors[0].Message)
	}
	require.Equal(t, len(resp.Payload.Errors), 0)
	return resp.Payload
}
