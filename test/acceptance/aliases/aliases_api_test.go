//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"github.com/weaviate/weaviate/test/helper/sample-schema/documents"
)

func Test_AliasesAPI(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithText2VecModel2Vec().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("create schema", func(t *testing.T) {
		t.Run("Books", func(t *testing.T) {
			booksClass := books.ClassModel2VecVectorizer()
			helper.CreateClass(t, booksClass)
			for _, book := range books.Objects() {
				helper.CreateObject(t, book)
				helper.AssertGetObjectEventually(t, book.Class, book.ID)
			}
		})
		t.Run("Document and Passage", func(t *testing.T) {
			docsClasses := documents.ClassesModel2VecVectorizer(false)
			helper.CreateClass(t, docsClasses[0])
			helper.CreateClass(t, docsClasses[1])
			for _, doc := range documents.Objects() {
				helper.CreateObject(t, doc)
				helper.AssertGetObjectEventually(t, doc.Class, doc.ID)
			}
		})
	})

	t.Run("create Aliases", func(t *testing.T) {
		tests := []struct {
			name  string
			alias *models.Alias
		}{
			{
				name:  books.DefaultClassName,
				alias: &models.Alias{Alias: "BookAlias", Class: books.DefaultClassName},
			},
			{
				name:  documents.Document,
				alias: &models.Alias{Alias: "DocumentAlias", Class: documents.Document},
			},
			{
				name:  documents.Passage,
				alias: &models.Alias{Alias: "PassageAlias1", Class: documents.Passage},
			},
			{
				name:  documents.Passage,
				alias: &models.Alias{Alias: "PassageAlias2", Class: documents.Passage},
			},
			{
				name:  documents.Passage,
				alias: &models.Alias{Alias: "PassageAlias3", Class: documents.Passage},
			},
			{
				name:  documents.Passage,
				alias: &models.Alias{Alias: "AliasThatWillBeReplaced", Class: documents.Passage},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				helper.CreateAlias(t, tt.alias)
				resp := helper.GetAliases(t, &tt.alias.Class)
				require.NotNil(t, resp)
				require.NotEmpty(t, resp.Aliases)
				aliasCreated := false
				for _, alias := range resp.Aliases {
					if tt.alias.Alias == alias.Alias && tt.alias.Class == alias.Class {
						aliasCreated = true
					}
				}
				assert.True(t, aliasCreated)
			})
		}
	})

	t.Run("get aliases", func(t *testing.T) {
		resp := helper.GetAliases(t, nil)
		require.NotNil(t, resp)
		require.NotEmpty(t, resp.Aliases)
		require.Equal(t, 6, len(resp.Aliases))
	})

	t.Run("replace alias", func(t *testing.T) {
		checkAlias := func(t *testing.T, aliasName, expectedClass string) {
			resp := helper.GetAlias(t, aliasName)
			require.NotNil(t, resp)
			require.NotEmpty(t, resp.Aliases)
			require.Equal(t, aliasName, resp.Aliases[0].Alias)
			require.Equal(t, expectedClass, resp.Aliases[0].Class)
		}
		aliasName := "AliasThatWillBeReplaced"
		checkAlias(t, aliasName, documents.Passage)
		helper.UpdateAlias(t, aliasName, documents.Document)
		checkAlias(t, aliasName, documents.Document)
	})

	t.Run("delete alias", func(t *testing.T) {
		checkAliasesCount := func(t *testing.T, count int) {
			resp := helper.GetAliases(t, nil)
			require.NotNil(t, resp)
			require.NotEmpty(t, resp.Aliases)
			require.Equal(t, count, len(resp.Aliases))
		}
		checkAliasesCount(t, 6)
		helper.DeleteAlias(t, "AliasThatWillBeReplaced")
		checkAliasesCount(t, 5)
	})

	t.Run("create with clashing names", func(t *testing.T) {
		t.Run("create aliases", func(t *testing.T) {
			tests := []struct {
				name             string
				alias            *models.Alias
				expectedErrorMsg string
			}{
				{
					name:             "clashing class name",
					alias:            &models.Alias{Alias: books.DefaultClassName, Class: documents.Passage},
					expectedErrorMsg: fmt.Sprintf("create alias: class %s already exists", documents.Passage),
				},
				{
					name:             "clashing alias name",
					alias:            &models.Alias{Alias: "BookAlias", Class: documents.Passage},
					expectedErrorMsg: fmt.Sprintf("create alias: alias %s already exists", "BookAlias"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					alias := tt.alias
					params := schema.NewAliasesCreateParams().WithBody(alias)
					resp, err := helper.Client(t).Schema.AliasesCreate(params, nil)
					require.Nil(t, resp)
					require.Error(t, err)
					errorPayload, _ := json.MarshalIndent(err, "", " ")
					assert.Contains(t, string(errorPayload), tt.expectedErrorMsg)
				})
			}
		})
		t.Run("create class", func(t *testing.T) {
			class := books.ClassModel2VecVectorizerWithName("BookAlias")
			params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			require.Nil(t, resp)
			require.Error(t, err)
			errorPayload, _ := json.MarshalIndent(err, "", " ")
			assert.Contains(t, string(errorPayload), fmt.Sprintf("class name %s already exists", class.Class))
		})

		t.Run("get class objects with alias", func(t *testing.T) {
			objWithClassName, err := helper.GetObject(t, books.DefaultClassName, books.ProjectHailMary)
			require.NoError(t, err)
			require.NotNil(t, objWithClassName)
			aliasName := "BookAlias"
			objWithAlias, err := helper.GetObject(t, aliasName, books.ProjectHailMary)
			require.NoError(t, err)
			require.NotNil(t, objWithAlias)
			assert.Equal(t, objWithAlias.Class, objWithAlias.Class)
			assert.Equal(t, objWithAlias.ID, objWithAlias.ID)
		})
	})
}
