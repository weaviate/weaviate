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
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
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

	var aliases []string
	t.Run("create aliases", func(t *testing.T) {
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
				name:  documents.Document,
				alias: &models.Alias{Alias: "PassageAlias", Class: documents.Document},
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
			{
				name: "create with different case",
				// passing in `aliasThatCreated` but should transform into `AliasThatCreated`.
				alias: &models.Alias{Alias: "aliasThatCreated", Class: documents.Passage},
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
					expAlias := entschema.UppercaseClassName(tt.alias.Alias)
					expClass := entschema.UppercaseClassName(tt.alias.Class)
					if expAlias == alias.Alias && expClass == alias.Class {
						aliasCreated = true
					}
				}
				assert.True(t, aliasCreated)
				aliases = append(aliases, tt.alias.Alias)
			})
		}
	})

	defer func() {
		resp := helper.GetAliases(t, nil)
		require.NotNil(t, resp)
		for _, alias := range resp.Aliases {
			helper.DeleteAlias(t, alias.Alias)
		}
		helper.DeleteClass(t, books.DefaultClassName)
		helper.DeleteClass(t, documents.Passage)
		helper.DeleteClass(t, documents.Document)
	}()

	t.Run("create alias with invalid char", func(t *testing.T) {
		cases := []struct {
			name  string
			input string
		}{
			{name: "symbols1", input: "invalid_alias_!#"},
			{name: "symbols2", input: "invalid_alias_@"},
			{name: "symbols3", input: "!invalid_alias_@"},
			{name: "symbols4", input: "#invalid_alias_*"},
			{name: "empty", input: ""},
			{name: "maxlength", input: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, // more than max 255 chars
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				alias := &models.Alias{Class: books.DefaultClassName, Alias: "valid_alias_!#"}
				p := schema.NewAliasesCreateParams().WithBody(alias)
				resp, err := helper.Client(t).Schema.AliasesCreate(p, nil)
				require.Error(t, err)
				assert.Nil(t, resp)
				var cerr *schema.AliasesCreateUnprocessableEntity
				ok := errors.As(err, &cerr) // convert to concrete error type
				assert.True(t, ok)
				assert.Contains(t, cerr.Payload.Error[0].Message, "is not a valid alias name")
			})
		}
	})

	t.Run("get aliases", func(t *testing.T) {
		resp := helper.GetAliases(t, nil)
		require.NotNil(t, resp)
		require.NotEmpty(t, resp.Aliases)
		require.Equal(t, 8, len(resp.Aliases))
	})

	t.Run("get alias", func(t *testing.T) {
		resp := helper.GetAlias(t, "BookAlias")
		require.NotNil(t, resp)
		require.Equal(t, "BookAlias", resp.Alias)
	})

	t.Run("get alias different case", func(t *testing.T) {
		resp := helper.GetAlias(t, "bookAlias") // first letter is different case.
		require.NotNil(t, resp)
		require.Equal(t, "BookAlias", resp.Alias)
	})

	t.Run("get alias not found", func(t *testing.T) {
		resp := helper.GetAliasNotFound(t, "AliasThatDoesntExist")
		require.Nil(t, resp)
	})

	t.Run("replace alias", func(t *testing.T) {
		checkAlias := func(t *testing.T, aliasName, expectedClass string) {
			resp := helper.GetAlias(t, aliasName)
			require.NotNil(t, resp)
			require.Equal(t, aliasName, resp.Alias)
			require.Equal(t, expectedClass, resp.Class)
		}
		aliasName := "AliasThatWillBeReplaced"
		checkAlias(t, aliasName, documents.Passage)
		helper.UpdateAlias(t, aliasName, documents.Document)
		checkAlias(t, aliasName, documents.Document)
	})

	t.Run("replace alias different case", func(t *testing.T) {
		checkAlias := func(t *testing.T, aliasName, expectedClass string) {
			resp := helper.GetAlias(t, aliasName)
			require.NotNil(t, resp)
			require.Equal(t, aliasName, resp.Alias)
			require.Equal(t, expectedClass, resp.Class)
		}
		aliasName := "AliasThatWillBeReplaced"
		dAliasName := "aliasThatWillBeReplaced" // same with first lower case
		checkAlias(t, aliasName, documents.Document)
		helper.UpdateAlias(t, dAliasName, documents.Passage)
		checkAlias(t, aliasName, documents.Passage)
	})

	t.Run("replace non existing alias", func(t *testing.T) {
		checkAlias := func(t *testing.T, aliasName, expectedClass string) {
			resp := helper.GetAliasNotFound(t, aliasName)
			require.Nil(t, resp)
		}
		aliasName := "AliasThatDoesntExist"
		checkAlias(t, aliasName, documents.Document)
		resp, err := helper.UpdateAliasWithReturn(t, aliasName, documents.Document)
		require.Error(t, err)
		require.Nil(t, resp)
		checkAlias(t, aliasName, documents.Document)
	})

	t.Run("replace alias of non existing collection", func(t *testing.T) {
		checkAlias := func(t *testing.T, aliasName, expectedClass string) {
			resp := helper.GetAlias(t, aliasName)
			require.NotNil(t, resp)
			require.Equal(t, aliasName, resp.Alias)
			require.Equal(t, expectedClass, resp.Class)
		}
		aliasName := "AliasThatWillBeReplaced"
		checkAlias(t, aliasName, documents.Passage)
		resp, err := helper.UpdateAliasWithReturn(t, aliasName, "errorCollection")
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("try to use updateAlias with existing collection name", func(t *testing.T) {
		resp, err := helper.UpdateAliasWithReturn(t, documents.Document, documents.Passage)
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("delete alias", func(t *testing.T) {
		checkAliasesCount := func(t *testing.T, count int) {
			resp := helper.GetAliases(t, nil)
			require.NotNil(t, resp)
			require.NotEmpty(t, resp.Aliases)
			require.Equal(t, count, len(resp.Aliases))
		}
		checkAliasesCount(t, 8)
		helper.DeleteAlias(t, "AliasThatWillBeReplaced")
		checkAliasesCount(t, 7)
	})

	t.Run("delete alias different case", func(t *testing.T) {
		checkAliasesCount := func(t *testing.T, count int) {
			resp := helper.GetAliases(t, nil)
			require.NotNil(t, resp)
			require.NotEmpty(t, resp.Aliases)
			require.Equal(t, count, len(resp.Aliases))
		}
		checkAliasesCount(t, 7)
		helper.DeleteAlias(t, "aliasThatCreated") // note first letter is small
		checkAliasesCount(t, 6)
	})

	t.Run("delete alias that doesn't exist", func(t *testing.T) {
		resp, err := helper.DeleteAliasWithReturn(t, "AliasThatWillBeReplaced")
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("create with clashing names", func(t *testing.T) {
		t.Run("create aliases", func(t *testing.T) {
			tests := []struct {
				name             string
				alias            *models.Alias
				expectedErrorMsg string
			}{
				{
					// trying to create alias with existing class name.
					// Should report "Hey, that alias that your are trying to create, there already exists class name with that".
					name:             "clashing class name",
					alias:            &models.Alias{Alias: books.DefaultClassName, Class: documents.Passage},
					expectedErrorMsg: fmt.Sprintf("create alias: class %s already exists", books.DefaultClassName),
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
			tests := []struct {
				name             string
				class            *models.Class
				expectedErrorMsg string
			}{
				{
					// trying to create class with existing class name.
					name:             "with existing class name",
					class:            books.ClassModel2VecVectorizerWithName(books.DefaultClassName),
					expectedErrorMsg: fmt.Sprintf("class name %s already exists", books.DefaultClassName),
				},
				// trying to create class with existing alias name.
				{
					name:             "with existing alias name",
					class:            books.ClassModel2VecVectorizerWithName("BookAlias"),
					expectedErrorMsg: fmt.Sprintf("alias name %s already exists", "BookAlias"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					params := schema.NewSchemaObjectsCreateParams().WithObjectClass(tt.class)
					resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
					require.Error(t, err)
					assert.Nil(t, resp)
					errorPayload, _ := json.MarshalIndent(err, "", " ")
					assert.Contains(t, string(errorPayload), tt.expectedErrorMsg)
				})
			}
		})
	})

	t.Run("create alias to non existing collection", func(t *testing.T) {
		alias := &models.Alias{Alias: "NonExistingAlias", Class: "NonExistingCollection"}
		resp, err := helper.CreateAliasWithReturn(t, alias)
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("tests with BookAlias", func(t *testing.T) {
		aliasName := "BookAlias"

		assertGetObject := func(t *testing.T, id strfmt.UUID) {
			objWithClassName, err := helper.GetObject(t, books.DefaultClassName, id)
			require.NoError(t, err)
			require.NotNil(t, objWithClassName)
			assert.Equal(t, books.DefaultClassName, objWithClassName.Class)

			objWithAlias, err := helper.GetObject(t, aliasName, id)
			require.NoError(t, err)
			require.NotNil(t, objWithAlias)
			assert.Equal(t, objWithClassName.ID, objWithAlias.ID)

			// no matter how the object is accessed(via collection name or alias name),
			// returned JSON should have original class name as source of truth.
			assert.Equal(t, books.DefaultClassName, objWithAlias.Class)
		}

		t.Run("create class with alias name", func(t *testing.T) {
			class := books.ClassModel2VecVectorizerWithName(aliasName)
			params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			require.Nil(t, resp)
			require.Error(t, err)
			errorPayload, _ := json.MarshalIndent(err, "", " ")
			assert.Contains(t, string(errorPayload), fmt.Sprintf("alias name %s already exists", class.Class))
		})
		t.Run("GraphQL Get query with alias", func(t *testing.T) {
			getQuery := `
				{
					Get{
						%s%s{
							title
							description
							_additional{
								id
							}
						}
					}
				}`
			tests := []struct {
				name  string
				query string
			}{
				{
					name:  "Get",
					query: fmt.Sprintf(getQuery, aliasName, ""),
				},
				{
					name:  "Get with nearText",
					query: fmt.Sprintf(getQuery, aliasName, `(nearText:{concepts:"Dune"})`),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					res := graphqlhelper.AssertGraphQL(t, nil, tt.query).Get("Get", aliasName).AsSlice()
					require.NotEmpty(t, res)
					for _, r := range res {
						elem, ok := r.(map[string]interface{})
						require.True(t, ok)
						title, ok := elem["title"].(string)
						require.True(t, ok)
						require.NotEmpty(t, title)
						description, ok := elem["description"].(string)
						require.True(t, ok)
						require.NotEmpty(t, description)
						id, ok := elem["_additional"].(map[string]interface{})["id"].(string)
						require.True(t, ok)
						require.NotEmpty(t, id)
					}
				})
			}
		})
		t.Run("GraphQL Aggregate query with alias", func(t *testing.T) {
			aggregateQuery := `
				{
					Aggregate{
						%s%s{
							meta{count}
						}
					}
				}`
			tests := []struct {
				name  string
				query string
			}{
				{
					name:  "Aggregate",
					query: fmt.Sprintf(aggregateQuery, aliasName, ""),
				},
				{
					name:  "Aggregate with nearText",
					query: fmt.Sprintf(aggregateQuery, aliasName, `(nearText:{concepts:"Dune" certainty:0.8})`),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					res := graphqlhelper.AssertGraphQL(t, nil, tt.query).Get("Aggregate", aliasName).AsSlice()
					require.NotEmpty(t, res)
					for _, r := range res {
						elem, ok := r.(map[string]interface{})
						require.True(t, ok)
						count, ok := elem["meta"].(map[string]interface{})["count"].(json.Number)
						require.True(t, ok)
						c, err := count.Int64()
						require.NoError(t, err)
						require.Greater(t, c, int64(0))
					}
				})
			}
		})
		t.Run("get class objects with alias", func(t *testing.T) {
			assertGetObject(t, books.ProjectHailMary)
		})

		t.Run("create object with alias", func(t *testing.T) {
			objID := strfmt.UUID("67b79643-cf8b-4b22-b206-000000000001")
			obj := &models.Object{
				Class: aliasName,
				ID:    objID,
				Properties: map[string]interface{}{
					"title":       "The Martian",
					"description": "Stranded on Mars after a dust storm forces his crew to evacuate, astronaut Mark Watney is presumed dead and left alone on the hostile planet.",
				},
			}
			created, err := helper.CreateObjectWithResponse(t, obj)
			require.NoError(t, err)
			// should still return original class name in the response (not alias)
			assert.Equal(t, books.DefaultClassName, created.Class)
			assertGetObject(t, objID)
		})

		t.Run("update object with alias", func(t *testing.T) {
			objID := strfmt.UUID("67b79643-cf8b-4b22-b206-000000000001")
			obj := &models.Object{
				Class: aliasName,
				ID:    objID,
				Properties: map[string]interface{}{
					"title":       "The Martian",
					"description": "A book about an astronaut Mark Watney.",
				},
			}
			updated, err := helper.UpdateObjectWithResponse(t, obj)
			require.NoError(t, err)
			// should still return original class name in the response (not alias)
			assert.Equal(t, books.DefaultClassName, updated.Class)
			assertGetObject(t, objID)
		})

		t.Run("patch object with alias", func(t *testing.T) {
			objID := strfmt.UUID("67b79643-cf8b-4b22-b206-000000000001")
			obj := &models.Object{
				Class: aliasName,
				ID:    objID,
				Properties: map[string]interface{}{
					"title":       "The Martian",
					"description": "A book about an astronaut Mark Watney.",
				},
			}
			err := helper.PatchObject(t, obj)
			require.NoError(t, err)
			assertGetObject(t, objID)
		})

		t.Run("head object with alias", func(t *testing.T) {
			objID := strfmt.UUID("67b79643-cf8b-4b22-b206-000000000001")
			err := helper.HeadObject(t, objID)
			require.NoError(t, err)
		})

		t.Run("validate object with alias", func(t *testing.T) {
			objID := strfmt.UUID("67b79643-cf8b-4b22-b206-000000000001")
			obj := &models.Object{
				Class: aliasName,
				ID:    objID,
				Properties: map[string]interface{}{
					"title":       "The Martian",
					"description": "A book about an astronaut Mark Watney.",
				},
			}
			err := helper.ValidateObject(t, obj)
			require.NoError(t, err)
			assertGetObject(t, objID)
		})

		t.Run("batch insert with alias", func(t *testing.T) {
			objID1 := strfmt.UUID("67b79643-cf8b-4b22-b206-000000000001")
			obj1 := &models.Object{
				Class: aliasName,
				ID:    objID1,
				Properties: map[string]interface{}{
					"title":       "The Martian",
					"description": "A book about an astronaut Mark Watney that was left on Mars.",
				},
			}
			objID2 := strfmt.UUID("67b79643-cf8b-4b22-b206-000000000002")
			obj2 := &models.Object{
				Class: aliasName,
				ID:    objID2,
				Properties: map[string]interface{}{
					"title":       "Nonexistent",
					"description": "A book about nothing.",
				},
			}
			resp := helper.CreateObjectsBatchWithResponse(t, []*models.Object{obj1, obj2})
			for _, obj := range resp {
				// should still return original class name in the response (not alias)
				assert.Equal(t, books.DefaultClassName, obj.Class)
			}
			assertGetObject(t, objID1)
			assertGetObject(t, objID2)
		})

		t.Run("batch delete with alias", func(t *testing.T) {
			valueText := "Nonexistent"
			batchDelete := &models.BatchDelete{
				Match: &models.BatchDeleteMatch{
					Class: aliasName,
					Where: &models.WhereFilter{
						Path:      []string{"title"},
						Operator:  models.WhereFilterOperatorEqual,
						ValueText: &valueText,
					},
				},
			}
			resp := helper.DeleteObjectsBatchWithResponse(t, batchDelete, types.ConsistencyLevelAll)
			require.NotNil(t, resp)
			require.NotNil(t, resp.Match)
			// should still return original class name in the response (not alias)
			assert.Equal(t, books.DefaultClassName, resp.Match.Class)
		})
	})
}
