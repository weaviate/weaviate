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

package local

import (
	"fmt"
	"runtime/debug"
	"testing"

	logrus "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

// These tests are component tests for the local package including all its
// subpackages, such as get, getmeta, etc.. However, they only assert that the
// graphql tree can be built under certain circumstances. This helps us to
// catch errors on edge cases like empty schemas, classes with empty
// properties, empty peer lists, peers with empty schemas, etc. However, we
// don't get any guarantee of whether the individual queries resolve
// correctly. For those cases we have unit tests in die individual subpackages
// (i.e. get, getmeta, aggregate, etc.).  Additionally we have (a few) e2e
// tests.

func TestBuild_GraphQLNetwork(t *testing.T) {
	tests := testCases{
		// This tests asserts that an action-only schema doesn't lead to errors.
		testCase{
			name: "with only objects locally",
			localSchema: schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{
						{
							Class: "BestLocalAction",
							Properties: []*models.Property{
								{
									DataType:     schema.DataTypeText.PropString(),
									Name:         "myStringProp",
									Tokenization: models.PropertyTokenizationWhitespace,
								},
							},
						},
					},
				},
			},
		},

		// This tests asserts that a things-only schema doesn't lead to errors.
		testCase{
			name: "with only objects locally",
			localSchema: schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{
						{
							Class: "BestLocalThing",
							Properties: []*models.Property{
								{
									DataType:     schema.DataTypeText.PropString(),
									Name:         "myStringProp",
									Tokenization: models.PropertyTokenizationWhitespace,
								},
							},
						},
					},
				},
			},
		},

		// This tests asserts that a class without any properties doesn't lead to
		// errors.
		testCase{
			name: "with things without properties locally",
			localSchema: schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{
						{
							Class:      "BestLocalThing",
							Properties: []*models.Property{},
						},
					},
				},
			},
		},

		testCase{
			name:        "without any peers",
			localSchema: validSchema(),
		},
	}

	tests.AssertNoError(t)
}

func TestBuild_RefProps(t *testing.T) {
	t.Run("expected error logs", func(t *testing.T) {
		tests := testCases{
			{
				name: "build class with nonexistent ref prop",
				localSchema: schema.Schema{
					Objects: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "ThisClassExists",
								Properties: []*models.Property{
									{
										DataType: []string{"ThisClassDoesNotExist"},
										Name:     "ofNonexistentClass",
									},
								},
							},
						},
					},
				},
			},
		}

		expectedLogMsg := "ignoring ref prop \"ofNonexistentClass\" on class \"ThisClassExists\", " +
			"because it contains reference to nonexistent class [\"ThisClassDoesNotExist\"]"

		tests.AssertErrorLogs(t, expectedLogMsg)
	})

	t.Run("expected success", func(t *testing.T) {
		tests := testCases{
			{
				name: "build class with existing non-circular ref prop",
				localSchema: schema.Schema{
					Objects: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "ThisClassExists",
								Properties: []*models.Property{
									{
										DataType: []string{"ThisClassAlsoExists"},
										Name:     "ofExistingClass",
									},
								},
							},
							{
								Class: "ThisClassAlsoExists",
								Properties: []*models.Property{
									{
										DataType:     schema.DataTypeText.PropString(),
										Name:         "stringProp",
										Tokenization: models.PropertyTokenizationWhitespace,
									},
								},
							},
						},
					},
				},
			},
			{
				name: "build class with existing circular ref prop",
				localSchema: schema.Schema{
					Objects: &models.Schema{
						Classes: []*models.Class{
							{
								Class: "ThisClassExists",
								Properties: []*models.Property{
									{
										DataType: []string{"ThisClassAlsoExists"},
										Name:     "ofExistingClass",
									},
								},
							},
							{
								Class: "ThisClassAlsoExists",
								Properties: []*models.Property{
									{
										DataType: []string{"ThisClassExists"},
										Name:     "ofExistingClass",
									},
								},
							},
						},
					},
				},
			},
		}

		tests.AssertNoError(t)
	})
}

type testCase struct {
	name        string
	localSchema schema.Schema
}

type testCases []testCase

func (tests testCases) AssertNoError(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			modules := modules.NewProvider()
			localSchema, err := Build(&test.localSchema, nil, config.Config{}, modules)
			require.Nil(t, err, test.name)

			schemaObject := graphql.ObjectConfig{
				Name:        "WeaviateObj",
				Description: "Location of the root query",
				Fields:      localSchema,
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("%v at %s", r, debug.Stack())
					}
				}()

				_, err = graphql.NewSchema(graphql.SchemaConfig{
					Query: graphql.NewObject(schemaObject),
				})
			}()

			assert.Nil(t, err, test.name)
		})
	}
}

// AssertErrorLogs still expects the test to pass without errors,
// but does expect the Build logger to contain errors messages
// from the GQL schema rebuilding thunk
func (tests testCases) AssertErrorLogs(t *testing.T, expectedMsg string) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			modules := modules.NewProvider()
			logger, logsHook := logrus.NewNullLogger()
			localSchema, err := Build(&test.localSchema, logger, config.Config{}, modules)
			require.Nil(t, err, test.name)

			schemaObject := graphql.ObjectConfig{
				Name:        "WeaviateObj",
				Description: "Location of the root query",
				Fields:      localSchema,
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("%v at %s", r, debug.Stack())
					}
				}()

				_, err = graphql.NewSchema(graphql.SchemaConfig{
					Query: graphql.NewObject(schemaObject),
				})
			}()

			last := logsHook.LastEntry()
			assert.Contains(t, last.Message, expectedMsg)
			assert.Nil(t, err)
		})
	}
}

func validSchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "BestLocalThing",
					Properties: []*models.Property{
						{
							DataType:     schema.DataTypeText.PropString(),
							Name:         "myStringProp",
							Tokenization: models.PropertyTokenizationWhitespace,
						},
					},
				},
			},
		},
	}
}
