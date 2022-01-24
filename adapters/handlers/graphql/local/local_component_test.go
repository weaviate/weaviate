//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package local

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests are component tests for the local package including all its
// subpackages, such as get, getmeta, etc.. However, they only assert that the
// graphql tree can be built under certain circumstances. This helps us to
// catch errors on edge cases like empty schemas, classes with empty
// properties, empty peer lists, peers with empty schemas, etc. However, we
// don't get any guaruantuee of whether the individual queries resolve
// correctly. For those cases we have unit tests in die individual subpackages
// (i.e. get, getmeta, aggreagate, etc.).  Additionally we have (a few) e2e
// tests.

func Test_GraphQLNetworkBuild(t *testing.T) {
	tests := testCases{

		// This tests asserts that an action-only schema doesn't lead to errors.
		testCase{
			name: "with only objects locally",
			localSchema: schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{
						&models.Class{
							Class: "BestLocalAction",
							Properties: []*models.Property{
								&models.Property{
									DataType: []string{"string"},
									Name:     "myStringProp",
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
						&models.Class{
							Class: "BestLocalThing",
							Properties: []*models.Property{
								&models.Property{
									DataType: []string{"string"},
									Name:     "myStringProp",
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
						&models.Class{
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

func validSchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "BestLocalThing",
					Properties: []*models.Property{
						&models.Property{
							DataType: []string{"string"},
							Name:     "myStringProp",
						},
					},
				},
			},
		},
	}
}
