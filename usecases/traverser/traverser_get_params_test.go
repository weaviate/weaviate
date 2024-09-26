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

package traverser

import (
	"testing"

	logrus "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestGetParams(t *testing.T) {
	t.Run("without any select properties", func(t *testing.T) {
		sp := search.SelectProperties{}
		assert.Equal(t, false, sp.HasRefs(), "indicates no refs are present")
	})

	t.Run("with only primitive select properties", func(t *testing.T) {
		sp := search.SelectProperties{
			search.SelectProperty{
				IsPrimitive: true,
				Name:        "Foo",
			},
			search.SelectProperty{
				IsPrimitive: true,
				Name:        "Bar",
			},
		}

		assert.Equal(t, false, sp.HasRefs(), "indicates no refs are present")

		resolve, err := sp.ShouldResolve([]string{"inCountry", "Country"})
		require.Nil(t, err)
		assert.Equal(t, false, resolve)
	})

	t.Run("with a ref prop", func(t *testing.T) {
		sp := search.SelectProperties{
			search.SelectProperty{
				IsPrimitive: true,
				Name:        "name",
			},
			search.SelectProperty{
				IsPrimitive: false,
				Name:        "inCity",
				Refs: []search.SelectClass{
					{
						ClassName: "City",
						RefProperties: search.SelectProperties{
							search.SelectProperty{
								Name:        "name",
								IsPrimitive: true,
							},
							search.SelectProperty{
								Name:        "inCountry",
								IsPrimitive: false,
								Refs: []search.SelectClass{
									{
										ClassName: "Country",
										RefProperties: search.SelectProperties{
											search.SelectProperty{
												Name:        "name",
												IsPrimitive: true,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		t.Run("checking for refs", func(t *testing.T) {
			assert.Equal(t, true, sp.HasRefs(), "indicates refs are present")
		})

		t.Run("checking valid single level ref", func(t *testing.T) {
			resolve, err := sp.ShouldResolve([]string{"inCity", "City"})
			require.Nil(t, err)
			assert.Equal(t, true, resolve)
		})

		t.Run("checking invalid single level ref", func(t *testing.T) {
			resolve, err := sp.ShouldResolve([]string{"inCity", "Town"})
			require.Nil(t, err)
			assert.Equal(t, false, resolve)
		})

		t.Run("checking valid nested ref", func(t *testing.T) {
			resolve, err := sp.ShouldResolve([]string{"inCity", "City", "inCountry", "Country"})
			require.Nil(t, err)
			assert.Equal(t, true, resolve)
		})

		t.Run("checking invalid nested level refs", func(t *testing.T) {
			resolve, err := sp.ShouldResolve([]string{"inCity", "Town", "inCountry", "Country"})
			require.Nil(t, err)
			assert.Equal(t, false, resolve)

			resolve, err = sp.ShouldResolve([]string{"inCity", "City", "inCountry", "Land"})
			require.Nil(t, err)
			assert.Equal(t, false, resolve)
		})

		t.Run("selecting a specific prop", func(t *testing.T) {
			prop := sp.FindProperty("inCity")
			assert.Equal(t, prop, &sp[1])
		})
	})
}

func TestGet_NestedRefDepthLimit(t *testing.T) {
	type testcase struct {
		name        string
		props       search.SelectProperties
		maxDepth    int
		expectedErr string
	}

	makeNestedRefProps := func(depth int) search.SelectProperties {
		root := search.SelectProperties{}
		next := &root
		for i := 0; i < depth; i++ {
			*next = append(*next,
				search.SelectProperty{Name: "nextNode"},
				search.SelectProperty{Name: "otherRef"},
			)
			class0 := search.SelectClass{ClassName: "LinkedListNode"}
			refs0 := []search.SelectClass{class0}
			(*next)[0].Refs = refs0
			class1 := search.SelectClass{ClassName: "LinkedListNode"}
			refs1 := []search.SelectClass{class1}
			(*next)[1].Refs = refs1
			next = &refs0[0].RefProperties
		}
		return root
	}

	newTraverser := func(depth int) *Traverser {
		logger, _ := logrus.NewNullLogger()
		schemaGetter := &fakeSchemaGetter{aggregateTestSchema}
		cfg := config.WeaviateConfig{
			Config: config.Config{
				QueryCrossReferenceDepthLimit: depth,
			},
		}
		return NewTraverser(&cfg, &fakeLocks{}, logger, &fakeAuthorizer{},
			&fakeVectorRepo{}, &fakeExplorer{}, schemaGetter, nil, nil, -1)
	}

	tests := []testcase{
		{
			name:     "succeed with explicitly set low depth limit",
			maxDepth: 5,
			props:    makeNestedRefProps(5),
		},
		{
			name:        "fail with explicitly set low depth limit",
			maxDepth:    5,
			props:       makeNestedRefProps(6),
			expectedErr: "nested references depth exceeds QUERY_CROSS_REFERENCE_DEPTH_LIMIT (5)",
		},
		{
			name:     "succeed with explicitly set high depth limit",
			maxDepth: 500,
			props:    makeNestedRefProps(500),
		},
		{
			name:        "fail with explicitly set high depth limit",
			maxDepth:    500,
			props:       makeNestedRefProps(501),
			expectedErr: "nested references depth exceeds QUERY_CROSS_REFERENCE_DEPTH_LIMIT (500)",
		},
		{
			name:        "fail with explicitly set low depth limit, but high actual depth",
			maxDepth:    10,
			props:       makeNestedRefProps(5000),
			expectedErr: "nested references depth exceeds QUERY_CROSS_REFERENCE_DEPTH_LIMIT (10)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.maxDepth == 0 {
				t.Fatalf("must provide maxDepth param for test %q", test.name)
			}
			traverser := newTraverser(test.maxDepth)
			err := traverser.probeForRefDepthLimit(test.props)
			if test.expectedErr != "" {
				assert.EqualError(t, err, test.expectedErr)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
