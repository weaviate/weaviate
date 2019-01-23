/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package get

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
	"github.com/stretchr/testify/assert"
)

func TestExtractEmptySchema(t *testing.T) {
	schema := &schema.Schema{
		Actions: nil,
		Things:  nil,
	}

	result := extractNetworkRefClassNames(schema)
	assert.Equal(t, []crossrefs.NetworkClass{}, result, "should be an empty list")
}

func TestExtractSchemaWithPrimitiveActions(t *testing.T) {
	schema := &schema.Schema{
		Actions: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "BestAction",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"string"},
							Name:       "bestStringProp",
						},
					},
				}},
		},
		Things: nil,
	}

	result := extractNetworkRefClassNames(schema)
	assert.Equal(t, []crossrefs.NetworkClass{}, result, "should be an empty list")
}

func TestExtractSchemaWithPrimitiveThings(t *testing.T) {
	schema := &schema.Schema{
		Things: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "BestThing",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"string"},
							Name:       "bestStringProp",
						},
					},
				}},
		},
		Actions: nil,
	}

	result := extractNetworkRefClassNames(schema)
	assert.Equal(t, []crossrefs.NetworkClass{}, result, "should be an empty list")
}

func TestExtractSchemaWithThingsWithLocalRefs(t *testing.T) {
	schema := &schema.Schema{
		Things: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "BestThing",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"AnotherFairlyGoodThing"},
							Name:       "BestReference",
						},
					},
				}},
		},
		Actions: nil,
	}

	result := extractNetworkRefClassNames(schema)
	assert.Equal(t, []crossrefs.NetworkClass{}, result, "should be an empty list")
}

func TestExtractSchemaWithThingsWithNetworkRefs(t *testing.T) {
	schema := &schema.Schema{
		Things: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "BestThing",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheBestThing"},
							Name:       "BestReference",
						},
					},
				},
				&models.SemanticSchemaClass{
					Class: "WorstThing",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheWorstThing"},
							Name:       "WorstReference",
						},
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheMediocreThing"},
							Name:       "MediocreReference",
						},
					},
				},
			},
		},
		Actions: nil,
	}

	result := extractNetworkRefClassNames(schema)
	assert.Equal(t, []crossrefs.NetworkClass{
		{PeerName: "OtherInstance", ClassName: "TheBestThing"},
		{PeerName: "OtherInstance", ClassName: "TheWorstThing"},
		{PeerName: "OtherInstance", ClassName: "TheMediocreThing"},
	}, result, "should find the network classes")
}

func TestExtractSchemaWithActionsWithNetworkRefs(t *testing.T) {
	schema := &schema.Schema{
		Actions: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "BestAction",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheBestThing"},
							Name:       "BestReference",
						},
					},
				},
				&models.SemanticSchemaClass{
					Class: "WorstThing",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheWorstThing"},
							Name:       "WorstReference",
						},
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheMediocreThing"},
							Name:       "MediocreReference",
						},
					},
				},
			},
		},
		Things: nil,
	}

	result := extractNetworkRefClassNames(schema)
	assert.Equal(t, []crossrefs.NetworkClass{
		{PeerName: "OtherInstance", ClassName: "TheBestThing"},
		{PeerName: "OtherInstance", ClassName: "TheWorstThing"},
		{PeerName: "OtherInstance", ClassName: "TheMediocreThing"},
	}, result, "should find the network classes")
}

func TestExtractSchemaWithDuplicates(t *testing.T) {
	schema := &schema.Schema{
		Actions: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "BestAction",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheBestThing"},
							Name:       "BestReference",
						},
					},
				},
				&models.SemanticSchemaClass{
					Class: "WorstThing",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheBestThing"},
							Name:       "WorstReference",
						},
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheBestThing"},
							Name:       "MediocreReference",
						},
					},
				},
			},
		},
		Things: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "BestThing",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheBestThing"},
							Name:       "BestReference",
						},
					},
				},
				&models.SemanticSchemaClass{
					Class: "WorstThing",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheBestThing"},
							Name:       "WorstReference",
						},
						&models.SemanticSchemaClassProperty{
							AtDataType: []string{"OtherInstance/TheBestThing"},
							Name:       "MediocreReference",
						},
					},
				},
			},
		},
	}

	result := extractNetworkRefClassNames(schema)
	assert.Equal(t,
		[]crossrefs.NetworkClass{{PeerName: "OtherInstance", ClassName: "TheBestThing"}},
		result, "should remove duplicates")
}
