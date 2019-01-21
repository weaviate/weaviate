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
package aggregate

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

// Builds the classes below a Network -> Aggregate -> (k kind.Kind)
func BuildAggregateClasses(dbSchema *schema.Schema, k kind.Kind, semanticSchema *models.SemanticSchema, knownClasses *map[string]*graphql.Object, weaviate string) (*graphql.Object, error) {
	classFields := graphql.Fields{}

	var kindName string
	switch k {
	case kind.THING_KIND:
		kindName = "Thing"
	case kind.ACTION_KIND:
		kindName = "Action"
	}

	for _, class := range semanticSchema.Classes {
		classField, err := buildAggregateClass(dbSchema, k, class, knownClasses, kindName, weaviate)
		if err != nil {
			return nil, fmt.Errorf("Could not build class for %s", class.Class)
		}
		classFields[class.Class] = classField
	}

	classes := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateNetworkAggregate%s%ssObj", weaviate, kindName),
		Fields:      classFields,
		Description: fmt.Sprintf(descriptions.NetworkAggregateThingsActionsObjDesc, kindName),
	})

	return classes, nil
}

// Build a single class in Network -> Aggregate -> (k kind.Kind) -> (models.SemanticSchemaClass)
func buildAggregateClass(dbSchema *schema.Schema, k kind.Kind, class *models.SemanticSchemaClass, knownClasses *map[string]*graphql.Object, kindName string, weaviate string) (*graphql.Field, error) {

	if len(class.Properties) == 0 {
		// if we don't have class properties, we can't build this particular class,
		// as it would not have any fields. So we have to return (without an
		// error), so as not to block the creation of other classes
		return nil, nil
	}

	classObject := graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("Aggregate%s%s", weaviate, class.Class),
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {

			classProperties := graphql.Fields{}

			// only generate these fields if the class contains a numeric property
			if classContainsNumericProperties(dbSchema, class) == true {
				classProperties["sum"] = &graphql.Field{
					Description: descriptions.NetworkAggregateSumDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "sum", weaviate),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["mode"] = &graphql.Field{
					Description: descriptions.NetworkAggregateModeDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "mode", weaviate),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["mean"] = &graphql.Field{
					Description: descriptions.NetworkAggregateMeanDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "mean", weaviate),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["median"] = &graphql.Field{
					Description: descriptions.NetworkAggregateMedianDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "median", weaviate),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["minimum"] = &graphql.Field{
					Description: descriptions.NetworkAggregateMinDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "minimum", weaviate),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["maximum"] = &graphql.Field{
					Description: descriptions.NetworkAggregateMaxDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "maximum", weaviate),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
			}

			// always generate these fields
			classProperties["count"] = &graphql.Field{
				Description: descriptions.NetworkAggregateCountDesc,
				Type:        generateCountPropertyFields(dbSchema, class, "count", weaviate),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return nil, fmt.Errorf("not supported")
				},
			}
			classProperties["groupedBy"] = &graphql.Field{
				Description: descriptions.NetworkAggregateGroupedByDesc,
				Type:        generateGroupedByPropertyFields(dbSchema, class, "groupedBy", weaviate),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return nil, fmt.Errorf("not supported")
				},
			}

			return classProperties
		}),
		Description: class.Description,
	})

	(*knownClasses)[class.Class] = classObject

	classField := graphql.Field{
		Type:        graphql.NewList(classObject),
		Description: class.Description,
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description: descriptions.FirstDesc,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: descriptions.AfterDesc,
				Type:        graphql.Int,
			},
			"where": &graphql.ArgumentConfig{
				Description: descriptions.NetworkAggregateWhereDesc,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        fmt.Sprintf("WeaviateNetworkAggregate%s%ss%sWhereInpObj", weaviate, kindName, class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateNetworkAggregate%s%ss%s", weaviate, kindName, class.Class)),
						Description: descriptions.NetworkAggregateWhereInpObjDesc,
					},
				),
			},
			"groupBy": &graphql.ArgumentConfig{
				Description: descriptions.GroupByDesc,
				Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return &classField, nil
}

// classContainsNumericProperties determines whether a specified class contains one or more numeric properties.
func classContainsNumericProperties(dbSchema *schema.Schema, class *models.SemanticSchemaClass) bool {

	for _, property := range class.Properties {
		propertyType, err := dbSchema.FindPropertyDataType(property.AtDataType)

		if err != nil {
			// We can't return an error in this FieldsThunk function, so we need to panic
			panic(fmt.Sprintf("buildGetClass: wrong propertyType for %s.%s; %s", class.Class, property.Name, err.Error()))
		}

		if propertyType.IsPrimitive() {
			primitivePropertyType := propertyType.AsPrimitive()

			if primitivePropertyType == schema.DataTypeInt || primitivePropertyType == schema.DataTypeNumber {
				return true
			}
		}
	}
	return false
}

func generateNumericPropertyFields(dbSchema *schema.Schema, class *models.SemanticSchemaClass, method string, weaviate string) *graphql.Object {
	classProperties := graphql.Fields{}

	for _, property := range class.Properties {
		var propertyField *graphql.Field
		propertyType, err := dbSchema.FindPropertyDataType(property.AtDataType)

		if err != nil {
			// We can't return an error in this FieldsThunk function, so we need to panic
			panic(fmt.Sprintf("buildAggregateClass: wrong propertyType for %s.%s; %s", class.Class, property.Name, err.Error()))
		}

		if propertyType.IsPrimitive() {
			primitivePropertyType := propertyType.AsPrimitive()

			if primitivePropertyType == schema.DataTypeInt {
				propertyField = &graphql.Field{
					Description: property.Description,
					Type:        graphql.Int,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}

				propertyField.Name = property.Name
				classProperties[property.Name] = propertyField
			}
			if primitivePropertyType == schema.DataTypeNumber {
				propertyField = &graphql.Field{
					Description: property.Description,
					Type:        graphql.Float,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}

				propertyField.Name = property.Name
				classProperties[property.Name] = propertyField
			}
		}
	}

	classPropertiesObj := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Aggregate%s%s%sObj", weaviate, class.Class, strings.Title(method)),
		Fields:      classProperties,
		Description: fmt.Sprintf(descriptions.NetworkAggregateNumericObj, method),
	})

	return classPropertiesObj
}

func generateCountPropertyFields(dbSchema *schema.Schema, class *models.SemanticSchemaClass, method string, weaviate string) *graphql.Object {
	classProperties := graphql.Fields{}

	for _, property := range class.Properties {
		propertyField := &graphql.Field{
			Description: property.Description,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}

		propertyField.Name = property.Name
		classProperties[property.Name] = propertyField
	}

	classPropertiesObj := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Aggregate%s%s%sObj", weaviate, class.Class, strings.Title(method)),
		Fields:      classProperties,
		Description: descriptions.NetworkAggregateCountObj,
	})

	return classPropertiesObj
}

func generateGroupedByPropertyFields(dbSchema *schema.Schema, class *models.SemanticSchemaClass, method string, weaviate string) *graphql.Object {
	classProperties := graphql.Fields{

		"path": &graphql.Field{
			Description: descriptions.NetworkAggregateGroupedByGroupedByPathDesc,
			Type:        graphql.NewList(graphql.String),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
		"value": &graphql.Field{
			Description: descriptions.NetworkAggregateGroupedByGroupedByValueDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	classPropertiesObj := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Aggregate%s%s%sObj", weaviate, class.Class, strings.Title(method)),
		Fields:      classProperties,
		Description: descriptions.NetworkAggregateGroupedByObjDesc,
	})

	return classPropertiesObj
}
