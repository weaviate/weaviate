/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package get

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/common"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get/refclasses"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// Build a single class in Local -> Get -> (k kind.Kind) -> (models.SemanticSchemaClass)
func buildGetClass(dbSchema *schema.Schema, k kind.Kind, class *models.SemanticSchemaClass,
	knownClasses *map[string]*graphql.Object, knownRefClasses refclasses.ByNetworkClass,
	peers peers.Peers) (*graphql.Field, error) {
	classObject := buildGetClassObject(k.Name(), class, dbSchema, knownClasses, knownRefClasses, peers)
	(*knownClasses)[class.Class] = classObject
	classField := buildGetClassField(classObject, k, class)
	return &classField, nil
}

func buildGetClassObject(kindName string, class *models.SemanticSchemaClass, dbSchema *schema.Schema,
	knownClasses *map[string]*graphql.Object, knownRefClasses refclasses.ByNetworkClass,
	peers peers.Peers) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			classProperties := graphql.Fields{}

			classProperties["uuid"] = &graphql.Field{
				Description: descriptions.LocalGetClassUUID,
				Type:        graphql.String,
			}

			for _, property := range class.Properties {
				propertyType, err := dbSchema.FindPropertyDataType(property.AtDataType)
				if err != nil {
					// We can't return an error in this FieldsThunk function, so we need to panic
					panic(fmt.Sprintf("buildGetClass: wrong propertyType for %s.%s.%s; %s",
						kindName, class.Class, property.Name, err.Error()))
				}

				var field *graphql.Field
				var fieldName string
				if propertyType.IsPrimitive() {
					fieldName = property.Name
					field = buildPrimitiveField(propertyType, property, kindName, class.Class)
				} else {
					// uppercase key because it's a reference
					fieldName = strings.Title(property.Name)
					field = buildReferenceField(propertyType, property, kindName, class.Class, knownClasses, knownRefClasses, peers)
				}
				classProperties[fieldName] = field
			}

			return classProperties
		}),
		Description: class.Description,
	})
}

func buildPrimitiveField(propertyType schema.PropertyDataType,
	property *models.SemanticSchemaClassProperty, kindName, className string) *graphql.Field {
	switch propertyType.AsPrimitive() {
	case schema.DataTypeString:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return p.Source.(map[string]interface{})[p.Info.FieldName], nil

			},
		}
	case schema.DataTypeText:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.String,
		}
	case schema.DataTypeInt:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.Int,
		}
	case schema.DataTypeNumber:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.Float,
		}
	case schema.DataTypeBoolean:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.Boolean,
		}
	case schema.DataTypeDate:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.String, // String since no graphql date datatype exists
		}
	case schema.DataTypeGeoCoordinate:
		obj := newGeoCoordinateObject(className, property.Name)

		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        obj,
			Resolve:     resolveGeoCoordinate,
		}
	default:
		panic(fmt.Sprintf("buildGetClass: unknown primitive type for %s.%s.%s; %s",
			kindName, className, property.Name, propertyType.AsPrimitive()))
	}
}

func newGeoCoordinateObject(className string, propertyName string) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Description: "GeoCoordinate as latitude and longitude in decimal form",
		Name:        fmt.Sprintf("%s%sGeoCoordinateObj", className, propertyName),
		Fields: graphql.Fields{
			"latitude": &graphql.Field{
				Name:        "Latitude",
				Description: "The Latitude of the point in decimal form.",
				Type:        graphql.Float,
			},
			"longitude": &graphql.Field{
				Name:        "Longitude",
				Description: "The Longitude of the point in decimal form.",
				Type:        graphql.Float,
			},
		},
	})
}

func resolveGeoCoordinate(p graphql.ResolveParams) (interface{}, error) {
	field := p.Source.(map[string]interface{})[p.Info.FieldName]
	if field == nil {
		return nil, nil
	}

	geo, ok := field.(*models.GeoCoordinate)
	if !ok {
		return nil, fmt.Errorf("expected a *models.GeoCoordinate, but got: %T", field)
	}

	return map[string]interface{}{
		"latitude":  geo.Latitude,
		"longitude": geo.Longitude,
	}, nil
}

func buildGetClassField(classObject *graphql.Object, k kind.Kind,
	class *models.SemanticSchemaClass) graphql.Field {
	kindName := strings.Title(k.Name())
	return graphql.Field{
		Type:        graphql.NewList(classObject),
		Description: class.Description,
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description: descriptions.First,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: descriptions.After,
				Type:        graphql.Int,
			},
			"where": &graphql.ArgumentConfig{
				Description: descriptions.LocalGetWhere,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        fmt.Sprintf("WeaviateLocalGet%ss%sWhereInpObj", kindName, class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateLocalGet%ss%s", kindName, class.Class)),
						Description: descriptions.LocalGetWhereInpObj,
					},
				),
			},
		},
		Resolve: makeResolveGetClass(k, class.Class),
	}
}

func makeResolveGetClass(k kind.Kind, className string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		filtersAndResolver := p.Source.(*filtersAndResolver)

		pagination, err := common.ExtractPaginationFromArgs(p.Args)
		if err != nil {
			return nil, err
		}

		// There can only be exactly one ast.Field; it is the class name.
		if len(p.Info.FieldASTs) != 1 {
			panic("Only one Field expected here")
		}

		selectionsOfClass := p.Info.FieldASTs[0].SelectionSet
		properties, err := extractProperties(selectionsOfClass)
		if err != nil {
			return nil, err
		}

		filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
		if err != nil {
			return nil, fmt.Errorf("could not extract filters: %s", err)
		}

		params := Params{
			Filters:    filters,
			Kind:       k,
			ClassName:  className,
			Pagination: pagination,
			Properties: properties,
		}

		return func() (interface{}, error) {
			return filtersAndResolver.resolver.LocalGetClass(&params)
		}, nil
	}
}

func isPrimitive(selectionSet *ast.SelectionSet) bool {
	if selectionSet == nil {
		return true
	}

	// if there is a selection set it could either be a cross-ref or a map-type
	// field like GeoCoordinate

	for _, subSelection := range selectionSet.Selections {
		if subsectionField, ok := subSelection.(*ast.Field); ok {
			if subsectionField.Name.Value == "latitude" || subsectionField.Name.Value == "longitude" {
				return true
			}
		}
	}

	// must be a ref field
	return false
}

func extractProperties(selections *ast.SelectionSet) ([]SelectProperty, error) {
	var properties []SelectProperty

	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		property := SelectProperty{Name: name}

		property.IsPrimitive = isPrimitive(field.SelectionSet)
		if !property.IsPrimitive {
			// We can interpret this property in different ways
			for _, subSelection := range field.SelectionSet.Selections {
				// Is it a field with the name __typename?
				subsectionField, ok := subSelection.(*ast.Field)
				if ok {
					if subsectionField.Name.Value == "__typename" {
						property.IncludeTypeName = true
						continue
					} else {
						return nil, fmt.Errorf("Expected a InlineFragment, not a '%s' field ", subsectionField.Name.Value)
					}
				}

				// Otherwise these _must_ be inline fragments
				fragment, ok := subSelection.(*ast.InlineFragment)
				if !ok {
					return nil, fmt.Errorf("Expected a InlineFragment; you need to specify as which type you want to retrieve a reference %#v", subSelection)
				}

				var className schema.ClassName
				var err error
				if strings.Contains(fragment.TypeCondition.Name.Value, "__") {
					// is a helper type for a network ref
					// don't validate anything as of now
					className = schema.ClassName(fragment.TypeCondition.Name.Value)
				} else {
					err, className = schema.ValidateClassName(fragment.TypeCondition.Name.Value)
					if err != nil {
						return nil, fmt.Errorf("the inline fragment type name '%s' is not a valid class name", fragment.TypeCondition.Name.Value)
					}
				}

				subProperties, err := extractProperties(fragment.SelectionSet)
				if err != nil {
					return nil, err
				}

				property.Refs = append(property.Refs, SelectClass{
					ClassName:     string(className),
					RefProperties: subProperties,
				})
			}
		}

		properties = append(properties, property)
	}

	return properties, nil
}
