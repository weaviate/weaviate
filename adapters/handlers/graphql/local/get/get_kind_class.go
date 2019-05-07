/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package get

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get/refclasses"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/telemetry"

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
				propertyType, err := dbSchema.FindPropertyDataType(property.DataType)
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
	case schema.DataTypeGeoCoordinates:
		obj := newGeoCoordinatesObject(className, property.Name)

		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        obj,
			Resolve:     resolveGeoCoordinates,
		}
	default:
		panic(fmt.Sprintf("buildGetClass: unknown primitive type for %s.%s.%s; %s",
			kindName, className, property.Name, propertyType.AsPrimitive()))
	}
}

func newGeoCoordinatesObject(className string, propertyName string) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Description: "GeoCoordinates as latitude and longitude in decimal form",
		Name:        fmt.Sprintf("%s%sGeoCoordinatesObj", className, propertyName),
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

func resolveGeoCoordinates(p graphql.ResolveParams) (interface{}, error) {
	field := p.Source.(map[string]interface{})[p.Info.FieldName]
	if field == nil {
		return nil, nil
	}

	geo, ok := field.(*models.GeoCoordinates)
	if !ok {
		return nil, fmt.Errorf("expected a *models.GeoCoordinates, but got: %T", field)
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
			"limit": &graphql.ArgumentConfig{
				Description: descriptions.First,
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
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected graphql root to be a map, but was %T", p.Source)
		}

		resolver, ok := source["Resolver"].(Resolver)
		if !ok {
			return nil, fmt.Errorf("expected source map to have a usable Resolver, but got %#v", source["Resolver"])
		}

		pagination, err := filters.ExtractPaginationFromArgs(p.Args)
		if err != nil {
			return nil, err
		}

		// There can only be exactly one ast.Field; it is the class name.
		if len(p.Info.FieldASTs) != 1 {
			panic("Only one Field expected here")
		}

		selectionsOfClass := p.Info.FieldASTs[0].SelectionSet
		properties, err := extractProperties(selectionsOfClass, p.Info.Fragments)
		if err != nil {
			return nil, err
		}

		filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
		if err != nil {
			return nil, fmt.Errorf("could not extract filters: %s", err)
		}

		params := kinds.LocalGetParams{
			Filters:    filters,
			Kind:       k,
			ClassName:  className,
			Pagination: pagination,
			Properties: properties,
		}

		// Log the request
		requestsLog, logOk := source["RequestsLog"].(RequestsLog)
		if !logOk {
			return nil, fmt.Errorf("expected source map to have a usable RequestsLog, but got %#v", source["RequestsLog"])
		}
		go func() {
			requestsLog.Register(telemetry.TypeGQL, telemetry.LocalQuery)
		}()

		return func() (interface{}, error) {
			return resolver.LocalGetClass(p.Context, &params)
		}, nil
	}
}

func isPrimitive(selectionSet *ast.SelectionSet) bool {
	if selectionSet == nil {
		return true
	}

	// if there is a selection set it could either be a cross-ref or a map-type
	// field like GeoCoordinates

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

func extractProperties(selections *ast.SelectionSet, fragments map[string]ast.Definition) ([]kinds.SelectProperty, error) {
	var properties []kinds.SelectProperty

	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		property := kinds.SelectProperty{Name: name}

		property.IsPrimitive = isPrimitive(field.SelectionSet)
		if !property.IsPrimitive {
			// We can interpret this property in different ways
			for _, subSelection := range field.SelectionSet.Selections {

				switch s := subSelection.(type) {
				case *ast.Field:
					// Is it a field with the name __typename?
					if s.Name.Value == "__typename" {
						property.IncludeTypeName = true
						continue
					} else {
						return nil, fmt.Errorf("Expected a InlineFragment, not a '%s' field ", s.Name.Value)
					}

				case *ast.FragmentSpread:
					ref, err := extractFragmentSpread(s, fragments)
					if err != nil {
						return nil, err
					}

					property.Refs = append(property.Refs, ref)

				case *ast.InlineFragment:
					ref, err := extractInlineFragment(s, fragments)
					if err != nil {
						return nil, err
					}

					property.Refs = append(property.Refs, ref)

				default:
					return nil, fmt.Errorf("unrecoginzed type in subs-selection: %T", subSelection)
				}
			}
		}

		properties = append(properties, property)
	}

	return properties, nil
}

func extractInlineFragment(fragment *ast.InlineFragment, fragments map[string]ast.Definition) (kinds.SelectClass, error) {
	var className schema.ClassName
	var err error
	var result kinds.SelectClass

	if strings.Contains(fragment.TypeCondition.Name.Value, "__") {
		// is a helper type for a network ref
		// don't validate anything as of now
		className = schema.ClassName(fragment.TypeCondition.Name.Value)
	} else {
		className, err = schema.ValidateClassName(fragment.TypeCondition.Name.Value)
		if err != nil {
			return result, fmt.Errorf("the inline fragment type name '%s' is not a valid class name", fragment.TypeCondition.Name.Value)
		}
	}

	subProperties, err := extractProperties(fragment.SelectionSet, fragments)
	if err != nil {
		return result, err
	}

	result.ClassName = string(className)
	result.RefProperties = subProperties
	return result, nil
}

func extractFragmentSpread(spread *ast.FragmentSpread, fragments map[string]ast.Definition) (kinds.SelectClass, error) {
	var result kinds.SelectClass
	name := spread.Name.Value

	def, ok := fragments[name]
	if !ok {
		return result, fmt.Errorf("spread fragment '%s' refers to unknown fragment", name)
	}

	className, err := hackyWorkaroundToExtractClassName(def, name)
	if err != nil {
		return result, err
	}

	subProperties, err := extractProperties(def.GetSelectionSet(), fragments)
	if err != nil {
		return result, err
	}

	result.ClassName = string(className)
	result.RefProperties = subProperties
	return result, nil
}

// It seems there's no proper way to extract this info unfortunately:
// https://github.com/graphql-go/graphql/issues/455
func hackyWorkaroundToExtractClassName(def ast.Definition, name string) (string, error) {
	loc := def.GetLoc()
	raw := loc.Source.Body[loc.Start:loc.End]
	r := regexp.MustCompile(fmt.Sprintf(`fragment\s*%s\s*on\s*(\w*)\s*{`, name))
	matches := r.FindSubmatch(raw)
	if len(matches) < 2 {
		return "", fmt.Errorf("could not extract a className from fragment")
	}

	return string(matches[1]), nil
}
