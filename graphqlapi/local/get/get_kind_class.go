package local_get

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	common "github.com/creativesoftwarefdn/weaviate/graphqlapi/common_resolver"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get/refclasses"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// Build a single class in Local -> Get -> (k kind.Kind) -> (models.SemanticSchemaClass)
func buildGetClass(dbSchema *schema.Schema, k kind.Kind, class *models.SemanticSchemaClass,
	knownClasses *map[string]*graphql.Object, knownRefClasses refclasses.ByNetworkClass) (*graphql.Field, error) {
	classObject := buildGetClassObject(k.Name(), class, dbSchema, knownClasses, knownRefClasses)
	(*knownClasses)[class.Class] = classObject
	classField := buildGetClassField(classObject, k, class)
	return &classField, nil
}

func buildGetClassObject(kindName string, class *models.SemanticSchemaClass, dbSchema *schema.Schema,
	knownClasses *map[string]*graphql.Object, knownRefClasses refclasses.ByNetworkClass) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			classProperties := graphql.Fields{}

			classProperties["uuid"] = &graphql.Field{
				Description: descriptions.LocalGetClassUUIDDesc,
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
					field = buildReferenceField(propertyType, property, kindName, class.Class, knownClasses, knownRefClasses)
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
	default:
		panic(fmt.Sprintf("buildGetClass: unknown primitive type for %s.%s.%s; %s",
			kindName, className, property.Name, propertyType.AsPrimitive()))
	}
}

func buildReferenceField(propertyType schema.PropertyDataType,
	property *models.SemanticSchemaClassProperty, kindName, className string,
	knownClasses *map[string]*graphql.Object, knownRefClasses refclasses.ByNetworkClass) *graphql.Field {
	refClasses := propertyType.Classes()
	propertyName := strings.Title(property.Name)
	dataTypeClasses := []*graphql.Object{}

	for _, refClassName := range refClasses {
		if desiredRefClass, err := crossrefs.ParseClass(string(refClassName)); err == nil {
			// is a network ref
			refClass, ok := knownRefClasses[desiredRefClass]
			if !ok {
				// we seem to have referenced a network class that doesn't exist
				// (anymore). This is unfortunate, but there are many good reasons for
				// this to happen. For example a peer could have left the network. We
				// therefore simply skip this refprop, so we don't destroy the entire
				// graphql api every time a peer leaves unexpectedly
				continue
			}

			dataTypeClasses = append(dataTypeClasses, refClass)
		} else {
			// is a local ref
			refClass, ok := (*knownClasses)[string(refClassName)]
			if !ok {
				panic(fmt.Sprintf("buildGetClass: unknown referenced class type for %s.%s.%s; %s",
					kindName, className, property.Name, refClassName))
			}

			dataTypeClasses = append(dataTypeClasses, refClass)
		}
	}

	if (len(dataTypeClasses)) == 0 {
		// this could be the case when we only have network-refs, but all network
		// refs were invalid (e.g. because the peers are gone). In this case we
		// must return (nil) early, otherwise graphql will error because it has a
		// union field with an empty list of unions.
		return nil
	}

	classUnion := graphql.NewUnion(graphql.UnionConfig{
		Name:  fmt.Sprintf("%s%s%s", className, propertyName, "Obj"),
		Types: dataTypeClasses,
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			// TODO: inspect type of result.
			return (*knownClasses)["City"]
		},
		Description: property.Description,
	})

	// TODO: Check cardinality

	return &graphql.Field{
		Type:        classUnion,
		Description: property.Description,
	}
}

func buildGetClassField(classObject *graphql.Object, k kind.Kind,
	class *models.SemanticSchemaClass) graphql.Field {
	kindName := strings.Title(k.Name())
	return graphql.Field{
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
				Description: descriptions.LocalGetWhereDesc,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        fmt.Sprintf("WeaviateLocalGet%ss%sWhereInpObj", kindName, class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateLocalGet%ss%s", kindName, class.Class)),
						Description: descriptions.LocalGetWhereInpObjDesc,
					},
				),
			},
		},
		Resolve: makeResolveGetClass(k, class.Class),
	}
}

func makeResolveGetClass(k kind.Kind, className string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		fmt.Printf("- thing class (supposed to extract pagination, now return nil)\n")
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

		// fmt.Print("\n\n\n\n\n")
		// spew.Dump(properties)
		// fmt.Print("\n\n\n\n\n")
		filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
		if err != nil {
			return nil, fmt.Errorf("could not extract filters: %s", err)
		}

		params := LocalGetClassParams{
			Filters:    filters,
			Kind:       k,
			ClassName:  className,
			Pagination: pagination,
			Properties: properties,
		}

		promise, err := filtersAndResolver.resolver.LocalGetClass(&params)
		return promise, err
	}
}

func extractProperties(selections *ast.SelectionSet) ([]SelectProperty, error) {
	// debugFieldAsts(fieldASTs)
	var properties []SelectProperty

	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		property := SelectProperty{Name: name}

		property.IsPrimitive = (field.SelectionSet == nil)

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
