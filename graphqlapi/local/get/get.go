package local_get

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	common "github.com/creativesoftwarefdn/weaviate/graphqlapi/common_resolver"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
	graphql_ast "github.com/graphql-go/graphql/language/ast"
	"strings"
)

func Build(dbSchema *schema.Schema) (*graphql.Field, error) {
	getKinds := graphql.Fields{}

	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("There are no Actions or Things classes defined yet.")
	}

	knownClasses := map[string]*graphql.Object{}

	if len(dbSchema.Actions.Classes) > 0 {
		localGetActions, err := buildGetClasses(dbSchema, kind.ACTION_KIND, dbSchema.Actions, &knownClasses)
		if err != nil {
			return nil, err
		}

		getKinds["Actions"] = &graphql.Field{
			Name:        "WeaviateLocalGetActions",
			Description: descriptions.LocalGetActionsDesc,
			Type:        localGetActions,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				fmt.Printf("- LocalGetActions (pass on Source)\n")
				// Does nothing; pass through the filters
				return p.Source, nil
			},
		}
	}

	if len(dbSchema.Things.Classes) > 0 {
		localGetThings, err := buildGetClasses(dbSchema, kind.THING_KIND, dbSchema.Things, &knownClasses)
		if err != nil {
			return nil, err
		}

		getKinds["Things"] = &graphql.Field{
			Name:        "WeaviateLocalGetThings",
			Description: descriptions.LocalGetThingsDesc,
			Type:        localGetThings,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				fmt.Printf("- LocalGetThings (pass on Source)\n")
				// Does nothing; pass through the filters
				return p.Source, nil
			},
		}
	}

	field := graphql.Field{
		Name:        "WeaviateLocalGet",
		Description: descriptions.LocalGetDesc,
		Args: graphql.FieldConfigArgument{
			"where": &graphql.ArgumentConfig{
				Description: descriptions.LocalGetWhereDesc,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        "WeaviateLocalGetWhereInpObj",
						Fields:      common_filters.BuildNew("WeaviateLocalGet"),
						Description: descriptions.LocalGetWhereInpObjDesc,
					},
				),
			},
		},
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:        "WeaviateLocalGetObj",
			Fields:      getKinds,
			Description: descriptions.LocalGetObjDesc,
		}),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			fmt.Printf("- LocalGet (extract resolver from source, parse filters )\n")
			resolver := p.Source.(map[string]interface{})["Resolver"].(Resolver)
			filters, err := common_filters.ExtractFilters(p.Args)

			if err != nil {
				return nil, err
			}

			return &filtersAndResolver{
				filters:  filters,
				resolver: resolver,
			}, nil
		},
	}

	return &field, nil
}

// Builds the classes below a Local -> Get -> (k kind.Kind)
func buildGetClasses(dbSchema *schema.Schema, k kind.Kind, semanticSchema *models.SemanticSchema, knownClasses *map[string]*graphql.Object) (*graphql.Object, error) {
	classFields := graphql.Fields{}

	var kindName string
	switch k {
	case kind.THING_KIND:
		kindName = "Thing"
	case kind.ACTION_KIND:
		kindName = "Action"
	}

	for _, class := range semanticSchema.Classes {
		classField, err := buildGetClass(dbSchema, k, class, knownClasses)
		if err != nil {
			return nil, fmt.Errorf("Could not build class for %s", class.Class)
		}
		classFields[class.Class] = classField
	}

	classes := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateLocalGet%ssObj", kindName),
		Fields:      classFields,
		Description: fmt.Sprintf(descriptions.LocalGetThingsActionsObjDesc, kindName),
	})

	return classes, nil
}

// Build a single class in Local -> Get -> (k kind.Kind) -> (models.SemanticSchemaClass)
func buildGetClass(dbSchema *schema.Schema, k kind.Kind, class *models.SemanticSchemaClass, knownClasses *map[string]*graphql.Object) (*graphql.Field, error) {
	classObject := graphql.NewObject(graphql.ObjectConfig{
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
					panic(fmt.Sprintf("buildGetClass: wrong propertyType for %s.%s.%s; %s", k.Name(), class.Class, property.Name, err.Error()))
				}

				var propertyField *graphql.Field

				if propertyType.IsPrimitive() {
					switch propertyType.AsPrimitive() {

					case schema.DataTypeString:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.String,
						}
					case schema.DataTypeText:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.String,
						}
					case schema.DataTypeInt:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.Int,
						}
					case schema.DataTypeNumber:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.Float,
						}
					case schema.DataTypeBoolean:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.Boolean,
						}
					case schema.DataTypeDate:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.String, // String since no graphql date datatype exists
						}
					default:
						panic(fmt.Sprintf("buildGetClass: unknown primitive type for %s.%s.%s; %s", k.Name(), class.Class, property.Name, propertyType.AsPrimitive()))
					}

					propertyField.Name = property.Name
					classProperties[property.Name] = propertyField
				} else {
					// This is a reference
					refClasses := propertyType.Classes()
					propertyName := strings.Title(property.Name)
					dataTypeClasses := make([]*graphql.Object, len(refClasses))

					for index, refClassName := range refClasses {
						refClass, ok := (*knownClasses)[string(refClassName)]

						if !ok {
							panic(fmt.Sprintf("buildGetClass: unknown referenced class type for %s.%s.%s; %s", k.Name(), class.Class, property.Name, refClassName))
						}

						dataTypeClasses[index] = refClass
					}

					classUnion := graphql.NewUnion(graphql.UnionConfig{
						Name:  fmt.Sprintf("%s%s%s", class.Class, propertyName, "Obj"),
						Types: dataTypeClasses,
						ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
							// TODO: inspect type of result.
							return (*knownClasses)["City"]
							fmt.Printf("Resolver: WHOOPTYDOO\n")
							return nil
						},
						Description: property.Description,
					})

					// TODO: Check cardinality

					classProperties[propertyName] = &graphql.Field{
						Type:        classUnion,
						Description: property.Description,
					}
				}
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
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			fmt.Printf("- thing class (supposed to extract pagination, now return nil)\n")
			filtersAndResolver := p.Source.(*filtersAndResolver)

			pagination, err := common.ExtractPaginationFromArgs(p.Args)
			if err != nil {
				return nil, err
			}

			// There can only be exactly one graphql_ast.Field; it is the class name.
			if len(p.Info.FieldASTs) != 1 {
				panic("Only one Field expected here")
			}

			selectionsOfClass := p.Info.FieldASTs[0].SelectionSet
			properties, err := extractProperties(selectionsOfClass)
			if err != nil {
				return nil, err
			}

			params := LocalGetClassParams{
				Filters:    filtersAndResolver.filters,
				Kind:       k,
				ClassName:  class.Class,
				Pagination: pagination,
				Properties: properties,
			}

			promise, err := filtersAndResolver.resolver.LocalGetClass(&params)
			return promise, err
		},
	}

	return &classField, nil
}

func extractProperties(selections *graphql_ast.SelectionSet) ([]SelectProperty, error) {
	//debugFieldAsts(fieldASTs)
	var properties []SelectProperty

	for _, selection := range selections.Selections {
		field := selection.(*graphql_ast.Field)
		name := field.Name.Value
		property := SelectProperty{Name: name}

		property.IsPrimitive = (field.SelectionSet == nil)

		if !property.IsPrimitive {
			// We can interpret this property in different ways
			for _, subSelection := range field.SelectionSet.Selections {
				// Is it a field with the name __typename?
				subsectionField, ok := subSelection.(*graphql_ast.Field)
				if ok {
					if subsectionField.Name.Value == "__typename" {
						property.IncludeTypeName = true
						continue
					} else {
						return nil, fmt.Errorf("Expected a InlineFragment, not a '%s' field ", subsectionField.Name.Value)
					}
				}

				// Otherwise these _must_ be inline fragments
				fragment, ok := subSelection.(*graphql_ast.InlineFragment)
				if !ok {
					return nil, fmt.Errorf("Expected a InlineFragment; you need to specify as which type you want to retrieve a reference %#v", subSelection)
				}

				err, className := schema.ValidateClassName(fragment.TypeCondition.Name.Value)
				if err != nil {
					return nil, fmt.Errorf("The inline fragment type name '%s' is not a valid class name.", fragment.TypeCondition.Name.Value)
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
