package local_get

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	common "github.com/creativesoftwarefdn/weaviate/graphqlapi/common_resolver"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_resolver"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
	graphql_ast "github.com/graphql-go/graphql/language/ast"
	"strings"
)

func Build(dbSchema *schema.Schema) (*graphql.Field, error) {
	getKinds := graphql.Fields{}

	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("There are not any Actions or Things classes defined yet.")
	}

	knownClasses := map[string]*graphql.Object{}

	if len(dbSchema.Actions.Classes) > 0 {
		localGetActions, err := buildGetClasses(dbSchema, kind.ACTION_KIND, dbSchema.Actions, &knownClasses)
		if err != nil {
			return nil, err
		}

		getKinds["Actions"] = &graphql.Field{
			Name:        "WeaviateLocalGetActions",
			Description: "Get Actions on the Local Weaviate",
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
			Description: "Get Things on the Local Weaviate",
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
		Description: "Get Things or Actions on the local weaviate",
		Args: graphql.FieldConfigArgument{
			"where": &graphql.ArgumentConfig{
				Description: "Filter options for the Get search, to convert the data to the filter input",
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        "WeaviateLocalGetWhereInpObj",
						Fields:      common_filters.CommonFilters,
						Description: "Filter options for the Get search, to convert the data to the filter input",
					},
				),
			},
		},
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:        "WeaviateLocalGetObj",
			Fields:      getKinds,
			Description: "Type of Get function to get Things or Actions on the Local Weaviate",
		}),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			fmt.Printf("- LocalGet (extracting filters, retrieving resolver)\n")
			resolver := p.Source.(map[string]interface{})["Resolver"].(Resolver)
			filters, err := extractLocalGetFilters(p)

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
		Description: fmt.Sprintf("Type of %ss i.e. %ss classes to Get on the Local Weaviate", kindName, kindName),
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
				Description: "UUID of the thing or action given by the local Weaviate instance",
				Type:        graphql.String,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					fmt.Printf("WHOOPTYDOO uuid\n")
					return "uuid", nil
				},
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
							Resolve: func(p graphql.ResolveParams) (interface{}, error) {
								fmt.Printf("GET PRIMITIVE PROP: string\n")
								return "primitive string", nil
							},
						}
					case schema.DataTypeInt:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.Int,
							Resolve: func(p graphql.ResolveParams) (interface{}, error) {
								fmt.Printf("GET PRIMITIVE PROP: int\n")
								return nil, nil
							},
						}
					case schema.DataTypeNumber:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.Float,
							Resolve: func(p graphql.ResolveParams) (interface{}, error) {
								fmt.Printf("GET PRIMITIVE PROP: float\n")
								return 4.2, nil
							},
						}
					case schema.DataTypeBoolean:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.Boolean,
							Resolve: func(p graphql.ResolveParams) (interface{}, error) {
								fmt.Printf("GET PRIMITIVE PROP: bool\n")
								return true, nil
							},
						}
					case schema.DataTypeDate:
						propertyField = &graphql.Field{
							Description: property.Description,
							Type:        graphql.String, // String since no graphql date datatype exists
							Resolve: func(p graphql.ResolveParams) (interface{}, error) {
								fmt.Printf("GET PRIMITIVE PROP: date\n")
								return "somedate", nil
							},
						}
					default:
						panic(fmt.Sprintf("buildGetClass: unknown primitive type for %s.%s.%s; %s", k.Name(), class.Class, property.Name, propertyType.AsPrimitive()))
					}

					propertyField.Name = property.Name

					fmt.Printf("ADDING CLASS PROPERTY %s.%s.%s = %#v\n", k.Name(), class.Class, property.Name, propertyField)
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
						Resolve: func(p graphql.ResolveParams) (interface{}, error) {
							fmt.Printf("- Resolve action property field (ref?)\n")
							fmt.Printf("WHOOPTYDOO2\n")
							return true, nil
						},
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
				Description: "Pagination option, show the first x results",
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: "Pagination option, show the results after the first x results",
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

			properties, err := extractPropertiesFromFieldASTs(p.Info.FieldASTs)
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

func extractLocalGetFilters(p graphql.ResolveParams) (*common_resolver.LocalFilters, error) {
	filters := common_resolver.LocalFilters{}

	return &filters, nil
}

func extractPropertiesFromFieldASTs(fieldASTs []*graphql_ast.Field) ([]SelectProperty, error) {
	var properties []SelectProperty

	for _, fieldAST := range fieldASTs {
		selections := fieldAST.SelectionSet.Selections
		if len(selections) != 1 {
			panic("unspected length")
		}
		selection := selections[0].(*graphql_ast.Field)
		name := selection.Name.Value
		properties = append(properties, SelectProperty{Name: name})
	}

	return properties, nil
}
