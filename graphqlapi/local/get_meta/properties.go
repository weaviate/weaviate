package local_get_meta

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/graphql-go/graphql"
)

type MetaProperties struct {
	properties map[schema.DataType]*graphql.Object
	classMeta  *graphql.Field
}

func newMetaProperties() MetaProperties {
	countProp := &graphql.Field{
		Name:        "MetaPropertyCountPresent",
		Type:        graphql.Int,
		Description: "Number of instances that have defined this property",
	}

	datatypeProp := &graphql.Field{
		Name:        "MetaPropertyDataType",
		Type:        graphql.String,
		Description: "Datatype of the property",
	}

	return MetaProperties{
		classMeta: &graphql.Field{
			Description: "Meta information about a class object and its (filtered) objects",
			Type: graphql.NewObject(graphql.ObjectConfig{
				Name:        "MetaObject",
				Description: "Meta information about a class object and its (filtered) objects",
				Fields: graphql.Fields{
					"count": &graphql.Field{
						Name:        fmt.Sprintf("MetaCount"),
						Description: "Total amount of found instances",
						Type:        graphql.Int,
					},
				},
			}),
		},
		properties: map[schema.DataType]*graphql.Object{

			schema.DataTypeString: graphql.NewObject(graphql.ObjectConfig{
				Name:        "MetaStringObject",
				Description: "Meta information about a string",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"topOccurences": &graphql.Field{
						Name: "MetaStringTopOccurances",
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

						Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: "MetaStringOccurances",
							Fields: graphql.Fields{
								"value": &graphql.Field{
									Name:        "MetaStringValue",
									Description: "The most frequently occurring value of this property in the dataset",
									Type:        graphql.String,
								},
								"occurs": &graphql.Field{
									Name:        "MetaStringOccurs",
									Description: "Number of occurrence of this property value",
									Type:        graphql.Int,
								},
							},
						}))},
				},
			}),

			schema.DataTypeNumber: graphql.NewObject(graphql.ObjectConfig{
				Name:        "MetaNumberObject",
				Description: "Meta information about a Number",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"lowest": &graphql.Field{
						Name:        "MetaNumberLowest",
						Description: "Lowest value found in the dataset for this property",
						Type:        graphql.Float,
					},
					"highest": &graphql.Field{
						Name:        "MetaNumberLowest",
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Float,
					},
					"average": &graphql.Field{
						Name:        "MetaNumberLowest",
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Float,
					},
					"sum": &graphql.Field{
						Name:        "MetaNumberLowest",
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Float,
					},
				},
			}),

			schema.DataTypeInt: graphql.NewObject(graphql.ObjectConfig{
				Name:        "MetaIntObject",
				Description: "Meta information about a Int",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"lowest": &graphql.Field{
						Name:        "MetaIntLowest",
						Description: "Lowest value found in the dataset for this property",
						Type:        graphql.Int,
					},
					"highest": &graphql.Field{
						Name:        "MetaIntLowest",
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Int,
					},
					"average": &graphql.Field{
						Name:        "MetaIntLowest",
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Float,
					},
					"sum": &graphql.Field{
						Name:        "MetaIntLowest",
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Int,
					},
				},
			}),

			schema.DataTypeBoolean: graphql.NewObject(graphql.ObjectConfig{
				Name:        "MetaBooleanObject",
				Description: "Meta information about a Boolean",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"totalTrue": &graphql.Field{
						Name:        "MetaBooleanTotalTrue",
						Description: "The amount of times this property's value is true in the dataset",
						Type:        graphql.Int,
					},
					"percentageTrue": &graphql.Field{
						Name:        "MetaBooleanPercentageTrue",
						Description: "Percentage of boolean values that is true",
						Type:        graphql.Float,
					},
					"totalFalse": &graphql.Field{
						Name:        "MetaBooleanTotalFalse",
						Description: "The amount of times this property's value is true in the dataset",
						Type:        graphql.Int,
					},
					"percentageFalse": &graphql.Field{
						Name:        "MetaBooleanPercentageFalse",
						Description: "Percentage of boolean values that is true",
						Type:        graphql.Float,
					},
				},
			}),

			schema.DataTypeDate: graphql.NewObject(graphql.ObjectConfig{
				Name:        "MetaDateObject",
				Description: "Meta information about a date",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"topOccurences": &graphql.Field{
						Name: "MetaDateTopOccurances",
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

						Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: "MetaDateOccurances",
							Fields: graphql.Fields{
								"value": &graphql.Field{
									Name:        "MetaDateValue",
									Description: "The most frequently occurring value of this property in the dataset",
									Type:        graphql.String,
								},
								"occurs": &graphql.Field{
									Name:        "MetaDateOccurs",
									Description: "Number of occurrence of this property value",
									Type:        graphql.Int,
								},
							},
						}))},
				},
			}),

			// End of types
		},
	}
}
