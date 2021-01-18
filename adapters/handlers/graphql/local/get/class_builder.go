//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package get

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/sirupsen/logrus"
)

type classBuilder struct {
	schema       *schema.Schema
	knownClasses map[string]*graphql.Object
	beaconClass  *graphql.Object
	logger       logrus.FieldLogger
}

func newClassBuilder(schema *schema.Schema, logger logrus.FieldLogger) *classBuilder {
	b := &classBuilder{}

	b.logger = logger
	b.schema = schema

	b.initKnownClasses()
	b.initBeaconClass()

	return b
}

func (b *classBuilder) initKnownClasses() {
	b.knownClasses = map[string]*graphql.Object{}
}

func (b *classBuilder) initBeaconClass() {
	b.beaconClass = graphql.NewObject(graphql.ObjectConfig{
		Name: "Beacon",
		Fields: graphql.Fields{
			"beacon": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
}

func (b *classBuilder) objects() (*graphql.Object, error) {
	return b.kinds(b.schema.Objects)
}

func (b *classBuilder) kinds(kindSchema *models.Schema) (*graphql.Object, error) {
	classFields := graphql.Fields{}

	for _, class := range kindSchema.Classes {
		classField, err := b.classField(class)
		if err != nil {
			return nil, fmt.Errorf("Could not build class for %s", class.Class)
		}
		classFields[class.Class] = classField
	}

	classes := graphql.NewObject(graphql.ObjectConfig{
		Name:        "GetObjectsObj",
		Fields:      classFields,
		Description: descriptions.GetObjectsActionsObj,
	})

	return classes, nil
}

func (b *classBuilder) classField(class *models.Class) (*graphql.Field, error) {
	classObject := b.classObject(class)
	b.knownClasses[class.Class] = classObject
	classField := buildGetClassField(classObject, class)
	return &classField, nil
}

func (b *classBuilder) classObject(class *models.Class) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			classProperties := graphql.Fields{}

			b.additionalFields(classProperties, class)

			for _, property := range class.Properties {
				propertyType, err := b.schema.FindPropertyDataType(property.DataType)
				if err != nil {
					// We can't return an error in this FieldsThunk function, so we need to panic
					panic(fmt.Sprintf("buildGetClass: wrong propertyType for %s.%s; %s",
						class.Class, property.Name, err.Error()))
				}

				if propertyType.IsPrimitive() {
					classProperties[property.Name] = b.primitiveField(propertyType, property,
						class.Class)
				} else {
					classProperties[property.Name] = b.referenceField(propertyType, property,
						class.Class)
				}
			}

			return classProperties
		}),
		Description: class.Description,
	})
}

func (b *classBuilder) additionalFields(classProperties graphql.Fields, class *models.Class) {
	additionalProperties := graphql.Fields{}
	additionalProperties["classification"] = b.additionalClassificationField(class)
	additionalProperties["interpretation"] = b.additionalInterpretationField(class)
	additionalProperties["nearestNeighbors"] = b.additionalNNField(class)
	additionalProperties["featureProjection"] = b.additionalFeatureProjectionField(class)
	additionalProperties["semanticPath"] = b.additionalSemanticPathField(class)
	additionalProperties["certainty"] = b.additionalCertaintyField(class)
	additionalProperties["id"] = b.additionalIDField()
	classProperties["_additional"] = &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:   fmt.Sprintf("%sAdditional", class.Class),
			Fields: additionalProperties,
		}),
	}
}

func (b *classBuilder) additionalIDField() *graphql.Field {
	return &graphql.Field{
		Description: descriptions.GetClassUUID,
		Type:        graphql.String,
	}
}

func (b *classBuilder) additionalClassificationField(class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalClassification", class.Class),
			Fields: graphql.Fields{
				"id":               &graphql.Field{Type: graphql.String},
				"basedOn":          &graphql.Field{Type: graphql.NewList(graphql.String)},
				"scope":            &graphql.Field{Type: graphql.NewList(graphql.String)},
				"classifiedFields": &graphql.Field{Type: graphql.NewList(graphql.String)},
				"completed":        &graphql.Field{Type: graphql.String},
			},
		}),
	}
}

func (b *classBuilder) additionalInterpretationField(class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalInterpretation", class.Class),
			Fields: graphql.Fields{
				"source": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalInterpretationSource", class.Class),
					Fields: graphql.Fields{
						"concept":    &graphql.Field{Type: graphql.String},
						"weight":     &graphql.Field{Type: graphql.Float},
						"occurrence": &graphql.Field{Type: graphql.Int},
					},
				}))},
			},
		}),
	}
}

func (b *classBuilder) additionalNNField(class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalNearestNeighbors", class.Class),
			Fields: graphql.Fields{
				"neighbors": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalNearestNeighborsNeighbor", class.Class),
					Fields: graphql.Fields{
						"concept":  &graphql.Field{Type: graphql.String},
						"distance": &graphql.Field{Type: graphql.Float},
					},
				}))},
			},
		}),
	}
}

func (b *classBuilder) additionalFeatureProjectionField(class *models.Class) *graphql.Field {
	return &graphql.Field{
		Args: graphql.FieldConfigArgument{
			"algorithm": &graphql.ArgumentConfig{
				Type:         graphql.String,
				DefaultValue: nil,
			},
			"dimensions": &graphql.ArgumentConfig{
				Type:         graphql.Int,
				DefaultValue: nil,
			},
			"learningRate": &graphql.ArgumentConfig{
				Type:         graphql.Int,
				DefaultValue: nil,
			},
			"iterations": &graphql.ArgumentConfig{
				Type:         graphql.Int,
				DefaultValue: nil,
			},
			"perplexity": &graphql.ArgumentConfig{
				Type:         graphql.Int,
				DefaultValue: nil,
			},
		},
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalFeatureProjection", class.Class),
			Fields: graphql.Fields{
				"vector": &graphql.Field{Type: graphql.NewList(graphql.Float)},
			},
		}),
	}
}

func (b *classBuilder) additionalSemanticPathField(class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalSemanticPath", class.Class),
			Fields: graphql.Fields{
				"path": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalSemanticPathElement", class.Class),
					Fields: graphql.Fields{
						"concept":            &graphql.Field{Type: graphql.String},
						"distanceToQuery":    &graphql.Field{Type: graphql.Float},
						"distanceToResult":   &graphql.Field{Type: graphql.Float},
						"distanceToNext":     &graphql.Field{Type: graphql.Float},
						"distanceToPrevious": &graphql.Field{Type: graphql.Float},
					},
				}))},
			},
		}),
	}
}

func (b *classBuilder) additionalCertaintyField(class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.Float,
	}
}
