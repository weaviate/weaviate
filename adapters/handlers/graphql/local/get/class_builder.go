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

package get

import (
	"fmt"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type classBuilder struct {
	schema          *schema.Schema
	knownClasses    map[string]*graphql.Object
	beaconClass     *graphql.Object
	logger          logrus.FieldLogger
	modulesProvider ModulesProvider
}

func newClassBuilder(schema *schema.Schema, logger logrus.FieldLogger,
	modulesProvider ModulesProvider,
) *classBuilder {
	b := &classBuilder{}

	b.logger = logger
	b.schema = schema
	b.modulesProvider = modulesProvider

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
	// needs to be defined outside the individual class as there can only be one definition of an enum
	fusionAlgoEnum := graphql.NewEnum(graphql.EnumConfig{
		Name: "FusionEnum",
		Values: graphql.EnumValueConfigMap{
			"rankedFusion": &graphql.EnumValueConfig{
				Value: common_filters.HybridRankedFusion,
			},
			"relativeScoreFusion": &graphql.EnumValueConfig{
				Value: common_filters.HybridRelativeScoreFusion,
			},
		},
	})
	classFields := graphql.Fields{}
	for _, class := range kindSchema.Classes {
		classField, err := b.classField(class, fusionAlgoEnum)
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

func (b *classBuilder) classField(class *models.Class, fusionEnum *graphql.Enum) (*graphql.Field, error) {
	classObject := b.classObject(class)
	b.knownClasses[class.Class] = classObject
	classField := buildGetClassField(classObject, class, b.modulesProvider, fusionEnum)
	return &classField, nil
}

func (b *classBuilder) classObject(class *models.Class) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			classProperties := graphql.Fields{}
			for _, property := range class.Properties {
				propertyType, err := b.schema.FindPropertyDataType(property.DataType)
				if err != nil {
					if errors.Is(err, schema.ErrRefToNonexistentClass) {
						// This is a common case when a class which is referenced
						// by another class is deleted, leaving the referencing
						// class with an invalid reference property. Panicking
						// is not necessary here
						b.logger.WithField("action", "graphql_rebuild").
							Warnf("ignoring ref prop %q on class %q, because it contains reference to nonexistent class %q",
								property.Name, class.Class, property.DataType)

						continue
					} else {
						// We can't return an error in this FieldsThunk function, so we need to panic
						panic(fmt.Sprintf("buildGetClass: wrong propertyType for %s.%s; %s",
							class.Class, property.Name, err.Error()))
					}
				}

				if propertyType.IsPrimitive() {
					classProperties[property.Name] = b.primitiveField(propertyType, property,
						class.Class)
				} else if propertyType.IsNested() {
					classProperties[property.Name] = b.nestedField(propertyType, property,
						class.Class)
				} else {
					classProperties[property.Name] = b.referenceField(propertyType, property,
						class.Class)
				}
			}

			b.additionalFields(classProperties, class)

			return classProperties
		}),
		Description: class.Description,
	})
}

func (b *classBuilder) additionalFields(classProperties graphql.Fields, class *models.Class) {
	additionalProperties := graphql.Fields{}
	additionalProperties["classification"] = b.additionalClassificationField(class)
	additionalProperties["certainty"] = b.additionalCertaintyField(class)
	additionalProperties["distance"] = b.additionalDistanceField(class)
	additionalProperties["vector"] = b.additionalVectorField(class)
	additionalProperties["id"] = b.additionalIDField()
	additionalProperties["creationTimeUnix"] = b.additionalCreationTimeUnix()
	additionalProperties["lastUpdateTimeUnix"] = b.additionalLastUpdateTimeUnix()
	additionalProperties["score"] = b.additionalScoreField()
	additionalProperties["explainScore"] = b.additionalExplainScoreField()
	additionalProperties["group"] = b.additionalGroupField(classProperties, class)
	if replicationEnabled(class) {
		additionalProperties["isConsistent"] = b.isConsistentField()
	}
	// module specific additional properties
	if b.modulesProvider != nil {
		for name, field := range b.modulesProvider.GetAdditionalFields(class) {
			additionalProperties[name] = field
		}
	}
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

func (b *classBuilder) additionalCertaintyField(class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.Float,
	}
}

func (b *classBuilder) additionalDistanceField(class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.Float,
	}
}

func (b *classBuilder) additionalVectorField(class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewList(graphql.Float),
	}
}

func (b *classBuilder) additionalCreationTimeUnix() *graphql.Field {
	return &graphql.Field{
		Type: graphql.String,
	}
}

func (b *classBuilder) additionalScoreField() *graphql.Field {
	return &graphql.Field{
		Type: graphql.String,
	}
}

func (b *classBuilder) additionalExplainScoreField() *graphql.Field {
	return &graphql.Field{
		Type: graphql.String,
	}
}

func (b *classBuilder) additionalLastUpdateTimeUnix() *graphql.Field {
	return &graphql.Field{
		Type: graphql.String,
	}
}

func (b *classBuilder) isConsistentField() *graphql.Field {
	return &graphql.Field{
		Type: graphql.Boolean,
	}
}

func (b *classBuilder) additionalGroupField(classProperties graphql.Fields, class *models.Class) *graphql.Field {
	hitsFields := graphql.Fields{
		"_additional": &graphql.Field{
			Type: graphql.NewObject(
				graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalGroupHitsAdditional", class.Class),
					Fields: graphql.Fields{
						"id":       &graphql.Field{Type: graphql.String},
						"vector":   &graphql.Field{Type: graphql.NewList(graphql.Float)},
						"distance": &graphql.Field{Type: graphql.Float},
					},
				},
			),
		},
	}
	for name, field := range classProperties {
		hitsFields[name] = field
	}
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalGroup", class.Class),
			Fields: graphql.Fields{
				"id": &graphql.Field{Type: graphql.Int},
				"groupedBy": &graphql.Field{
					Type: graphql.NewObject(graphql.ObjectConfig{
						Name: fmt.Sprintf("%sAdditionalGroupGroupedBy", class.Class),
						Fields: graphql.Fields{
							"path": &graphql.Field{
								Type: graphql.NewList(graphql.String),
							},
							"value": &graphql.Field{
								Type: graphql.String,
							},
						},
					}),
				},

				"minDistance": &graphql.Field{Type: graphql.Float},
				"maxDistance": &graphql.Field{Type: graphql.Float},
				"count":       &graphql.Field{Type: graphql.Int},
				"hits": &graphql.Field{
					Type: graphql.NewList(graphql.NewObject(
						graphql.ObjectConfig{
							Name:   fmt.Sprintf("%sAdditionalGroupHits", class.Class),
							Fields: hitsFields,
						},
					)),
				},
			},
		}),
	}
}
