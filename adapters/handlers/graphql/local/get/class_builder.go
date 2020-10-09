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
	"strings"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get/refclasses"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus"
)

type classBuilder struct {
	schema          *schema.Schema
	peers           peers.Peers
	knownClasses    map[string]*graphql.Object
	knownRefClasses refclasses.ByNetworkClass
	beaconClass     *graphql.Object
	logger          logrus.FieldLogger
}

func newClassBuilder(schema *schema.Schema, peers peers.Peers, logger logrus.FieldLogger) *classBuilder {
	b := &classBuilder{}

	b.logger = logger
	b.schema = schema
	b.peers = peers

	b.initKnownClasses()
	b.initRefs()
	b.initBeaconClass()

	return b
}

func (b *classBuilder) initKnownClasses() {
	b.knownClasses = map[string]*graphql.Object{}
}

func (b *classBuilder) initRefs() {
	networkRefs := extractNetworkRefClassNames(b.schema)
	knownRefClasses, err := refclasses.FromPeers(b.peers, networkRefs)
	if err != nil {
		msg := "an error occured while trying to build known network ref classes, " +
			"this kind of error won't block the graphql api, but it does mean that the mentioned refs " +
			"will not be available. This error is expected when the network is not ready yet. If so, " +
			"it should not reappear after a peer update"
		b.logger.WithField("action", "graphql_rebuild").WithError(err).Warning(msg)
	}

	b.knownRefClasses = knownRefClasses
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

func (b *classBuilder) actions() (*graphql.Object, error) {
	return b.kinds(kind.Action, b.schema.Actions)
}

func (b *classBuilder) things() (*graphql.Object, error) {
	return b.kinds(kind.Thing, b.schema.Things)
}

func (b *classBuilder) kinds(k kind.Kind, kindSchema *models.Schema) (*graphql.Object, error) {
	classFields := graphql.Fields{}
	kindName := strings.Title(k.Name())

	for _, class := range kindSchema.Classes {
		classField, err := b.classField(k, class)
		if err != nil {
			return nil, fmt.Errorf("Could not build class for %s", class.Class)
		}
		classFields[class.Class] = classField
	}

	classes := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Get%ssObj", kindName),
		Fields:      classFields,
		Description: fmt.Sprintf(descriptions.GetThingsActionsObj, kindName),
	})

	return classes, nil
}

func (b *classBuilder) classField(k kind.Kind, class *models.Class) (*graphql.Field, error) {
	classObject := b.classObject(k.Name(), class)
	b.knownClasses[class.Class] = classObject
	classField := buildGetClassField(classObject, k, class)
	return &classField, nil
}

func (b *classBuilder) classObject(kindName string, class *models.Class) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			classProperties := graphql.Fields{}

			classProperties["uuid"] = &graphql.Field{
				Description: descriptions.GetClassUUID,
				Type:        graphql.String,
			}

			b.underscoreFields(classProperties, kindName, class)

			for _, property := range class.Properties {
				propertyType, err := b.schema.FindPropertyDataType(property.DataType)
				if err != nil {
					// We can't return an error in this FieldsThunk function, so we need to panic
					panic(fmt.Sprintf("buildGetClass: wrong propertyType for %s.%s.%s; %s",
						kindName, class.Class, property.Name, err.Error()))
				}

				var field *graphql.Field
				var fieldName string
				if propertyType.IsPrimitive() {
					fieldName = property.Name
					field = b.primitiveField(propertyType, property, kindName, class.Class)
				} else {
					// uppercase key because it's a reference
					fieldName = strings.Title(property.Name)
					field = b.referenceField(propertyType, property, kindName, class.Class)
				}
				classProperties[fieldName] = field
			}

			return classProperties
		}),
		Description: class.Description,
	})
}

func (b *classBuilder) underscoreFields(classProperties graphql.Fields, kindName string, class *models.Class) {
	classProperties["_classification"] = b.underscoreClassificationField(kindName, class)
	classProperties["_interpretation"] = b.underscoreInterpretationField(kindName, class)
	classProperties["_nearestNeighbors"] = b.underscoreNNField(kindName, class)
	classProperties["_featureProjection"] = b.underscoreFeatureProjectionField(kindName, class)
	classProperties["_semanticPath"] = b.underscoreSemanticPathField(kindName, class)
	classProperties["_certainty"] = b.underscor

}

func (b *classBuilder) underscoreClassificationField(kindName string, class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sUnderscoreClassification", class.Class),
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

func (b *classBuilder) underscoreInterpretationField(kindName string, class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sUnderscoreInterpretation", class.Class),
			Fields: graphql.Fields{
				"source": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sUnderscoreInterpretationSource", class.Class),
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

func (b *classBuilder) underscoreNNField(kindName string, class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sUnderscoreNearestNeighbors", class.Class),
			Fields: graphql.Fields{
				"neighbors": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sUnderscoreNearestNeighborsNeighbor", class.Class),
					Fields: graphql.Fields{
						"concept":  &graphql.Field{Type: graphql.String},
						"distance": &graphql.Field{Type: graphql.Float},
					},
				}))},
			},
		}),
	}
}

func (b *classBuilder) underscoreFeatureProjectionField(kindName string, class *models.Class) *graphql.Field {
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
			Name: fmt.Sprintf("%sUnderscoreFeatureProjection", class.Class),
			Fields: graphql.Fields{
				"vector": &graphql.Field{Type: graphql.NewList(graphql.Float)},
			},
		}),
	}
}

func (b *classBuilder) underscoreSemanticPathField(kindName string, class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sUnderscoreSemanticPath", class.Class),
			Fields: graphql.Fields{
				"path": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sUnderscoreSemanticPathElement", class.Class),
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

func (b *classBuilder) underscoreCertaintyField(kindName string, class *models.Class) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql),
	}
}
