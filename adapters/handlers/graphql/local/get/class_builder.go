//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
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
