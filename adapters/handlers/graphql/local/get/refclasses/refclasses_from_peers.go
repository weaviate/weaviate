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

package refclasses

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
)

// ByNetworkClass is map of GraphQLObjects representing referenced network classes
type ByNetworkClass map[crossrefs.NetworkClass]*graphql.Object

// FromPeers returns the referenced classes in form of a ByNetworkClass map.
// They differ from the actual classes as used in the network package in that -
// since they already are a reference - dont allow another reference. I.e. they
// can only have primitive props.
//
// This function errors if a peer is not present or doesn't have the class in
// its schema.  However, this error should only be logged down the line, it
// should never prevent the graphql schema from being built, at is very
// possible that peers leave the network.  This shouldn't prevent us from
// building our own grapqhl api. We can simply remove the references down the
// line.
func FromPeers(peers peers.Peers, classes []crossrefs.NetworkClass) (ByNetworkClass, error) {
	result := ByNetworkClass{}
	if len(classes) == 0 {
		return result, nil
	}

	for _, desiredClass := range classes {
		peer, err := peers.ByName(desiredClass.PeerName)
		if err != nil {
			return result, fmt.Errorf("could not build class '%s': %s", desiredClass.String(), err)
		}

		class := peer.Schema.FindClassByName(schema.ClassName(desiredClass.ClassName))
		if class == nil {
			return result, fmt.Errorf("could not build class '%s': peer '%s' has no such class",
				desiredClass.String(), peer.Name)
		}

		name := fmt.Sprintf("%s__%s", desiredClass.PeerName, desiredClass.ClassName)
		result[desiredClass] = graphqlObjectFromClass(name, desiredClass.String(), class, &peer.Schema)
	}

	return result, nil
}

func graphqlObjectFromClass(name string, networkClassName string, class *models.Class,
	dbSchema *schema.Schema) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: name,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			classProperties := graphql.Fields{}

			classProperties["uuid"] = &graphql.Field{
				Description: descriptions.LocalGetClassUUID,
				Type:        graphql.String,
			}

			for _, property := range class.Properties {
				propertyType, err := dbSchema.FindPropertyDataType(property.DataType)
				if err != nil {
					// this schema from a remote instance is not fully in our control, if
					// we don't understand the datatype it could be down to an
					// out-of-date schema, different versions (e.g. a newer version of
					// weaviate could have added a new primitive type) or anythign else
					// that's out of our control. We thus choose to skip this property,
					// rather than panicking and preventing all other network refs from
					// being built as well.
					continue
				}

				var field *graphql.Field
				var fieldName string
				if propertyType.IsPrimitive() {
					// ignore non-primitive props
					fieldName = property.Name
					field = buildPrimitiveField(propertyType, property, networkClassName, class.Class)
					classProperties[fieldName] = field
				}
			}

			return classProperties
		}),
		Description: class.Description,
	})
}

func buildPrimitiveField(propertyType schema.PropertyDataType,
	property *models.Property, networkClassName, className string) *graphql.Field {
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
			networkClassName, className, property.Name, propertyType.AsPrimitive()))
	}
}
