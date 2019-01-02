package local_get

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get/refclasses"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
	"github.com/graphql-go/graphql"
)

// NetworkRef is a WIP, it will most likely change
type NetworkRef struct {
	AtClass string
	RawRef  map[string]interface{}
}

// LocalRef to be filled by the database connector to indicate that the
// particular reference field is a local ref and does not require further
// resolving, as opposed to a NetworkRef.
type LocalRef struct {
	AtClass string
	Fields  map[string]interface{}
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
		Name:        fmt.Sprintf("%s%s%s", className, propertyName, "Obj"),
		Types:       dataTypeClasses,
		ResolveType: makeResolveClassUnionType(knownClasses, knownRefClasses),
		Description: property.Description,
	})

	return &graphql.Field{
		Type:        graphql.NewList(classUnion),
		Description: property.Description,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			items := p.Source.(map[string]interface{})[p.Info.FieldName].([]interface{})
			results := make([]interface{}, len(items), len(items))
			for i, item := range items {
				switch v := item.(type) {
				case LocalRef:
					localRef := v.Fields
					localRef["__refClassType"] = "local"
					localRef["__refClassName"] = v.AtClass
					results[i] = localRef

				case NetworkRef:
					networkRef := func() (interface{}, error) {
						return map[string]interface{}{
							"__refClassType":     "network",
							"__refClassName":     "Country",
							"__refClassPeerName": "WeaviateB",
							"name":               "hard-coded, but should be network resolved",
						}, nil
					}
					results[i] = networkRef

				default:
					return nil, fmt.Errorf("unsupported type %t", v)
				}
			}
			return results, nil
		},
	}
}

func makeResolveClassUnionType(knownClasses *map[string]*graphql.Object,
	knownRefClasses refclasses.ByNetworkClass) graphql.ResolveTypeFn {
	return func(p graphql.ResolveTypeParams) *graphql.Object {
		valueMap := p.Value.(map[string]interface{})
		refType := valueMap["__refClassType"].(string)
		switch refType {
		case "local":
			className := valueMap["__refClassName"].(string)
			return (*knownClasses)[className]
		case "network":
			className := valueMap["__refClassName"].(string)
			peerName := valueMap["__refClassPeerName"].(string)
			return knownRefClasses[crossrefs.NetworkClass{ClassName: className, PeerName: peerName}]
		default:
			panic(fmt.Sprintf("unknown ref type %#v", refType))
		}
	}
}
