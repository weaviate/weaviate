/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package get

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/get/refclasses"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/common/peers"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/crossrefs"
	"github.com/graphql-go/graphql"
)

// NetworkRef is a WIP, it will most likely change
type NetworkRef struct {
	crossrefs.NetworkKind
}

// LocalRef to be filled by the database connector to indicate that the
// particular reference field is a local ref and does not require further
// resolving, as opposed to a NetworkRef.
type LocalRef struct {
	Class  string
	Fields map[string]interface{}
}

func buildReferenceField(propertyType schema.PropertyDataType,
	property *models.SemanticSchemaClassProperty, kindName, className string,
	knownClasses *map[string]*graphql.Object, knownRefClasses refclasses.ByNetworkClass,
	peers peers.Peers) *graphql.Field {
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
		Resolve:     makeResolveRefField(peers),
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
			classObj, ok := (*knownClasses)[className]
			if !ok {
				panic(fmt.Errorf(
					"local ref refers to class '%s', but no such kind exists in the peer network", className))
			}
			return classObj

		case "network":
			className := valueMap["__refClassName"].(string)
			peerName := valueMap["__refClassPeerName"].(string)
			remoteKind := crossrefs.NetworkClass{ClassName: className, PeerName: peerName}
			classObj, ok := knownRefClasses[remoteKind]
			if !ok {
				panic(fmt.Errorf(
					"network ref refers to remote kind %v, but no such kind exists in the peer network",
					remoteKind))
			}

			return classObj

		default:
			panic(fmt.Sprintf("unknown ref type %#v", refType))
		}
	}
}

func makeResolveRefField(peers peers.Peers) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		if p.Source.(map[string]interface{})[p.Info.FieldName] == nil {
			return nil, nil
		}

		items := p.Source.(map[string]interface{})[p.Info.FieldName].([]interface{})
		results := make([]interface{}, len(items), len(items))
		for i, item := range items {
			switch v := item.(type) {
			case LocalRef:
				// inject some meta data so the ResolveType can determine the type
				localRef := v.Fields
				localRef["__refClassType"] = "local"
				localRef["__refClassName"] = v.Class
				results[i] = localRef

			case NetworkRef:
				networkRef := func() (interface{}, error) {
					result, err := peers.RemoteKind(v.NetworkKind)
					if err != nil {
						return nil, fmt.Errorf("could not get remote kind for '%v': %s", v, err)
					}

					var schema map[string]interface{}
					schema, err = extractSchemaFromKind(v, result)
					if err != nil {
						return nil, err
					}

					// inject some meta data so the ResolveType can determine the type
					schema["__refClassType"] = "network"
					schema["__refClassPeerName"] = v.PeerName
					schema["uuid"] = v.ID
					return schema, nil
				}
				results[i] = networkRef

			default:
				return nil, fmt.Errorf("unsupported type %t", v)
			}
		}
		return results, nil
	}
}
func extractSchemaFromKind(v NetworkRef, result interface{}) (map[string]interface{}, error) {
	switch v.Kind {
	case kind.Thing:
		thing, ok := result.(*models.Thing)
		if !ok {
			return nil, fmt.Errorf("expected a models.Thing, but remote instance returned %#v", result)
		}

		schema, ok := thing.Schema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected schema of '%v', to be a map, but is: %#v", v, schema)
		}
		schema["__refClassName"] = thing.Class

		return schema, nil
	case kind.Action:
		action, ok := result.(models.Action)
		if !ok {
			return nil, fmt.Errorf("expected a models.Action, but remote instance returned %#v", result)
		}

		schema, ok := action.Schema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected schema of '%v', to be a map, but is: %#v", v, schema)
		}
		schema["__refClassName"] = action.Class
		return schema, nil

	default:
		return nil, fmt.Errorf("not a known kind: %s", v.Kind)
	}
}
