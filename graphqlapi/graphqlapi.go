/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package graphqlapi

import (
	"github.com/go-openapi/strfmt"
	graphql "github.com/graphql-go/graphql"
	"log"

	"github.com/weaviate/weaviate/connectors"
	// "github.com/weaviate/weaviate/connectors/utils"
	"github.com/weaviate/weaviate/models"
)

var (
	// WeaviateGraphQLSchema is the schema initialized by the InitSchema function
	WeaviateGraphQLSchema graphql.Schema
)

// InitSchema the GraphQL schema
func InitSchema(databaseConnector dbconnector.DatabaseConnector) error {
	// objectEnum := graphql.NewEnum(graphql.EnumConfig{
	// 	Name:        "ObjectType",
	// 	Description: "One of the type of the objects.",
	// 	Values: graphql.EnumValueConfigMap{
	// 		"THING": &graphql.EnumValueConfig{
	// 			Value:       connutils.RefTypeThing,
	// 			Description: "Thing type",
	// 		},
	// 		"ACTION": &graphql.EnumValueConfig{
	// 			Value:       connutils.RefTypeAction,
	// 			Description: "Action type",
	// 		},
	// 		"KEY": &graphql.EnumValueConfig{
	// 			Value:       connutils.RefTypeKey,
	// 			Description: "Key type",
	// 		},
	// 	},
	// })

	objectInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name:        "WeaviateObject",
		Description: "An object in the weaviate database",
		Fields: graphql.Fields{
			"uuid": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The id of the object.",
			},
		},
	})

	schemaInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name:        "WeaviateSchemaObject",
		Description: "An object that has to commit to weaviate's Thing or Action schema.",
		Fields: graphql.Fields{
			"atContext": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The context on which the object is in.",
			},
			"atClass": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The class of the object.",
			},
			"creationTimeUnix": &graphql.Field{
				Type:        graphql.Int,
				Description: "The creation time of the object.",
			},
			"lastUpdateTimeUnix": &graphql.Field{
				Type:        graphql.Int,
				Description: "The last update time of the object.",
			},
			// Schema
		},
	})

	keyType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "Key",
		Description: "A key from the weaviate database.",
		Fields: graphql.Fields{
			"uuid": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The id of the key.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
						return key.KeyID, nil
					}
					return nil, nil
				},
			},
			"token": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The token of the key.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
						return key.Token, nil // TODO: Only return when have rights
					}
					return nil, nil
				},
			},
			"email": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The email of the key.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
						return key.Email, nil
					}
					return nil, nil
				},
			},
			"ipOrigin": &graphql.Field{
				Type:        graphql.NewList(graphql.String),
				Description: "The allowed ip-origins of the key.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
						return key.IPOrigin, nil
					}
					return nil, nil
				},
			},
			"keyExpiresUnix": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Int),
				Description: "The unix timestamp of when the key expires.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
						return key.KeyExpiresUnix, nil
					}
					return nil, nil
				},
			},
			"read": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Boolean),
				Description: "Whether the key has read-rights.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
						return key.Read, nil
					}
					return nil, nil
				},
			},
			"execute": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Boolean),
				Description: "Whether the key has execute-rights.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
						return key.Execute, nil
					}
					return nil, nil
				},
			},
			"write": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Boolean),
				Description: "Whether the key has write-rights.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
						return key.Write, nil
					}
					return nil, nil
				},
			},
			"delete": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Boolean),
				Description: "Whether the key has delete-rights.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
						return key.Delete, nil
					}
					return nil, nil
				},
			},
		},
		Interfaces: []*graphql.Interface{
			objectInterface,
		},
	})

	// Add to interface here, because when initializing the interface, keyType does not exist.
	schemaInterface.AddFieldConfig("key", &graphql.Field{
		Type:        keyType,
		Description: "The key which is the owner of the object.",
	})

	// Add to keyType here, because when initializing the keyType, keyType itself does not exist.
	keyType.AddFieldConfig("parent", &graphql.Field{
		Type:        keyType,
		Description: "The parent of the key.",
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			keyResponse := models.KeyTokenGetResponse{}
			if key, ok := p.Source.(models.KeyTokenGetResponse); ok {
				err := databaseConnector.GetKey(key.Parent.NrDollarCref, &keyResponse)
				if err != nil {
					return keyResponse, err
				}
			}
			return keyResponse, nil
		},
	})

	thingType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "Thing",
		Description: "A thing from the weaviate database, based on the weaviate schema.",
		Fields: graphql.Fields{
			"atContext": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The context on which the object is in.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if thing, ok := p.Source.(models.ThingGetResponse); ok {
						return thing.AtContext, nil
					}
					return nil, nil
				},
			},
			"atClass": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The class of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if thing, ok := p.Source.(models.ThingGetResponse); ok {
						return thing.AtClass, nil
					}
					return nil, nil
				},
			},
			"creationTimeUnix": &graphql.Field{
				Type:        graphql.Int,
				Description: "The creation time of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if thing, ok := p.Source.(models.ThingGetResponse); ok {
						return thing.CreationTimeUnix, nil // TODO: Nil in response?
					}
					return nil, nil
				},
			},
			"lastUpdateTimeUnix": &graphql.Field{
				Type:        graphql.Int,
				Description: "The last update time of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if thing, ok := p.Source.(models.ThingGetResponse); ok {
						return thing.LastUpdateTimeUnix, nil
					}
					return nil, nil
				},
			},
			"uuid": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The id of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if thing, ok := p.Source.(models.ThingGetResponse); ok {
						return thing.ThingID, nil
					}
					return nil, nil
				},
			},
			"key": &graphql.Field{
				Type:        keyType,
				Description: "The key which is the owner of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					keyResponse := models.KeyTokenGetResponse{}
					if thing, ok := p.Source.(models.ThingGetResponse); ok {
						err := databaseConnector.GetKey(thing.Key.NrDollarCref, &keyResponse)
						if err != nil {
							return keyResponse, err
						}
					}
					return keyResponse, nil
				},
			},
		},
		Interfaces: []*graphql.Interface{
			schemaInterface,
			objectInterface,
		},
	})

	objectSubjectType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "ObjectSubject",
		Description: "An object / subject, part of action. These are both of type Thing.",
		Fields: graphql.Fields{
			"object": &graphql.Field{
				Type:        thingType,
				Description: "The thing which is the object of this action.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					thingResponse := models.ThingGetResponse{}
					if ref, ok := p.Source.(*models.ObjectSubject); ok {
						err := databaseConnector.GetThing(ref.Object.NrDollarCref, &thingResponse)
						if err != nil {
							return thingResponse, err
						}
					}
					return thingResponse, nil
				},
			},
			"subject": &graphql.Field{
				Type:        thingType,
				Description: "The thing which is the subject of this action.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					thingResponse := models.ThingGetResponse{}
					if ref, ok := p.Source.(*models.ObjectSubject); ok {
						err := databaseConnector.GetThing(ref.Subject.NrDollarCref, &thingResponse)
						if err != nil {
							return thingResponse, err
						}
					}
					return thingResponse, nil
				},
			},
		},
	})

	actionType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "Action",
		Description: "A action from the weaviate database, based on the weaviate schema.",
		Fields: graphql.Fields{
			"atContext": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The context on which the object is in.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if action, ok := p.Source.(models.ActionGetResponse); ok {
						return action.AtContext, nil
					}
					return nil, nil
				},
			},
			"atClass": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The class of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if action, ok := p.Source.(models.ActionGetResponse); ok {
						return action.AtClass, nil
					}
					return nil, nil
				},
			},
			"creationTimeUnix": &graphql.Field{
				Type:        graphql.Int,
				Description: "The creation time of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if action, ok := p.Source.(models.ActionGetResponse); ok {
						return action.CreationTimeUnix, nil // TODO: Nil in response?
					}
					return nil, nil
				},
			},
			"lastUpdateTimeUnix": &graphql.Field{
				Type:        graphql.Int,
				Description: "The last update time of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if action, ok := p.Source.(models.ActionGetResponse); ok {
						return action.LastUpdateTimeUnix, nil
					}
					return nil, nil
				},
			},
			"uuid": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The id of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if action, ok := p.Source.(models.ActionGetResponse); ok {
						return action.ActionID, nil
					}
					return nil, nil
				},
			},
			"things": &graphql.Field{
				Type:        objectSubjectType,
				Description: "The thing which is the object of this action.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					if action, ok := p.Source.(models.ActionGetResponse); ok {
						return action.Things, nil
					}
					return nil, nil
				},
			},
			"key": &graphql.Field{
				Type:        keyType,
				Description: "The key which is the owner of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					keyResponse := models.KeyTokenGetResponse{}
					if action, ok := p.Source.(models.ActionGetResponse); ok {
						err := databaseConnector.GetKey(action.Key.NrDollarCref, &keyResponse)
						if err != nil {
							return keyResponse, err
						}
					}
					return keyResponse, nil
				},
			},
		},
		Interfaces: []*graphql.Interface{
			schemaInterface,
			objectInterface,
		},
	})

	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"thing": &graphql.Field{
				Type: thingType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{
						Description: "UUID of the thing",
						Type:        graphql.NewNonNull(graphql.String),
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					thingResponse := models.ThingGetResponse{}
					UUID := strfmt.UUID(p.Args["id"].(string))
					err := databaseConnector.GetThing(UUID, &thingResponse)
					if err != nil {
						return thingResponse, err
					}
					return thingResponse, nil
				},
			},
			"action": &graphql.Field{
				Type: actionType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{
						Description: "UUID of the action",
						Type:        graphql.NewNonNull(graphql.String),
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					actionResponse := models.ActionGetResponse{}
					actionResponse.Schema = map[string]models.JSONObject{}
					actionResponse.Things = &models.ObjectSubject{}

					UUID := strfmt.UUID(p.Args["id"].(string))
					err := databaseConnector.GetAction(UUID, &actionResponse)
					if err != nil {
						return actionResponse, err
					}
					return actionResponse, nil
				},
			},
			"key": &graphql.Field{
				Type: keyType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{
						Description: "UUID of the key",
						Type:        graphql.NewNonNull(graphql.String),
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					keyResponse := models.KeyTokenGetResponse{}

					UUID := strfmt.UUID(p.Args["id"].(string))
					err := databaseConnector.GetKey(UUID, &keyResponse)
					if err != nil {
						return keyResponse, err
					}
					return keyResponse, nil
				},
			},
		},
	})

	var err error
	WeaviateGraphQLSchema, err = graphql.NewSchema(graphql.SchemaConfig{
		Query: queryType,
	})

	log.Println("INFO: GraphQL initialisation finished.")

	return err
}
