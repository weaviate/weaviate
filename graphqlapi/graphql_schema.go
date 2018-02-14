/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package graphqlapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	ast "github.com/graphql-go/graphql/language/ast"
	"gopkg.in/nicksrandall/dataloader.v5"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

// GraphQLSchema has some basic variables.
type GraphQLSchema struct {
	weaviateGraphQLSchema graphql.Schema
	serverConfig          *config.WeaviateConfig
	serverSchema          *schema.WeaviateSchema
	dbConnector           dbconnector.DatabaseConnector
	usedKey               models.KeyTokenGetResponse
	messaging             *messages.Messaging
	thingsDataLoader      *dataloader.Loader
	context               context.Context
}

// NewGraphQLSchema create a new schema object
func NewGraphQLSchema(databaseConnector dbconnector.DatabaseConnector, serverConfig *config.WeaviateConfig, serverSchema *schema.WeaviateSchema, m *messages.Messaging) *GraphQLSchema {
	// Initializing the schema and set its variables
	gqls := new(GraphQLSchema)
	gqls.dbConnector = databaseConnector
	gqls.serverConfig = serverConfig
	gqls.serverSchema = serverSchema
	gqls.messaging = m
	gqls.context = context.Background()

	///// RESOLVER DATALOADER TEST
	// setup batch function
	batchFn := func(ctx context.Context, keys dataloader.Keys) []*dataloader.Result {
		results := []*dataloader.Result{}
		// do some aync work to get data for specified keys
		// append to this list resolved values
		things, err := func(keys dataloader.Keys) (things *models.ThingsListResponse, err error) {
			things = &models.ThingsListResponse{}

			UUIDs := []strfmt.UUID{}

			for _, v := range keys {
				UUIDs = append(UUIDs, strfmt.UUID(v.String()))
			}

			err = databaseConnector.GetThings(UUIDs, things)

			return
		}(keys)

		for _, v := range things.Things {
			results = append(results, &dataloader.Result{v, err})
		}
		return results
	}

	// create Loader with an in-memory cache
	gqls.thingsDataLoader = dataloader.NewBatchedLoader(
		batchFn,
		dataloader.WithWait(50*time.Millisecond),
		dataloader.WithBatchCapacity(100),
	)
	//////// END DATALOADERTEST

	// Return the schema, note that InitSchema has to be runned before this could be used
	return gqls
}

var thingType *graphql.Object

// SetKey sets the key that is used for the latest request
func (f *GraphQLSchema) SetKey(key models.KeyTokenGetResponse) {
	f.usedKey = key
}

// GetGraphQLSchema returns the schema if it is set
func (f *GraphQLSchema) GetGraphQLSchema() (graphql.Schema, error) {
	defer f.messaging.TimeTrack(time.Now())

	// Return the schema, note that InitSchema has to be runned before this could be used
	if &f.weaviateGraphQLSchema == nil {
		return graphql.Schema{}, errors.New("schema is not initialized, perhaps you forget to run 'InitSchema'")
	}

	return f.weaviateGraphQLSchema, nil
}

// InitSchema the GraphQL schema
func (f *GraphQLSchema) InitSchema() error {
	defer f.messaging.TimeTrack(time.Now())

	// Create the interface to which all objects (Key, Thing and Action) must comply
	objectInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name:        "WeaviateObject",
		Description: "An object in the Weaviate database",
		// Add the mandatory fields for this interface
		Fields: graphql.Fields{
			"uuid": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The id of the object.",
			},
		},
	})

	// Create the interface to which all schema-objects (Thing and Action) must comply
	schemaInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name:        "WeaviateSchemaObject",
		Description: "An object that has to commit to weaviate's Thing or Action schema.",
		// Add the mandatory fields for this interface
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
				Type:        graphql.NewNonNull(graphql.Float),
				Description: "The creation time of the object.",
			},
			"lastUpdateTimeUnix": &graphql.Field{
				Type:        graphql.Float,
				Description: "The last update time of the object.",
			},
		},
	})

	// Create the interface to which all lists (Thing and Action) must comply
	listInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name:        "WeaviateListObject",
		Description: "A list of objects in the Weaviate database",
		// Add the mandatory fields for this interface
		Fields: graphql.Fields{
			"totalResults": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Int),
				Description: "The totalResults that could be found in the database.",
			},
		},
	})

	// The keyType which all single key-responses will use
	keyType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "Key",
		Description: "A key from the Weaviate database.",
		Fields: graphql.Fields{
			"uuid": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The id of the key.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Key Response
					if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
						return key.KeyID, nil
					}
					return nil, nil
				},
			},
			"token": &graphql.Field{
				Type:        graphql.String,
				Description: "The token of the key.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Key Response
					if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
						// Return incl token if it is own key, otherwise do not return
						if f.usedKey.Token != key.Token {
							return nil, graphql.NewLocatedError(
								errors.New("the asked token is not accessible for the given API-token"),
								graphql.FieldASTsToNodeASTs(p.Info.FieldASTs),
							)
						}

						return key.Token, nil
					}
					return nil, nil
				},
			},
			"email": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The email of the key.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Key Response
					if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
						return key.Email, nil
					}
					return nil, nil
				},
			},
			"ipOrigin": &graphql.Field{
				Type:        graphql.NewList(graphql.String),
				Description: "The allowed ip-origins of the key.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Key Response
					if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
						return key.IPOrigin, nil
					}
					return []string{}, nil
				},
			},
			"keyExpiresUnix": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Float),
				Description: "The unix timestamp of when the key expires.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Key Response
					if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
						return float64(key.KeyExpiresUnix), nil
					}
					return nil, nil
				},
			},
			"read": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Boolean),
				Description: "Whether the key has read-rights.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Key Response
					if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
						return key.Read, nil
					}
					return nil, nil
				},
			},
			"execute": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Boolean),
				Description: "Whether the key has execute-rights.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Key Response
					if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
						return key.Execute, nil
					}
					return nil, nil
				},
			},
			"write": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Boolean),
				Description: "Whether the key has write-rights.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Key Response
					if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
						return key.Write, nil
					}
					return nil, nil
				},
			},
			"delete": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Boolean),
				Description: "Whether the key has delete-rights.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Key Response
					if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
						return key.Delete, nil
					}
					return nil, nil
				},
			},
		},
		// The interfaces this object satifies
		Interfaces: []*graphql.Interface{
			objectInterface,
		},
	})

	// Add to schemaInterface here, because when initializing the interface, keyType does not exist.
	schemaInterface.AddFieldConfig("key", &graphql.Field{
		Type:        keyType,
		Description: "The key which is the owner of the object.",
	})

	// Add to keyType here, because when initializing the keyType, keyType itself does not exist.
	keyType.AddFieldConfig("children", &graphql.Field{
		Type:        graphql.NewNonNull(graphql.NewList(keyType)),
		Description: "Get all children of this key.",
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// Resolve the data from the Key Response
			if key, ok := p.Source.(*models.KeyTokenGetResponse); ok {
				// Init the respons as an array of keys
				children := []*models.KeyTokenGetResponse{}

				// Get the key-ID from the object
				keyUUID := key.KeyID

				// Get children UUID's from the database
				err := f.dbConnector.GetKeyChildren(keyUUID, &children)

				// If an error is given, return the empty array
				if err != nil {
					return children, err
				}

				// Return children without error
				return children, nil
			}
			return []interface{}{}, nil
		},
	})

	// The thingType which all single thing-responses will use
	thingType = graphql.NewObject(graphql.ObjectConfig{
		Name:        "Thing",
		Description: "A thing from the Weaviate database, based on the Weaviate schema.",
		Fields: graphql.Fields{
			"atContext": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The context on which the object is in.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Thing Response
					if thing, ok := p.Source.(*models.ThingGetResponse); ok {
						return thing.AtContext, nil
					}
					return nil, nil
				},
			},
			"atClass": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The class of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Thing Response
					if thing, ok := p.Source.(*models.ThingGetResponse); ok {
						return thing.AtClass, nil
					}
					return nil, nil
				},
			},
			"creationTimeUnix": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Float),
				Description: "The creation time of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Thing Response
					if thing, ok := p.Source.(*models.ThingGetResponse); ok {
						return float64(thing.CreationTimeUnix), nil
					}
					return nil, nil
				},
			},
			"lastUpdateTimeUnix": &graphql.Field{
				Type:        graphql.Float,
				Description: "The last update time of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Thing Response
					if thing, ok := p.Source.(*models.ThingGetResponse); ok {
						return float64(thing.LastUpdateTimeUnix), nil
					}
					return nil, nil
				},
			},
			"uuid": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The id of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Thing Response
					if thing, ok := p.Source.(*models.ThingGetResponse); ok {
						return thing.ThingID, nil
					}

					return nil, nil
				},
			},
			"key": &graphql.Field{
				Type:        keyType,
				Description: "The key which is the owner of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					keyResponse := &models.KeyTokenGetResponse{}
					if thing, ok := p.Source.(*models.ThingGetResponse); ok {
						// If no UUID is given, just return nil
						if thing.Key.NrDollarCref == "" {
							return nil, nil
						}

						// Do a new request with the key from the reference object
						err := f.resolveCrossRef(p.Info.FieldASTs, thing.Key, keyResponse)

						if err != nil {
							return keyResponse, err
						}

						// Return found
						return keyResponse, nil
					}

					// Return nothing
					return nil, nil
				},
			},
		},
		// The interfaces this object satifies
		Interfaces: []*graphql.Interface{
			schemaInterface,
			objectInterface,
		},
	})

	// The thingListType which wil be used when returning a list
	thingListType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "ThingList",
		Description: "Things listed from the Weaviate database belonging to the user, based on the Weaviate schema.",
		Fields: graphql.Fields{
			"things": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.NewList(thingType)),
				Description: "The actual things",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Things Response
					if things, ok := p.Source.(*models.ThingsListResponse); ok {
						return things.Things, nil
					}
					return []*models.ThingGetResponse{}, nil
				},
			},
			"totalResults": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Int),
				Description: "The total amount of things without pagination",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Things Response
					if things, ok := p.Source.(*models.ThingsListResponse); ok {
						return things.TotalResults, nil
					}
					return nil, nil
				},
			},
		},
		// The interfaces this object satifies
		Interfaces: []*graphql.Interface{
			listInterface,
		},
	})

	// The objectSubjectType which is used in the ActionType only to assign the object and subject things
	objectSubjectType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "ObjectSubject",
		Description: "An object / subject, part of action. These are both of type Thing.",
		Fields: graphql.Fields{
			"object": &graphql.Field{
				Type:        thingType,
				Description: "The thing which is the object of this action.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					thingResponse := &models.ThingGetResponse{}
					if ref, ok := p.Source.(*models.ObjectSubject); ok {
						// If no UUID is given, just return nil
						if ref.Object.NrDollarCref == "" {
							return nil, nil
						}

						// Evaluate the Cross reference
						err := f.resolveCrossRef(p.Info.FieldASTs, ref.Object, thingResponse)

						if err != nil {
							return thingResponse, err
						}

						// Return found
						return thingResponse, nil
					}
					// Return nothing
					return nil, nil
				},
			},
			"subject": &graphql.Field{
				Type:        thingType,
				Description: "The thing which is the subject of this action.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					thingResponse := &models.ThingGetResponse{}
					if ref, ok := p.Source.(*models.ObjectSubject); ok {
						// If no UUID is given, just return nil
						if ref.Subject.NrDollarCref == "" {
							return nil, nil
						}

						// Do a new request with the thing from the reference object
						err := f.resolveCrossRef(p.Info.FieldASTs, ref.Subject, thingResponse)
						if err != nil {
							return thingResponse, err
						}

						// Return found
						return thingResponse, nil
					}
					// Return nothing
					return nil, nil
				},
			},
		},
	})

	// The actionType which all single action-responses will use
	actionType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "Action",
		Description: "A action from the Weaviate database, based on the Weaviate schema.",
		Fields: graphql.Fields{
			"atContext": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The context on which the object is in.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Action Response
					if action, ok := p.Source.(*models.ActionGetResponse); ok {
						return action.AtContext, nil
					}
					return nil, nil
				},
			},
			"atClass": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The class of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Action Response
					if action, ok := p.Source.(*models.ActionGetResponse); ok {
						return action.AtClass, nil
					}
					return nil, nil
				},
			},
			"creationTimeUnix": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Float),
				Description: "The creation time of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Action Response
					if action, ok := p.Source.(*models.ActionGetResponse); ok {
						return float64(action.CreationTimeUnix), nil
					}
					return nil, nil
				},
			},
			"lastUpdateTimeUnix": &graphql.Field{
				Type:        graphql.Float,
				Description: "The last update time of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Action Response
					if action, ok := p.Source.(*models.ActionGetResponse); ok {
						return float64(action.LastUpdateTimeUnix), nil
					}
					return nil, nil
				},
			},
			"uuid": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "The id of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Action Response
					if action, ok := p.Source.(*models.ActionGetResponse); ok {
						return action.ActionID, nil
					}
					return nil, nil
				},
			},
			"things": &graphql.Field{
				Type:        objectSubjectType,
				Description: "The thing which is the object of this action.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Action Response
					if action, ok := p.Source.(*models.ActionGetResponse); ok {
						return action.Things, nil
					}
					return nil, nil
				},
			},
			"key": &graphql.Field{
				Type:        keyType,
				Description: "The key which is the owner of the object.",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					keyResponse := &models.KeyTokenGetResponse{}
					if action, ok := p.Source.(*models.ActionGetResponse); ok {
						// If no UUID is given, just return nil
						if action.Key.NrDollarCref == "" {
							return nil, nil
						}

						// Do a new request with the key from the reference object
						err := f.resolveCrossRef(p.Info.FieldASTs, action.Key, keyResponse)
						if err != nil {
							return keyResponse, err
						}

						// Return found
						return keyResponse, nil
					}

					// Return nothing
					return nil, nil
				},
			},
		},
		// The interfaces this object satifies
		Interfaces: []*graphql.Interface{
			schemaInterface,
			objectInterface,
		},
	})

	// The actionListType which wil be used when returning a list
	actionListType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "ActionList",
		Description: "Actions listed from the Weaviate database belonging to the given thing and user, based on the Weaviate schema.",
		Fields: graphql.Fields{
			"actions": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.NewList(actionType)),
				Description: "The actual actions",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Actions Response
					if actions, ok := p.Source.(*models.ActionsListResponse); ok {
						return actions.Actions, nil
					}
					return []*models.ActionGetResponse{}, nil
				},
			},
			"totalResults": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.Int),
				Description: "The total amount of actions without pagination",
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Resolve the data from the Actions Response
					if actions, ok := p.Source.(*models.ActionsListResponse); ok {
						return actions.TotalResults, nil
					}
					return nil, nil
				},
			},
		},
		// The interfaces this object satifies
		Interfaces: []*graphql.Interface{
			listInterface,
		},
	})

	// Init the action list field
	actionListField := &graphql.Field{
		Type:        actionListType,
		Description: "The actions belonging to this thing, sorted on creation time..",
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description:  "First X items from the given offset, when none given, it will be " + string(f.serverConfig.Environment.Limit) + ".",
				Type:         graphql.Int,
				DefaultValue: f.serverConfig.Environment.Limit,
			},
			"offset": &graphql.ArgumentConfig{
				Description:  "Offset from the most recent item.",
				Type:         graphql.Int,
				DefaultValue: 0,
			},
			"schema": &graphql.ArgumentConfig{
				Description: "Schema filter options.",
				Type:        graphql.String,
			},
			"class": &graphql.ArgumentConfig{
				Description: "Class filter options.",
				Type:        graphql.String,
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// Resolve the data from the Thing Response
			if thing, ok := p.Source.(*models.ThingGetResponse); ok {
				// Initialize the thing response
				actionsResponse := &models.ActionsListResponse{}

				// Return if the ThingID is empty
				if thing.ThingID == "" {
					return actionsResponse, nil
				}

				// Get the pagination from the arguments
				var first int
				if p.Args["first"] != nil {
					first = p.Args["first"].(int)
				}

				var offset int
				if p.Args["offset"] != nil {
					offset = p.Args["offset"].(int)
				}

				// Get the filter options for schema, init the options
				wheres := []*connutils.WhereQuery{}

				// Check whether the schema var is filled in
				if p.Args["schema"] != nil {
					// Rewrite the string to structs
					where, err := connutils.WhereStringToStruct("schema", p.Args["schema"].(string))

					// If error is given, return it
					if err != nil {
						return actionsResponse, err
					}

					// Append wheres to the list
					wheres = append(wheres, &where)
				}

				// Check whether the class var is filled in
				if p.Args["class"] != nil {
					// Rewrite the string to structs
					where, err := connutils.WhereStringToStruct("atClass", p.Args["class"].(string))

					// If error is given, return it
					if err != nil {
						return actionsResponse, err
					}

					// Append wheres to the list
					wheres = append(wheres, &where)
				}

				// Do a request on the database to get the Thing
				err := f.dbConnector.ListActions(thing.ThingID, first, offset, wheres, actionsResponse)

				// Return error, if needed.
				if err != nil && actionsResponse.TotalResults == 0 {
					return actionsResponse, err
				}
				return actionsResponse, nil
			}

			return []interface{}{}, nil
		},
	}

	// Add to thingType here, because when initializing the thingType, actionListType does not exist.
	thingType.AddFieldConfig("actions", actionListField)

	// Build the schema fields in a separate function based on the given schema (thing)
	thingSchemaFields, errT := f.buildFieldsBySchema(f.serverSchema.ThingSchema.Schema)

	// Return error if it has one
	if errT != nil {
		return errT
	}

	// Build the schema fields in a separate function based on the given schema (action)
	actionSchemaFields, errA := f.buildFieldsBySchema(f.serverSchema.ActionSchema.Schema)

	// Return error if it has one
	if errA != nil {
		return errA
	}

	// The thingsSchemaType in wich all the possible fields are addes
	thingSchemaType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "ThingSchema",
		Description: "Schema type for things from the database, based on the Weaviate thing-schema.",
		Fields:      thingSchemaFields,
	})

	// The actionsSchemaType in wich all the possible fields are addes
	actionSchemaType := graphql.NewObject(graphql.ObjectConfig{
		Name:        "ActionSchema",
		Description: "Schema type for actions from the database, based on the Weaviate action-schema.",
		Fields:      actionSchemaFields,
	})

	// Add the schema, as in this position all needed variables are set
	thingType.AddFieldConfig("schema", &graphql.Field{
		Type:        thingSchemaType,
		Description: "The values filled in the schema properties.",
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// Resolve the data from the Thing Response
			if thing, ok := p.Source.(*models.ThingGetResponse); ok {
				return thing.Schema, nil
			}

			return nil, nil
		},
	})

	// Add the schema, as in this position all needed variables are set
	actionType.AddFieldConfig("schema", &graphql.Field{
		Type:        actionSchemaType,
		Description: "The values filled in the schema properties.",
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// Resolve the data from the Action Response
			if action, ok := p.Source.(*models.ActionGetResponse); ok {
				return action.Schema, nil
			}

			return nil, nil
		},
	})

	// The queryType is the main type in the tree, here does the query resolving start
	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			// Query to get a single thing
			"thing": &graphql.Field{
				Description: "Gets a single thing based on the given ID.",
				Type:        thingType,
				Args: graphql.FieldConfigArgument{
					"uuid": &graphql.ArgumentConfig{
						Description: "UUID of the thing",
						Type:        graphql.NewNonNull(graphql.String),
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Initialize the thing response
					thingResponse := &models.ThingGetResponse{}

					// Get the ID from the arguments
					UUID := strfmt.UUID(p.Args["uuid"].(string))

					// Do a request on the database to get the Thing
					err := f.dbConnector.GetThing(UUID, thingResponse)

					if err != nil {
						return thingResponse, err
					}
					return thingResponse, nil
				},
			},
			// Query to get a list of things
			"listThings": &graphql.Field{
				Description: "Lists Things belonging to the used key, sorted on creation time.",
				Type:        thingListType,
				Args: graphql.FieldConfigArgument{
					"first": &graphql.ArgumentConfig{
						Description:  "First X items from the given offset, when none given, it will be " + string(f.serverConfig.Environment.Limit) + ".",
						Type:         graphql.Int,
						DefaultValue: f.serverConfig.Environment.Limit,
					},
					"offset": &graphql.ArgumentConfig{
						Description:  "Offset from the most recent item.",
						Type:         graphql.Int,
						DefaultValue: 0,
					},
					"schema": &graphql.ArgumentConfig{
						Description: "Schema filter options.",
						Type:        graphql.String,
					},
					"class": &graphql.ArgumentConfig{
						Description: "Class filter options.",
						Type:        graphql.String,
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					defer f.messaging.TimeTrack(time.Now())
					// Initialize the thing response
					thingsResponse := &models.ThingsListResponse{}

					// Get the pagination from the arguments
					var first int
					if p.Args["first"] != nil {
						first = p.Args["first"].(int)
					}

					var offset int
					if p.Args["offset"] != nil {
						offset = p.Args["offset"].(int)
					}

					// Get the filter options for schema, init the options
					wheres := []*connutils.WhereQuery{}

					// Check whether the schema var is filled in
					if p.Args["schema"] != nil {
						// Rewrite the string to structs
						where, err := connutils.WhereStringToStruct("schema", p.Args["schema"].(string))

						// If error is given, return it
						if err != nil {
							return thingsResponse, err
						}

						// Append wheres to the list
						wheres = append(wheres, &where)
					}

					// Check whether the class var is filled in
					if p.Args["class"] != nil {
						// Rewrite the string to structs
						where, err := connutils.WhereStringToStruct("atClass", p.Args["class"].(string))

						// If error is given, return it
						if err != nil {
							return thingsResponse, err
						}

						// Append wheres to the list
						wheres = append(wheres, &where)
					}

					// Do a request on the database to get the Thing
					err := f.dbConnector.ListThings(first, offset, f.usedKey.KeyID, wheres, thingsResponse)

					// Return error, if needed.
					if err != nil && thingsResponse.TotalResults == 0 {
						return thingsResponse, err
					}
					return thingsResponse, nil
				},
			},
			// Query to get a single action
			"action": &graphql.Field{
				Description: "Gets a single action based on the given ID.",
				Type:        actionType,
				Args: graphql.FieldConfigArgument{
					"uuid": &graphql.ArgumentConfig{
						Description: "UUID of the action",
						Type:        graphql.NewNonNull(graphql.String),
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Initialize the action response
					actionResponse := &models.ActionGetResponse{}
					actionResponse.Schema = map[string]models.JSONObject{}
					actionResponse.Things = &models.ObjectSubject{}

					// Get the ID from the arguments
					UUID := strfmt.UUID(p.Args["uuid"].(string))

					// Do a request on the database to get the Action
					err := f.dbConnector.GetAction(UUID, actionResponse)
					if err != nil {
						return actionResponse, err
					}
					return actionResponse, nil
				},
			},
			// Query to get a single key
			"key": &graphql.Field{
				Description: "Gets a single key based on the given ID.",
				Type:        keyType,
				Args: graphql.FieldConfigArgument{
					"uuid": &graphql.ArgumentConfig{
						Description: "UUID of the key",
						Type:        graphql.NewNonNull(graphql.String),
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Initialize the key response
					keyResponse := &models.KeyTokenGetResponse{}

					// Get the ID from the arguments
					UUID := strfmt.UUID(p.Args["uuid"].(string))

					// Do a request on the database to get the Key
					err := f.dbConnector.GetKey(UUID, keyResponse)
					if err != nil {
						return &models.KeyTokenGetResponse{}, err
					}

					return keyResponse, nil
				},
			},
			// Query to get mine single key
			"myKey": &graphql.Field{
				Description: "Gets my single key based on the given token.",
				Type:        keyType,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					// Get the ID from the class' variables
					return &f.usedKey, nil
				},
			},
		},
	})

	// Init error var
	var err error

	// Add the schema to the exported variable.
	f.weaviateGraphQLSchema, err = graphql.NewSchema(graphql.SchemaConfig{
		Query: queryType,
	})

	// Print for logging
	f.messaging.InfoMessage("GraphQL initialisation finished.")

	return err
}

// GetSubQuery returns a new query, which was the subquery
// Can be used with "collectQuery := GetSubQuery(p.Info.FieldASTs[0].SelectionSet.Selections)""
func GetSubQuery(selections []ast.Selection) string {
	// Init an empty string
	collectQuery := ""

	// For every selection, append the name of the field to build up the fields to select in the query
	for _, selection := range selections {
		collectQuery += " " + selection.(*ast.Field).Name.Value

		// Go into the selection when it has sub-selections, concatinate with the brackets
		if selection.(*ast.Field).SelectionSet != nil {
			collectQuery += " { " + GetSubQuery(selection.(*ast.Field).SelectionSet.Selections) + " } "
		}
	}

	// Return the query.
	return collectQuery
}

// buildFieldsBySchema builds a GraphQL fields object for the given fields
func (f *GraphQLSchema) buildFieldsBySchema(ws *models.SemanticSchema) (graphql.Fields, error) {
	defer f.messaging.TimeTrack(time.Now())

	// Init the fields for the return of the function
	fields := graphql.Fields{}

	// Do it for every class
	for _, schemaClass := range ws.Classes {

		// Do it for every property in that class
		for _, schemaProp := range schemaClass.Properties {

			// If the property isn't allready added
			if _, ok := fields[schemaProp.Name]; !ok {
				// Get the datatype
				dt, err := schema.GetPropertyDataType(schemaClass, schemaProp.Name)

				if err != nil {
					return fields, err
				}

				var scalar graphql.Output
				if *dt == schema.DataTypeString {
					scalar = graphql.String
				} else if *dt == schema.DataTypeInt {
					scalar = graphql.Int
				} else if *dt == schema.DataTypeNumber {
					scalar = graphql.Float
				} else if *dt == schema.DataTypeBoolean {
					scalar = graphql.Boolean
				} else if *dt == schema.DataTypeDate {
					scalar = graphql.DateTime
				} else if *dt == schema.DataTypeCRef {
					// TODO: Cref is not always of type 'thing'
					scalar = thingType
				}

				// Make a new field and add it
				fields[schemaProp.Name] = &graphql.Field{
					Name:        schemaProp.Name,
					Type:        scalar,
					Description: fmt.Sprintf("Value of schema property '%s'", schemaProp.Name),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						// Init the interface for the return value
						var rVal interface{}

						// Cast it to an interface
						if schema, ok := p.Source.(map[string]interface{}); ok {
							rVal = schema[p.Info.FieldName]
						}

						// Return value based on data type
						if *dt == schema.DataTypeString {
							if value, ok := rVal.(string); ok {
								return value, nil
							}
							return nil, nil
						} else if *dt == schema.DataTypeInt {
							if value, ok := rVal.(int64); ok {
								return value, nil
							}
							return nil, nil
						} else if *dt == schema.DataTypeNumber {
							if value, ok := rVal.(float64); ok {
								return value, nil
							}
							return nil, nil
						} else if *dt == schema.DataTypeBoolean {
							if value, ok := rVal.(bool); ok {
								return value, nil
							}
							return nil, nil
						} else if *dt == schema.DataTypeDate {
							if value, ok := rVal.(string); ok {
								return graphql.DateTime.ParseValue(value), nil
							}
							return nil, nil
						} else if *dt == schema.DataTypeCRef {
							// TODO: Cref is not always of type 'thing'
							// Data type is not a value, but an cref object
							thingResponse := &models.ThingGetResponse{}

							if value, ok := rVal.(*models.SingleRef); ok {
								// If no UUID is given, just return nil
								if value.NrDollarCref == "" {
									return nil, nil
								}

								// Evaluate the Cross reference
								err := f.resolveCrossRef(p.Info.FieldASTs, value, thingResponse)
								if err != nil {
									return thingResponse, err
								}

								// Return found
								return thingResponse, nil
							} else {
								return nil, errors.New("Invalid type for CREF")
							}

							// // Return nothing
							// return nil, nil
						}

						// Return parsing error
						err := errors.New("error parsing schema")

						// Return error for GraphQL
						return nil, gqlerrors.NewError(
							err.Error(),
							gqlerrors.FieldASTsToNodeASTs(p.Info.FieldASTs),
							err.Error(),
							nil,
							[]int{},
							err,
						)
					},
				}
			}
		}
	}

	return fields, nil
}

// resolveCrossRef Resolves a Cross reference
func (f *GraphQLSchema) resolveCrossRef(fields []*ast.Field, cref *models.SingleRef, objectLoaded interface{}) error {
	defer f.messaging.TimeTrack(time.Now())

	var err error

	// TODO enable
	// if f.serverConfig.GetHostAddress() != *cref.LocationURL {
	// 	// Return an error because you want to connect to other server. You need an license.
	// 	err = errors.New("a license for connection to another Weaviate-instance is required in order to resolve this query further")
	// } else {
	// Check whether the request has to be done for key, thing or action types
	if cref.Type == "Thing" {
		// DATALOADER TEST
		var result interface{}
		// StringKey is a convenience method that make wraps string to implement `Key` interface
		thunk := f.thingsDataLoader.Load(f.context, dataloader.StringKey(string(cref.NrDollarCref)))
		result, err = thunk()

		objectLoaded.(*models.ThingGetResponse).Thing = result.(*models.ThingGetResponse).Thing
		objectLoaded.(*models.ThingGetResponse).ThingID = result.(*models.ThingGetResponse).ThingID
		// END DATALOADER TEST

		// err = f.dbConnector.GetThing(cref.NrDollarCref, objectLoaded.(*models.ThingGetResponse))
	} else if cref.Type == "Action" {
		err = f.dbConnector.GetAction(cref.NrDollarCref, objectLoaded.(*models.ActionGetResponse))
	} else if cref.Type == "Key" {
		err = f.dbConnector.GetKey(cref.NrDollarCref, objectLoaded.(*models.KeyTokenGetResponse))
	} else {
		err = fmt.Errorf("can't resolve the given type '%s'", cref.Type)
	}
	// }

	if err != nil {
		stack := err.Error()
		return gqlerrors.NewError(
			err.Error(),
			gqlerrors.FieldASTsToNodeASTs(fields),
			stack,
			nil,
			[]int{},
			err,
		)
	}

	return nil
}
