/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

// Package restapi with all rest API functions.
package restapi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"

	errors "github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	graceful "github.com/tylerb/graceful"
	"google.golang.org/grpc/grpclog"

	"github.com/creativesoftwarefdn/weaviate/auth"
	"github.com/creativesoftwarefdn/weaviate/broker"
	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/cassandra"
	"github.com/creativesoftwarefdn/weaviate/connectors/dataloader"
	"github.com/creativesoftwarefdn/weaviate/connectors/foobar"
	"github.com/creativesoftwarefdn/weaviate/connectors/kvcache"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

const pageOverride int = 1

var connectorOptionGroup *swag.CommandLineOptionsGroup
var databaseSchema schema.WeaviateSchema
var serverConfig *config.WeaviateConfig
var dbConnector dbconnector.DatabaseConnector
var graphQLSchema *graphqlapi.GraphQLSchema
var messaging *messages.Messaging

type keyTokenHeader struct {
	Key   strfmt.UUID `json:"key"`
	Token strfmt.UUID `json:"token"`
}

func init() {
	discard := ioutil.Discard
	myGRPCLogger := log.New(discard, "", log.LstdFlags)
	grpclog.SetLogger(myGRPCLogger)

	// Create temp folder if it does not exist
	tempFolder := "temp"
	if _, err := os.Stat(tempFolder); os.IsNotExist(err) {
		messaging.InfoMessage("Temp folder created...")
		os.Mkdir(tempFolder, 0766)
	}
}

// getLimit returns the maximized limit
func getLimit(paramMaxResults *int64) int {
	maxResults := serverConfig.Environment.Limit
	// Get the max results from params, if exists
	if paramMaxResults != nil {
		maxResults = *paramMaxResults
	}

	// Max results form URL, otherwise max = config.Limit.
	return int(math.Min(float64(maxResults), float64(serverConfig.Environment.Limit)))
}

// getPage returns the page if set
func getPage(paramPage *int64) int {
	page := int64(pageOverride)
	// Get the page from params, if exists
	if paramPage != nil {
		page = *paramPage
	}

	// Page form URL, otherwise max = config.Limit.
	return int(page)
}

func generateMultipleRefObject(keyIDs []strfmt.UUID) models.MultipleRef {
	// Init the response
	refs := models.MultipleRef{}

	// Init localhost
	url := serverConfig.GetHostAddress()

	// Generate SingleRefs
	for _, keyID := range keyIDs {
		refs = append(refs, &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: keyID,
			Type:         string(connutils.RefTypeKey),
		})
	}

	return refs
}

func deleteKey(ctx context.Context, databaseConnector dbconnector.DatabaseConnector, parentUUID strfmt.UUID) {
	// Find its children
	var allIDs []strfmt.UUID

	// Get all the children to remove
	allIDs, _ = auth.GetKeyChildrenUUIDs(ctx, databaseConnector, parentUUID, false, allIDs, 0, 0)

	// Append the children to the parent UUIDs to remove all
	allIDs = append(allIDs, parentUUID)

	// Delete for every child
	for _, keyID := range allIDs {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get the key to delete
		dbConnector.GetKey(ctx, keyID, &keyResponse)

		databaseConnector.DeleteKey(ctx, &keyResponse.Key, keyID)
	}
}

// GetAllConnectors contains all available connectors
func GetAllConnectors() []dbconnector.DatabaseConnector {
	// Set all existing connectors
	connectors := []dbconnector.DatabaseConnector{
		&foobar.Foobar{},
		&cassandra.Cassandra{},
	}

	return connectors
}

// GetAllCacheConnectors contains all available cache-connectors
func GetAllCacheConnectors() []dbconnector.CacheConnector {
	// Set all existing connectors
	connectors := []dbconnector.CacheConnector{
		&kvcache.KVCache{},
		&dataloader.DataLoader{},
	}

	return connectors
}

// CreateDatabaseConnector gets the database connector by name from config
func CreateDatabaseConnector(env *config.Environment) dbconnector.DatabaseConnector {
	// Get all connectors
	connectors := GetAllConnectors()
	cacheConnectors := GetAllCacheConnectors()

	// Init the db-connector variable
	var connector dbconnector.DatabaseConnector

	// Loop through all connectors and determine its name
	for _, c := range connectors {
		if c.GetName() == env.Database.Name {
			messaging.InfoMessage(fmt.Sprintf("Using database '%s'", env.Database.Name))
			connector = c
			break
		}
	}

	// Loop through all cache-connectors and determine its name
	for _, cc := range cacheConnectors {
		if cc.GetName() == env.Cache.Name {
			messaging.InfoMessage(fmt.Sprintf("Using cache layer '%s'", env.Cache.Name))
			cc.SetDatabaseConnector(connector)
			connector = cc
			break
		}
	}

	return connector
}

func configureFlags(api *operations.WeaviateAPI) {
	connectorOptionGroup = config.GetConfigOptionGroup()

	api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{
		*connectorOptionGroup,
	}
}

// createErrorResponseObject is a common function to create an error response
func createErrorResponseObject(message string) *models.ErrorResponse {
	// Initialize return value
	er := &models.ErrorResponse{}

	// Fill the error with the message
	er.Error = &models.ErrorResponseError{
		Message: message,
	}

	return er
}

func headerAPIKeyHandling(ctx context.Context, keyToken string) (*models.KeyTokenGetResponse, error) {
	// Convert JSON string to struct
	kth := keyTokenHeader{}
	json.Unmarshal([]byte(keyToken), &kth)

	// Validate both headers
	if kth.Key == "" || kth.Token == "" {
		return nil, errors.New(401, connutils.StaticMissingHeader)
	}

	// Create key
	validatedKey := models.KeyGetResponse{}

	// Check if the user has access, true if yes
	hashed, err := dbConnector.ValidateToken(ctx, kth.Key, &validatedKey)

	// Error printing
	if err != nil {
		return nil, errors.New(401, err.Error())
	}

	// Check token
	if !connutils.TokenHashCompare(hashed, kth.Token) {
		return nil, errors.New(401, connutils.StaticInvalidToken)
	}

	// Validate the key on expiry time
	currentUnix := connutils.NowUnix()

	if validatedKey.KeyExpiresUnix != -1 && validatedKey.KeyExpiresUnix < currentUnix {
		return nil, errors.New(401, connutils.StaticKeyExpired)
	}

	// Init response object
	validatedKeyToken := models.KeyTokenGetResponse{}
	validatedKeyToken.KeyGetResponse = validatedKey
	validatedKeyToken.Token = kth.Token

	// key is valid, next step is allowing per Handler handling
	return &validatedKeyToken, nil
}


// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix"
func configureServer(s *graceful.Server, scheme, addr string) {
	// Create message service
	messaging = &messages.Messaging{}

	// Load the config using the flags
	serverConfig = &config.WeaviateConfig{}
	err := serverConfig.LoadConfig(connectorOptionGroup, messaging)

	// Add properties to the config
	serverConfig.Hostname = addr
	serverConfig.Scheme = scheme

	// Fatal error loading config file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	// Load the schema using the config
	databaseSchema = schema.WeaviateSchema{}
	err = databaseSchema.LoadSchema(&serverConfig.Environment, messaging)

	// Fatal error loading schema file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	// Connect to MQTT via Broker
	weaviateBroker.ConnectToMqtt(serverConfig.Environment.Broker.Host, serverConfig.Environment.Broker.Port)

	// Create the database connector usint the config
	dbConnector = CreateDatabaseConnector(&serverConfig.Environment)

	// Error the system when the database connector returns no connector
	if dbConnector == nil {
		messaging.ExitError(78, "database with the name '"+serverConfig.Environment.Database.Name+"' couldn't be loaded from the config")
	}

	// Set connector vars
	err = dbConnector.SetConfig(&serverConfig.Environment)
	// Fatal error loading config file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	err = dbConnector.SetSchema(&databaseSchema)
	// Fatal error loading schema file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	err = dbConnector.SetMessaging(messaging)
	// Fatal error setting messaging
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	dbConnector.SetServerAddress(serverConfig.GetHostAddress())

	// connect the database
	errConnect := dbConnector.Connect()
	if errConnect != nil {
		messaging.ExitError(1, "database with the name '"+serverConfig.Environment.Database.Name+"' gave an error when connecting: "+errConnect.Error())
	}

	// init the database
	var errInit error
	errInit = dbConnector.Init()
	if errInit != nil {
		messaging.ExitError(1, "database with the name '"+serverConfig.Environment.Database.Name+"' gave an error when initializing: "+errInit.Error())
	}

	// Init the GraphQL schema
	graphQLSchema = graphqlapi.NewGraphQLSchema(dbConnector, serverConfig, &databaseSchema, messaging)

	// Error init
	errInitGQL := graphQLSchema.InitSchema()
	if errInitGQL != nil {
		messaging.ExitError(1, "GraphQL schema initialization gave an error when initializing: "+errInitGQL.Error())
	}
}

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation
func setupMiddlewares(handler http.Handler) http.Handler {
	// Rewrite / workaround because of issue with handling two API keys
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kth := keyTokenHeader{
			Key:   strfmt.UUID(r.Header.Get("X-API-KEY")),
			Token: strfmt.UUID(r.Header.Get("X-API-TOKEN")),
		}
		jkth, _ := json.Marshal(kth)
		r.Header.Set("X-API-KEY", string(jkth))
		r.Header.Set("X-API-TOKEN", string(jkth))

		messaging.InfoMessage("generated both headers X-API-KEY and X-API-TOKEN")

		ctx := r.Context()
		ctx, err := dbConnector.Attach(ctx)

		if err != nil {
			messaging.ExitError(1, "database or cache gave an error when attaching context: "+err.Error())
		}

		handler.ServeHTTP(w, r.WithContext(ctx))
	})
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
func setupGlobalMiddleware(handler http.Handler) http.Handler {
	return handler
}
