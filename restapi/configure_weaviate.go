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
	"net/http"
	"os"

	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/rs/cors"
	"google.golang.org/grpc/grpclog"

	"github.com/creativesoftwarefdn/weaviate/broker"
	"github.com/creativesoftwarefdn/weaviate/config"
	dbconnector "github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/schema"

	libcontextionary "github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/graphiql"
	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
)

const pageOverride int = 1

var connectorOptionGroup *swag.CommandLineOptionsGroup
var databaseSchema schema.WeaviateSchema
var contextionary *libcontextionary.Contextionary
var network libnetwork.Network
var serverConfig *config.WeaviateConfig
var dbConnector dbconnector.DatabaseConnector
var graphQL graphqlapi.GraphQL
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

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	/*
	 * HANDLE X-API-KEY
	 */
	// Applies when the "X-API-KEY" header is set
	api.APIKeyAuth = func(token string) (interface{}, error) {
		ctx := context.Background()
		return headerAPIKeyHandling(ctx, token)
	}

	/*
	 * HANDLE X-API-TOKEN
	 */
	// Applies when the "X-API-TOKEN" header is set
	api.APITokenAuth = func(token string) (interface{}, error) {
		ctx := context.Background()
		return headerAPIKeyHandling(ctx, token)
	}

	/*
	 * HANDLE ACTIONS
	 */
	configureAPIActions(api)

	/*
	 * HANDLE KEYS
	 */
	configureAPIKeys(api)

	/*
	 * HANDLE THINGS
	 */
	configureAPIThings(api)

	/*
	 * HANDLE GRAPHQL
	 */
	configureAPIGraphql(api)

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix"
func configureServer(s *http.Server, scheme, addr string) {
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

	loadContextionary()

	connectToNetwork()

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

	graphQL, err = graphqlapi.CreateSchema(&dbConnector, serverConfig, &databaseSchema, messaging)

	if err != nil {
		messaging.ExitError(1, "GraphQL schema initialization gave an error when initializing: "+err.Error())
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
	handleCORS := cors.Default().Handler
	handler = handleCORS(handler)
	handler = graphiql.AddMiddleware(handler)

	return addLogging(handler)
}

// This function loads the Contextionary database, and creates
// an in-memory database for the centroids of the classes / properties in the Schema.
func loadContextionary() {
	// First load the file backed contextionary
	if serverConfig.Environment.Contextionary.KNNFile == "" {
		messaging.ExitError(78, "Contextionary KNN file not specified")
	}

	if serverConfig.Environment.Contextionary.IDXFile == "" {
		messaging.ExitError(78, "Contextionary IDX file not specified")
	}

	mmaped_contextionary, err := libcontextionary.LoadVectorFromDisk(serverConfig.Environment.Contextionary.KNNFile, serverConfig.Environment.Contextionary.IDXFile)

	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not load Contextionary; %+v", err))
	}

	messaging.InfoMessage("Contextionary loaded from disk")

	// Now create the in-memory contextionary based on the classes / properties.
	in_memory_contextionary, err := databaseSchema.BuildInMemoryContextionaryFromSchema(mmaped_contextionary)
	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not build in-memory contextionary from schema; %+v", err))
	}

	// Combine contextionaries
	contextionaries := []libcontextionary.Contextionary{*in_memory_contextionary, *mmaped_contextionary}
	combined, err := libcontextionary.CombineVectorIndices(contextionaries)

	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not combine the contextionary database with the in-memory generated contextionary; %+v", err))
	}

	messaging.InfoMessage("Contextionary extended with names in the schema")

	// urgh, go.
	x := libcontextionary.Contextionary(combined)
	contextionary = &x

	// whoop!
}

func connectToNetwork() {
	if serverConfig.Environment.Network == nil {
		messaging.InfoMessage(fmt.Sprintf("No network configured, not joining one"))
		network = libnetwork.FakeNetwork{}
	} else {
		genesis_url := strfmt.URI(serverConfig.Environment.Network.GenesisURL)
		public_url := strfmt.URI(serverConfig.Environment.Network.PublicURL)
		peer_name := serverConfig.Environment.Network.PeerName

		messaging.InfoMessage(fmt.Sprintf("Network configured, connecting to Genesis '%v'", genesis_url))
		new_net, err := libnetwork.BootstrapNetwork(messaging, genesis_url, public_url, peer_name)
		if err != nil {
			messaging.ExitError(78, fmt.Sprintf("Could not connect to network! Reason: %+v", err))
		} else {
			network = *new_net
		}
	}
}
