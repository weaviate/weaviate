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
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/creativesoftwarefdn/weaviate/restapi/batch"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/graphql"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/knowledge_tools"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/meta"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/p2_p"
	"github.com/creativesoftwarefdn/weaviate/restapi/state"

	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/rs/cors"
	"google.golang.org/grpc/grpclog"

	weaviateBroker "github.com/creativesoftwarefdn/weaviate/broker"
	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database"
	dblisting "github.com/creativesoftwarefdn/weaviate/database/listing"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	databaseSchema "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	schemaContextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	etcdSchemaManager "github.com/creativesoftwarefdn/weaviate/database/schema_manager/etcd"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
	graphqlnetwork "github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	rest_api_utils "github.com/creativesoftwarefdn/weaviate/restapi/rest_api_utils"
	"github.com/creativesoftwarefdn/weaviate/validation"

	libcontextionary "github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/graphiql"
	"github.com/creativesoftwarefdn/weaviate/lib/feature_flags"
	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	libnetworkFake "github.com/creativesoftwarefdn/weaviate/network/fake"
	libnetworkP2P "github.com/creativesoftwarefdn/weaviate/network/p2p"
	"github.com/creativesoftwarefdn/weaviate/restapi/swagger_middleware"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	telemutils "github.com/creativesoftwarefdn/weaviate/telemetry/utils"
)

const pageOverride int = 1
const error422 string = "The request is well-formed but was unable to be followed due to semantic errors."

var connectorOptionGroup *swag.CommandLineOptionsGroup

// rawContextionary is the contextionary as we read it from the files. It is
// not extended by schema builds. It is important to keep this untouched copy,
// so that we can rebuild a clean contextionary on every schema change based on
// this contextionary and the current schema
var rawContextionary libcontextionary.Contextionary

// contextionary is the contextionary we keep amending on every schema change
var contextionary libcontextionary.Contextionary
var network libnetwork.Network
var serverConfig *config.WeaviateConfig
var graphQL graphqlapi.GraphQL
var messaging *messages.Messaging

var appState *state.State

var db database.Database

var requestsLog *telemetry.RequestsLog
var reporter *telemetry.Reporter

type State struct {
	db database.Database
}

type graphQLRoot struct {
	database.Database
	libnetwork.Network
	contextionary *schemaContextionary.Contextionary
	log           *telemetry.RequestsLog
}

func (r graphQLRoot) GetNetworkResolver() graphqlnetwork.Resolver {
	return r.Network
}

func (r graphQLRoot) GetContextionary() fetch.Contextionary {
	return r.contextionary
}

func (r graphQLRoot) GetRequestsLog() *telemetry.RequestsLog {
	return r.log
}

type keyTokenHeader struct {
	Key   strfmt.UUID `json:"key"`
	Token strfmt.UUID `json:"token"`
}

func init() {
	appState = &state.State{}

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

	// Generate SingleRefs
	for _, keyID := range keyIDs {
		refs = append(refs, &models.SingleRef{
			NrDollarCref: strfmt.URI(keyID),
		})
	}

	return refs
}

func configureFlags(api *operations.WeaviateAPI) {
	connectorOptionGroup = config.GetConfigOptionGroup()

	api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{
		*connectorOptionGroup,
	}
}

// createErrorResponseObject is a common function to create an error response
func createErrorResponseObject(messages ...string) *models.ErrorResponse {
	// Initialize return value
	er := &models.ErrorResponse{}

	// appends all error messages to the error
	for _, message := range messages {
		er.Error = append(er.Error, &models.ErrorResponseErrorItems0{
			Message: message,
		})
	}

	return er
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	enabled := telemutils.IsEnabled()
	interval := telemutils.GetInterval()
	url := telemutils.GetURL()

	requestsLog = telemetry.NewLog(enabled, &appState.ServerConfig.Environment.Network.PeerName)

	reporter = telemetry.NewReporter(requestsLog, interval, url, enabled)

	setupSchemaHandlers(api, requestsLog)
	setupThingsHandlers(api, requestsLog)
	setupActionsHandlers(api, requestsLog)
	setupBatchHandlers(api, requestsLog)

	api.MetaWeaviateMetaGetHandler = meta.WeaviateMetaGetHandlerFunc(func(params meta.WeaviateMetaGetParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return meta.NewWeaviateMetaGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer dbLock.Unlock()
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		// Create response object
		metaResponse := &models.Meta{}

		// Set the response object's values
		metaResponse.Hostname = serverConfig.GetHostAddress()
		metaResponse.ActionsSchema = databaseSchema.ActionSchema.Schema
		metaResponse.ThingsSchema = databaseSchema.ThingSchema.Schema

		// Register the request
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypePOST, telemetry.LocalQueryMeta))
		}()

		return meta.NewWeaviateMetaGetOK().WithPayload(metaResponse)
	})

	api.P2PWeaviateP2pGenesisUpdateHandler = p2_p.WeaviateP2pGenesisUpdateHandlerFunc(func(params p2_p.WeaviateP2pGenesisUpdateParams) middleware.Responder {
		newPeers := make([]peers.Peer, 0)

		for _, genesisPeer := range params.Peers {
			peer := peers.Peer{
				ID:         genesisPeer.ID,
				Name:       genesisPeer.Name,
				URI:        genesisPeer.URI,
				SchemaHash: genesisPeer.SchemaHash,
			}

			newPeers = append(newPeers, peer)
		}

		err := network.UpdatePeers(newPeers)

		if err == nil {
			// Register the request
			go func() {
				requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypePOST, telemetry.NetworkQueryMeta))
			}()

			return p2_p.NewWeaviateP2pGenesisUpdateOK()
		}
		return p2_p.NewWeaviateP2pGenesisUpdateInternalServerError()
	})

	api.P2PWeaviateP2pHealthHandler = p2_p.WeaviateP2pHealthHandlerFunc(func(params p2_p.WeaviateP2pHealthParams) middleware.Responder {

		// Register the request // added as comment to ensure registration inclusion when this function is implemented
		//		go func() {
		//			requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypePOST, telemetry.NetworkQueryMeta))
		//		}()
		// For now, always just return success.
		return middleware.NotImplemented("operation P2PWeaviateP2pHealth has not yet been implemented")
	})

	api.GraphqlWeaviateGraphqlPostHandler = graphql.WeaviateGraphqlPostHandlerFunc(func(params graphql.WeaviateGraphqlPostParams) middleware.Responder {
		defer messaging.TimeTrack(time.Now())
		messaging.DebugMessage("Starting GraphQL resolving")

		errorResponse := &models.ErrorResponse{}

		// Get all input from the body of the request, as it is a POST.
		query := params.Body.Query
		operationName := params.Body.OperationName

		// If query is empty, the request is unprocessable
		if query == "" {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: "query cannot be empty",
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Only set variables if exists in request
		var variables map[string]interface{}
		if params.Body.Variables != nil {
			variables = params.Body.Variables.(map[string]interface{})
		}

		if graphQL == nil {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: "no graphql provider present, " +
						"this is most likely because no schema is present. Import a schema first!",
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		result := graphQL.Resolve(query, operationName, variables, nil)

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)
		if jsonErr != nil {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: fmt.Sprintf("couldn't marshal json: %s", jsonErr),
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Put the data in a response ready object
		graphQLResponse := &models.GraphQLResponse{}
		marshallErr := json.Unmarshal(resultJSON, graphQLResponse)

		// If json gave error, return nothing.
		if marshallErr != nil {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: fmt.Sprintf("couldn't unmarshal json: %s\noriginal result was %#v", marshallErr, result),
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Register the request
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeGQL, telemetry.LocalAdd))
		}()

		// Return the response
		return graphql.NewWeaviateGraphqlPostOK().WithPayload(graphQLResponse)
	})

	/*
	 * HANDLE BATCHING
	 */

	api.GraphqlWeaviateGraphqlBatchHandler = graphql.WeaviateGraphqlBatchHandlerFunc(func(params graphql.WeaviateGraphqlBatchParams) middleware.Responder {
		defer messaging.TimeTrack(time.Now())
		messaging.DebugMessage("Starting GraphQL batch resolving")

		if graphQL == nil {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: "no graphql provider present, " +
						"this is most likely because no schema is present. Import a schema first!",
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		amountOfBatchedRequests := len(params.Body)
		errorResponse := &models.ErrorResponse{}

		if amountOfBatchedRequests == 0 {
			return graphql.NewWeaviateGraphqlBatchUnprocessableEntity().WithPayload(errorResponse)
		}
		requestResults := make(chan rest_api_utils.UnbatchedRequestResponse, amountOfBatchedRequests)

		wg := new(sync.WaitGroup)

		// Generate a goroutine for each separate request
		for requestIndex, unbatchedRequest := range params.Body {
			wg.Add(1)
			go handleUnbatchedGraphQLRequest(wg, context.Background(), unbatchedRequest, requestIndex, &requestResults, requestsLog)
		}

		wg.Wait()

		close(requestResults)

		batchedRequestResponse := make([]*models.GraphQLResponse, amountOfBatchedRequests)

		// Add the requests to the result array in the correct order
		for unbatchedRequestResult := range requestResults {
			batchedRequestResponse[unbatchedRequestResult.RequestIndex] = unbatchedRequestResult.Response
		}

		return graphql.NewWeaviateGraphqlBatchOK().WithPayload(batchedRequestResponse)
	})

	/*
	 * HANDLE KNOWLEDGE TOOLS
	 */
	api.KnowledgeToolsWeaviateToolsMapHandler = knowledge_tools.WeaviateToolsMapHandlerFunc(func(params knowledge_tools.WeaviateToolsMapParams) middleware.Responder {
		return middleware.NotImplemented("operation knowledge_tools.WeaviateToolsMap has not yet been implemented")
	})

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

func setupBatchHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog) {
	batchAPI := batch.New(appState, requestsLog)

	api.WeaviateBatchingThingsCreateHandler = operations.WeaviateBatchingThingsCreateHandlerFunc(batchAPI.ThingsCreate)
	api.WeaviateBatchingActionsCreateHandler = operations.WeaviateBatchingActionsCreateHandlerFunc(batchAPI.ActionsCreate)
}

// Handle a single unbatched GraphQL request, return a tuple containing the index of the request in the batch and either the response or an error
func handleUnbatchedGraphQLRequest(wg *sync.WaitGroup, ctx context.Context, unbatchedRequest *models.GraphQLQuery, requestIndex int, requestResults *chan rest_api_utils.UnbatchedRequestResponse, log *telemetry.RequestsLog) {
	defer wg.Done()

	// Get all input from the body of the request
	query := unbatchedRequest.Query
	operationName := unbatchedRequest.OperationName
	graphQLResponse := &models.GraphQLResponse{}

	// Return an unprocessable error if the query is empty
	if query == "" {

		// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
		errorCode := strconv.Itoa(graphql.WeaviateGraphqlBatchUnprocessableEntityCode)
		errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
		errors := []*models.GraphQLError{&models.GraphQLError{Message: errorMessage}}
		graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
		*requestResults <- rest_api_utils.UnbatchedRequestResponse{
			requestIndex,
			&graphQLResponse,
		}
	} else {

		// Extract any variables from the request
		var variables map[string]interface{}
		if unbatchedRequest.Variables != nil {
			variables = unbatchedRequest.Variables.(map[string]interface{})
		}

		result := graphQL.Resolve(query, operationName, variables, ctx)

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)

		// Return an unprocessable error if marshalling the result to JSON failed
		if jsonErr != nil {

			// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
			errorCode := strconv.Itoa(graphql.WeaviateGraphqlBatchUnprocessableEntityCode)
			errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
			errors := []*models.GraphQLError{&models.GraphQLError{Message: errorMessage}}
			graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
			*requestResults <- rest_api_utils.UnbatchedRequestResponse{
				requestIndex,
				&graphQLResponse,
			}
		} else {

			// Put the result data in a response ready object
			marshallErr := json.Unmarshal(resultJSON, graphQLResponse)

			// Return an unprocessable error if unmarshalling the result to JSON failed
			if marshallErr != nil {

				// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
				errorCode := strconv.Itoa(graphql.WeaviateGraphqlBatchUnprocessableEntityCode)
				errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
				errors := []*models.GraphQLError{&models.GraphQLError{Message: errorMessage}}
				graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
				*requestResults <- rest_api_utils.UnbatchedRequestResponse{
					requestIndex,
					&graphQLResponse,
				}
			} else {

				// Register the request
				go func() {
					requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeGQL, telemetry.LocalAdd))
				}()

				// Return the GraphQL response
				*requestResults <- rest_api_utils.UnbatchedRequestResponse{
					requestIndex,
					graphQLResponse,
				}
			}
		}
	}
}

func handleBatchedActionsCreateRequest(wg *sync.WaitGroup, ctx context.Context, batchedRequest *models.ActionCreate, requestIndex int, requestResults *chan rest_api_utils.BatchedActionsCreateRequestResponse, async bool, requestLocks *rest_api_utils.RequestLocks, fieldsToKeep map[string]int) {
	defer wg.Done()

	// Generate UUID for the new object
	UUID := connutils.GenerateUUID()

	// Validate schema given in body with the weaviate schema
	databaseSchema := schema.HackFromDatabaseSchema(requestLocks.DBLock.GetSchema())

	// Create Action object
	action := &models.Action{}
	action.AtContext = batchedRequest.AtContext
	action.LastUpdateTimeUnix = 0

	if _, ok := fieldsToKeep["@class"]; ok {
		action.AtClass = batchedRequest.AtClass
	}
	if _, ok := fieldsToKeep["schema"]; ok {
		action.Schema = batchedRequest.Schema
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		action.CreationTimeUnix = connutils.NowUnix()
	}

	// Create request result object
	result := &models.ActionsGetResponseAO1Result{}
	result.Errors = nil

	// Create request response object
	responseObject := &models.ActionsGetResponse{}
	responseObject.Action = *action
	if _, ok := fieldsToKeep["actionid"]; ok {
		responseObject.ActionID = UUID
	}
	responseObject.Result = result

	resultStatus := models.ActionsGetResponseAO1ResultStatusSUCCESS

	validatedErr := validation.ValidateActionBody(ctx, batchedRequest, databaseSchema, requestLocks.DBConnector,
		network, serverConfig)

	if validatedErr != nil {
		// Edit request result status
		responseObject.Result.Errors = createErrorResponseObject(validatedErr.Error())
		resultStatus = models.ActionsGetResponseAO1ResultStatusFAILED
		responseObject.Result.Status = &resultStatus
	} else {
		// Handle asynchronous requests
		if async {
			requestLocks.DelayedLock.IncSteps()
			resultStatus = models.ActionsGetResponseAO1ResultStatusPENDING
			responseObject.Result.Status = &resultStatus

			go func() {
				defer requestLocks.DelayedLock.Unlock()
				err := requestLocks.DBConnector.AddAction(ctx, action, UUID)

				if err != nil {
					// Edit request result status
					resultStatus = models.ActionsGetResponseAO1ResultStatusFAILED
					responseObject.Result.Status = &resultStatus
					responseObject.Result.Errors = createErrorResponseObject(err.Error())
				}
			}()
		} else {
			// Handle synchronous requests
			err := requestLocks.DBConnector.AddAction(ctx, action, UUID)

			if err != nil {
				// Edit request result status
				resultStatus = models.ActionsGetResponseAO1ResultStatusFAILED
				responseObject.Result.Status = &resultStatus
				responseObject.Result.Errors = createErrorResponseObject(err.Error())
			}
		}
	}

	// Send this batched request's response and its place in the batch request to the channel
	*requestResults <- rest_api_utils.BatchedActionsCreateRequestResponse{
		requestIndex,
		responseObject,
	}
}

func handleBatchedThingsCreateRequest(wg *sync.WaitGroup, ctx context.Context, batchedRequest *models.ThingCreate, requestIndex int, requestResults *chan rest_api_utils.BatchedThingsCreateRequestResponse, async bool, requestLocks *rest_api_utils.RequestLocks, fieldsToKeep map[string]int) {
	defer wg.Done()

	// Generate UUID for the new object
	UUID := connutils.GenerateUUID()

	// Validate schema given in body with the weaviate schema
	databaseSchema := schema.HackFromDatabaseSchema(requestLocks.DBLock.GetSchema())

	// Create Thing object
	thing := &models.Thing{}
	thing.AtContext = batchedRequest.AtContext
	thing.LastUpdateTimeUnix = 0

	if _, ok := fieldsToKeep["@class"]; ok {
		thing.AtClass = batchedRequest.AtClass
	}
	if _, ok := fieldsToKeep["schema"]; ok {
		thing.Schema = batchedRequest.Schema
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		thing.CreationTimeUnix = connutils.NowUnix()
	}

	// Create request result object
	result := &models.ThingsGetResponseAO1Result{}
	result.Errors = nil

	// Create request response object
	responseObject := &models.ThingsGetResponse{}

	responseObject.Thing = *thing
	if _, ok := fieldsToKeep["thingid"]; ok {
		responseObject.ThingID = UUID
	}
	responseObject.Result = result

	resultStatus := models.ThingsGetResponseAO1ResultStatusSUCCESS

	validatedErr := validation.ValidateThingBody(ctx, batchedRequest, databaseSchema, requestLocks.DBConnector,
		network, serverConfig)

	if validatedErr != nil {
		// Edit request result status
		responseObject.Result.Errors = createErrorResponseObject(validatedErr.Error())
		resultStatus = models.ThingsGetResponseAO1ResultStatusFAILED
		responseObject.Result.Status = &resultStatus
	} else {
		// Handle asynchronous requests
		if async {
			requestLocks.DelayedLock.IncSteps()
			resultStatus = models.ThingsGetResponseAO1ResultStatusPENDING
			responseObject.Result.Status = &resultStatus

			go func() {
				defer requestLocks.DelayedLock.Unlock()
				err := requestLocks.DBConnector.AddThing(ctx, thing, UUID)

				if err != nil {
					// Edit request result status
					resultStatus = models.ThingsGetResponseAO1ResultStatusFAILED
					responseObject.Result.Status = &resultStatus
					responseObject.Result.Errors = createErrorResponseObject(err.Error())
				}
			}()
		} else {
			// Handle synchronous requests
			err := requestLocks.DBConnector.AddThing(ctx, thing, UUID)

			if err != nil {
				// Edit request result status
				resultStatus = models.ThingsGetResponseAO1ResultStatusFAILED
				responseObject.Result.Status = &resultStatus
				responseObject.Result.Errors = createErrorResponseObject(err.Error())
			}
		}
	}

	// Send this batched request's response and its place in the batch request to the channel
	*requestResults <- rest_api_utils.BatchedThingsCreateRequestResponse{
		requestIndex,
		responseObject,
	}
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
	// context for the startup procedure. (So far the only subcommand respecting
	// the context is the schema initialization, as this uses the etcd client
	// requiring context. Nevertheless it would make sense to have everything
	// that goes on in here pay attention to the context, so we can have a
	// "startup in x seconds or fail")
	ctx := context.Background()
	// The timeout is arbitrary we have to adjust it as we go along, if we
	// realize it is to big/small
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Create message service
	messaging = &messages.Messaging{}
	appState.Messaging = messaging

	// Load the config using the flags
	serverConfig = &config.WeaviateConfig{}
	appState.ServerConfig = serverConfig
	err := serverConfig.LoadConfig(connectorOptionGroup, messaging)

	// Add properties to the config
	serverConfig.Hostname = addr
	serverConfig.Scheme = scheme

	// Fatal error loading config file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	loadContextionary()

	connectToNetwork()

	// Connect to MQTT via Broker
	weaviateBroker.ConnectToMqtt(serverConfig.Environment.Broker.Host, serverConfig.Environment.Broker.Port)

	// Create the database connector usint the config
	err, dbConnector := dblisting.NewConnector(serverConfig.Environment.Database.Name, serverConfig.Environment.Database.DatabaseConfig)
	// Could not find, or configure connector.
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	// parse config store URL
	configStore, err := url.Parse(serverConfig.Environment.ConfigStore.URL)
	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("cannot parse config store URL: %s", err))
	}

	// Construct a distributed lock
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{configStore.String()}})
	if err != nil {
		log.Fatal(err)
	}

	s1, err := concurrency.NewSession(etcdClient)
	if err != nil {
		log.Fatal(err)
	}

	manager, err := etcdSchemaManager.New(ctx, etcdClient, dbConnector, network)
	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not initialize local database state: %v", err))
	}

	manager.RegisterSchemaUpdateCallback(updateSchemaCallback)

	// initialize the contextinoary with the rawContextionary, it will get updated on each schema update
	contextionary = rawContextionary

	// Now instantiate a database, with the configured lock, manager and connector.
	dbParams := &database.Params{
		LockerKey:     "/weaviate/schema-connector-rw-lock",
		LockerSession: s1,
		SchemaManager: manager,
		Connector:     dbConnector,
		Contextionary: contextionary,
		Messaging:     messaging,
	}
	db, err = database.New(ctx, dbParams)
	if err != nil {
		messaging.ExitError(1, fmt.Sprintf("Could not initialize the database: %s", err.Error()))
	}
	appState.Database = db

	manager.TriggerSchemaUpdateCallbacks()
	network.RegisterUpdatePeerCallback(func(peers peers.Peers) {
		manager.TriggerSchemaUpdateCallbacks()
	})

	network.RegisterSchemaGetter(&schemaGetter{db: db})
}

func updateSchemaCallback(updatedSchema schema.Schema) {
	// Note that this is thread safe; we're running in a single go-routine, because the event
	// handlers are called when the SchemaLock is still held.
	rebuildContextionary(updatedSchema)
	rebuildGraphQL(updatedSchema, requestsLog)
}

func rebuildContextionary(updatedSchema schema.Schema) {
	// build new contextionary extended by the local schema
	schemaContextionary, err := databaseSchema.BuildInMemoryContextionaryFromSchema(updatedSchema, &rawContextionary)
	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not build in-memory contextionary from schema; %+v", err))
	}

	// Combine contextionaries
	contextionaries := []libcontextionary.Contextionary{rawContextionary, *schemaContextionary}
	combined, err := libcontextionary.CombineVectorIndices(contextionaries)

	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not combine the contextionary database with the in-memory generated contextionary; %+v", err))
	}

	messaging.InfoMessage("Contextionary extended with names in the schema")

	contextionary = libcontextionary.Contextionary(combined)
}

func rebuildGraphQL(updatedSchema schema.Schema, requestsLog *telemetry.RequestsLog) {
	peers, err := network.ListPeers()
	if err != nil {
		graphQL = nil
		messaging.ErrorMessage(fmt.Sprintf("could not list network peers to regenerate schema:\n%#v\n", err))
		return
	}

	c11y := schemaContextionary.New(contextionary)
	root := graphQLRoot{Database: db, Network: network, contextionary: c11y, log: requestsLog}
	updatedGraphQL, err := graphqlapi.Build(&updatedSchema, peers, root, messaging)
	if err != nil {
		// TODO: turn on safe mode gh-520
		graphQL = nil
		messaging.ErrorMessage(fmt.Sprintf("Could not re-generate GraphQL schema, because:\n%#v\n", err))
	} else {
		messaging.InfoMessage("Updated GraphQL schema")
		graphQL = updatedGraphQL
	}
}

type schemaGetter struct {
	db database.Database
}

func (s *schemaGetter) Schema() (schema.Schema, error) {
	dbLock, err := s.db.ConnectorLock()
	if err != nil {
		return schema.Schema{}, err
	}

	defer dbLock.Unlock()
	return dbLock.GetSchema(), nil
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

		handler.ServeHTTP(w, r)
	})
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
// Contains "x-api-key", "x-api-token" for legacy reasons, older interfaces might need these headers.
func setupGlobalMiddleware(handler http.Handler) http.Handler {
	handleCORS := cors.New(cors.Options{
		OptionsPassthrough: true,
		AllowedHeaders:     []string{"*"},
	}).Handler
	handler = handleCORS(handler)

	if feature_flags.EnableDevUI {
		handler = graphiql.AddMiddleware(handler)
		handler = swagger_middleware.AddMiddleware([]byte(SwaggerJSON), handler)
	}

	handler = addLogging(handler)
	handler = addPreflight(handler)

	return handler
}

func addLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if serverConfig.Environment.Debug {
			log.Printf("Received request: %+v %+v\n", r.Method, r.URL)
		}
		next.ServeHTTP(w, r)
	})
}

func addPreflight(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		}

		next.ServeHTTP(w, r)
	})
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

	mmapedContextionary, err := libcontextionary.LoadVectorFromDisk(serverConfig.Environment.Contextionary.KNNFile, serverConfig.Environment.Contextionary.IDXFile)

	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not load Contextionary; %+v", err))
	}

	messaging.InfoMessage("Contextionary loaded from disk")

	rawContextionary = mmapedContextionary
}

func connectToNetwork() {
	if serverConfig.Environment.Network == nil {
		messaging.InfoMessage(fmt.Sprintf("No network configured, not joining one"))
		network = libnetworkFake.FakeNetwork{}
		appState.Network = network
	} else {
		genesis_url := strfmt.URI(serverConfig.Environment.Network.GenesisURL)
		public_url := strfmt.URI(serverConfig.Environment.Network.PublicURL)
		peer_name := serverConfig.Environment.Network.PeerName

		messaging.InfoMessage(fmt.Sprintf("Network configured, connecting to Genesis '%v'", genesis_url))
		new_net, err := libnetworkP2P.BootstrapNetwork(messaging, genesis_url, public_url, peer_name)
		if err != nil {
			messaging.ExitError(78, fmt.Sprintf("Could not connect to network! Reason: %+v", err))
		} else {
			network = *new_net
			appState.Network = *new_net
		}
	}
}

func errPayloadFromSingleErr(err error) *models.ErrorResponse {
	return &models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{{
		Message: fmt.Sprintf("%s", err),
	}}}
}
