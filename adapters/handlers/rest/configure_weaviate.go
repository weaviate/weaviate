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

// Package rest with all rest API functions.
package rest

import (
	"crypto/tls"
	"io/ioutil"
	"log"
	"os"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/state"

	"github.com/go-openapi/swag"
	"google.golang.org/grpc/grpclog"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/operations"
	"github.com/creativesoftwarefdn/weaviate/usecases/config"
	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/messages"

	libcontextionary "github.com/creativesoftwarefdn/weaviate/contextionary"
	libnetwork "github.com/creativesoftwarefdn/weaviate/usecases/network"
)

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
var graphQL graphql.GraphQL
var messaging *messages.Messaging

var appState *state.State

var db database.Database

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

// configureAPI -> see configure_api.go

// configureServer -> see configure_server.go

func configureFlags(api *operations.WeaviateAPI) {
	connectorOptionGroup = config.GetConfigOptionGroup()

	api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{
		*connectorOptionGroup,
	}
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}
