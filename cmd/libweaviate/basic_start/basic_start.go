//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package main

// This example shows how to start weaviate and get the api, which allows scripting a weaviate instance.

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-openapi/loads"
	"github.com/go-openapi/swag"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/usecases/config"
)

func main() {
	dataPath := flag.String("data-path", "./data", "path to the data directory")
	origin := flag.String("origin", "http://localhost:8080", "listen address")
	defaultVectoriser := flag.String("default-vectoriser", "none", "default vectoriser")

	flag.Parse()
	// set environment variables
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("LOG_FORMAT", "text")
	os.Setenv("PROMETHEUS_MONITORING_ENABLED", "true")
	os.Setenv("GO_BLOCK_PROFILE_RATE", "20")
	os.Setenv("GO_MUTEX_PROFILE_FRACTION", "20")
	os.Setenv("PERSISTENCE_DATA_PATH", *dataPath)
	os.Setenv("ORIGIN", *origin)
	os.Setenv("QUERY_DEFAULTS_LIMIT", "20")
	os.Setenv("QUERY_MAXIMUM_RESULTS", "10000")
	os.Setenv("TRACK_VECTOR_DIMENSIONS", "true")
	os.Setenv("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED", "true")
	os.Setenv("DEFAULT_VECTORIZER_MODULE", *defaultVectoriser)



	logger := logrus.New()
	var connectorOptionGroup *swag.CommandLineOptionsGroup = config.GetConfigOptionGroup()

	// Load the config using the flags
	serverConfig := &config.WeaviateConfig{}
	err := serverConfig.LoadConfig(connectorOptionGroup, logger)

	swaggerSpec, err := loads.Embedded(rest.SwaggerJSON, rest.FlatSwaggerJSON)
	if err != nil {
		log.Fatalln(err)
	}
	api := operations.NewWeaviateAPI(swaggerSpec)
	rest.ConfigureFlags(api)
	handler := rest.ConfigureAPI(api)

	http.Handle("/", handler)
	fmt.Printf("Weaviate is running on %s\n", *origin)
	http.ListenAndServe(":8080", nil)


}