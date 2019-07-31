//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Utility to load a database schema into a Weaviate instance.
package main

import (
	"flag"
	"net/url"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/tools/schema_loader/loader"
	log "github.com/sirupsen/logrus"
)

func main() {
	var rootApiKey string
	var rootApiToken string
	var weaviateUrlString string
	var actionSchemaFile string
	var thingSchemaFile string
	var replaceExisting bool
	var debug bool

	flag.StringVar(&rootApiKey, "api-key", "657a48b9-e000-4d9a-b51d-69a0b621c1b9", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&rootApiToken, "api-token", "57ac8392-1ecc-4e17-9350-c9c866ac832b", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&weaviateUrlString, "weaviate-url", "http://localhost:8080/v1/", "The address where weaviate can be reached")
	flag.StringVar(&actionSchemaFile, "action-schema", "", "The action schema to load")
	flag.StringVar(&thingSchemaFile, "thing-schema", "", "The thing schema to load")
	flag.BoolVar(&replaceExisting, "replace-existing", true, "Replace the existing schema classes in case they already exist")
	flag.BoolVar(&debug, "debug", false, "Print out detailed debug information")
	flag.Parse()

	weaviateUrl, err := url.Parse(weaviateUrlString)

	if err != nil {
		panic("URL provided is illegal")
	}

	if actionSchemaFile == "" {
		panic("Action schema file is not set")
	}

	if thingSchemaFile == "" {
		panic("Thing schema file is not set")
	}

	logger := log.New()
	if debug {
		logger.Level = log.DebugLevel
	}

	transport := httptransport.New(weaviateUrl.Host, weaviateUrl.RequestURI(), []string{weaviateUrl.Scheme})

	loader := loader.New().
		SetLogger(logger).
		SetTransport(transport).
		SetKeyAndToken(rootApiKey, rootApiToken).
		FromSchemaFiles(actionSchemaFile, thingSchemaFile).
		ReplaceExistingClasses(replaceExisting)

	err = loader.Load()

	if err != nil {
		if errParsed, ok := err.(*schema.SchemaThingsPropertiesAddUnprocessableEntity); ok {
			panic(errParsed.Payload.Error[0].Message)
		}
		if errParsed, ok := err.(*schema.SchemaActionsPropertiesAddUnprocessableEntity); ok {
			panic(errParsed.Payload.Error[0].Message)
		}
		panic(err.Error())
	}
}
