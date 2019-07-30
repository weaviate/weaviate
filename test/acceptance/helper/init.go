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

package helper

// This file contains the init() function for the helper package.
// In go, each package can have an init() function that runs whenever a package is "imported" in a program, before
// the main function runs.
//
// In our case, we use it to parse additional flags that are used to configure the helper to point to the right
// Weaviate instance, with the correct key and token.

import (
	"flag"
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// Configuration flags provided by the user that runs an acceptance test.
var RootApiKey strfmt.UUID
var RootApiToken string
var ServerPort string
var ServerHost string
var ServerScheme string
var DebugHTTP bool

// Credentials for the root key
var RootAuth runtime.ClientAuthInfoWriterFunc

func init() {
	var rootApiKey string
	flag.StringVar(&rootApiKey, "api-key", "657a48b9-e000-4d9a-b51d-69a0b621c1b9", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&RootApiToken, "api-token", "57ac8392-1ecc-4e17-9350-c9c866ac832b", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&ServerPort, "server-port", "8080", "Port number on which the server is running.")
	flag.StringVar(&ServerHost, "server-host", "127.0.0.1", "Host-name on which the server is running.")
	flag.StringVar(&ServerScheme, "server-scheme", "http", "Scheme on which the server is running.")
	flag.BoolVar(&DebugHTTP, "debug-http", false, "Whether or not to print HTTP traffic")
	flag.Parse()

	RootApiKey = strfmt.UUID(rootApiKey)

	if ServerScheme == "" {
		ServerScheme = "http"
	}

	if ServerPort == "" {
		panic("Server port is not set!")
	}

	RootAuth = CreateAuth(RootApiKey, RootApiToken)
}

func GetWeaviateURL() string {
	return fmt.Sprintf("%s://%s:%s", ServerScheme, ServerHost, ServerPort)
}
