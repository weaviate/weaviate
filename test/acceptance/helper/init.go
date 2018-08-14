package helper

// This file contains the init() function for the helper package.
// In go, each package can have an init() function that runs whenever a package is "imported" in a program, before
// the main function runs.
//
// In our case, we use it to parse additional flags that are used to configure the helper to point to the right
// Weaviate instance, with the correct key and token.

import (
	"flag"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// Configuration flags provided by the user that runs an acceptance test.
var RootApiKey strfmt.UUID
var RootApiToken string
var ServerPort string
var ServerHost string
var ServerScheme string
var DebugClient bool

// Credentials for the root key
var RootAuth runtime.ClientAuthInfoWriterFunc

func init() {
	var rootApiKey string
	flag.StringVar(&rootApiKey, "api-key", "", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&RootApiToken, "api-token", "", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&ServerPort, "server-port", "", "Port number on which the server is running.")
	flag.StringVar(&ServerHost, "server-host", "", "Host-name on which the server is running.")
	flag.StringVar(&ServerScheme, "server-scheme", "", "Scheme on which the server is running.")
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
