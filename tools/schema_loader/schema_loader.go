// Utility to load a database schema into a Weaviate instance.
package main

import (
	"flag"
	"github.com/creativesoftwarefdn/weaviate/tools/schema_loader/loader"
	log "github.com/sirupsen/logrus"
)

func main() {
	var rootApiKey string
	var rootApiToken string
	var serverPort string
	var serverHost string
	var serverScheme string
	var actionSchemaFile string
	var thingSchemaFile string
	var replaceExisting bool
	var debug bool

	flag.StringVar(&rootApiKey, "api-key", "657a48b9-e000-4d9a-b51d-69a0b621c1b9", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&rootApiToken, "api-token", "57ac8392-1ecc-4e17-9350-c9c866ac832b", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&serverPort, "server-port", "8080", "Port number on which the server is running.")
	flag.StringVar(&serverHost, "server-host", "127.0.0.1", "Host-name on which the server is running.")
	flag.StringVar(&serverScheme, "server-scheme", "http", "The http schema to use")
	flag.StringVar(&actionSchemaFile, "action-schema", "", "The action schema to load")
	flag.StringVar(&thingSchemaFile, "thing-schema", "", "The thing schema to load")
	flag.BoolVar(&replaceExisting, "replace-existing", true, "Replace the existing schema classes in case they already exist")
	flag.BoolVar(&debug, "debug", false, "Print out detailed debug information")
	flag.Parse()

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

	loader := loader.New().
		SetLogger(logger).
		SetHost(serverHost).
		SetPort(serverPort).
		SetScheme(serverScheme).
		SetKeyAndToken(rootApiKey, rootApiToken).
		FromSchemaFiles(actionSchemaFile, thingSchemaFile).
		ReplaceExistingClasses(replaceExisting)

	err := loader.Load()

	if err != nil {
		panic(err)
	}
}
