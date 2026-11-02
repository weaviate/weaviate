//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package main

import (
	"errors"
	"log"
	"os"

	"github.com/go-openapi/loads"
	flags "github.com/jessevdk/go-flags"

	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
)

// go-swagger does not generate this file (--exclude-main in
// tools/gen-code-from-swagger.sh), so edits here survive regeneration.

func main() {
	swaggerSpec, err := loads.Embedded(rest.SwaggerJSON, rest.FlatSwaggerJSON)
	if err != nil {
		log.Fatalln(err)
	}

	api := operations.NewWeaviateAPI(swaggerSpec)
	server := rest.NewServer(api)
	defer server.Shutdown()

	parser := flags.NewParser(server, flags.Default)
	parser.ShortDescription = swaggerSpec.Spec().Info.Title
	parser.LongDescription = swaggerSpec.Spec().Info.Description
	server.ConfigureFlags()
	for _, optsGroup := range api.CommandLineOptionsGroups {
		_, err := parser.AddGroup(optsGroup.ShortDescription, optsGroup.LongDescription, optsGroup.Options)
		if err != nil {
			log.Fatalln(err)
		}
	}

	if _, err := parser.Parse(); err != nil {
		code := 1
		var fe *flags.Error
		if errors.As(err, &fe) && fe.Type == flags.ErrHelp {
			code = 0
		}
		os.Exit(code)
	}

	server.ConfigureAPI()

	if err := server.ServeAndShutdown(); err != nil {
		log.Fatalln(err)
	}
}
