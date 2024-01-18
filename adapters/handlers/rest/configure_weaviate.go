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

// Package rest with all rest API functions.
package rest

import (
	"crypto/tls"

	"github.com/go-openapi/swag"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/usecases/config"
)

var connectorOptionGroup *swag.CommandLineOptionsGroup

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
