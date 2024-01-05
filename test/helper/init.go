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

package helper

// This file contains the init() function for the helper package.
// In go, each package can have an init() function that runs whenever a package is "imported" in a program, before
// the main function runs.
//
// In our case, we use it to parse additional flags that are used to configure the helper to point to the right
// Weaviate instance, with the correct key and token.

import (
	"fmt"

	"github.com/go-openapi/runtime"
)

// Configuration flags provided by the user that runs an acceptance test.
var (
	ServerPort   string
	ServerHost   string
	ServerScheme string
	DebugHTTP    bool
)

// Credentials for the root key
var RootAuth runtime.ClientAuthInfoWriterFunc

func init() {
	if ServerScheme == "" {
		ServerScheme = "http"
	}

	if ServerPort == "" {
		ServerPort = "8080"
	}

	RootAuth = nil
}

func GetWeaviateURL() string {
	return fmt.Sprintf("%s://%s:%s", ServerScheme, ServerHost, ServerPort)
}
