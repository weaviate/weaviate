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

package helper

// This file contains the init() function for the helper package.
// In go, each package can have an init() function that runs whenever a package is "imported" in a program, before
// the main function runs.
//
// In our case, we use it to parse additional flags that are used to configure the helper to point to the right
// Weaviate instance, with the correct key and token.

import (
	"fmt"
	"sync"

	"github.com/go-openapi/runtime"
)

// Configuration flags provided by the user that runs an acceptance test.
var (
	ServerPort     string
	ServerHost     string
	ServerGRPCPort string
	ServerGRPCHost string
	ServerScheme   string
	DebugHTTP      bool
)

// serverMu guards the server address globals above. Suites call SetupClient or
// ResetClient between tests while EventuallyWithT tick goroutines from the
// previous test may still be reading them through Client() or ClientGRPC().
var serverMu sync.RWMutex

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

func ResetClient() {
	serverMu.Lock()
	ServerScheme = "http"
	ServerPort = "8080"
	ServerGRPCPort = ""
	serverMu.Unlock()

	RootAuth = nil
}

// serverTarget reads the HTTP server address under serverMu.
func serverTarget() (target, scheme string) {
	serverMu.RLock()
	defer serverMu.RUnlock()
	return fmt.Sprintf("%s:%s", ServerHost, ServerPort), ServerScheme
}

func GetWeaviateURL() string {
	serverMu.RLock()
	defer serverMu.RUnlock()
	return fmt.Sprintf("%s://%s:%s", ServerScheme, ServerHost, ServerPort)
}

func GetWeaviateGRPCURL() string {
	serverMu.RLock()
	defer serverMu.RUnlock()
	return fmt.Sprintf("%s:%s", ServerGRPCHost, ServerGRPCPort)
}
