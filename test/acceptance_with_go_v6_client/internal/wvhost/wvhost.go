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

// Package wvhost resolves the Weaviate REST and gRPC endpoints for acceptance
// tests. Defaults are localhost:8080 / localhost:50051 / localhost:6060;
// WV_TEST_HOST, WV_TEST_REST_PORT, WV_TEST_GRPC_PORT and WV_TEST_DEBUG_PORT
// override host and port independently.
//
// The v6 client takes host and port separately, so ports are exposed as ints
// next to the "host:port" forms the REST endpoints still use.
package wvhost

import (
	"os"
	"strconv"
)

func Host() string {
	if h := os.Getenv("WV_TEST_HOST"); h != "" {
		return h
	}
	return "localhost"
}

func REST() string {
	return Host() + ":" + strconv.Itoa(RESTPort())
}

func GRPC() string {
	return Host() + ":" + strconv.Itoa(GRPCPort())
}

func Debug() string {
	return Host() + ":" + strconv.Itoa(DebugPort())
}

func RESTPort() int {
	return port("WV_TEST_REST_PORT", 8080)
}

func GRPCPort() int {
	return port("WV_TEST_GRPC_PORT", 50051)
}

func DebugPort() int {
	return port("WV_TEST_DEBUG_PORT", 6060)
}

func port(env string, def int) int {
	p, err := strconv.Atoi(os.Getenv(env))
	if err != nil {
		return def
	}
	return p
}
