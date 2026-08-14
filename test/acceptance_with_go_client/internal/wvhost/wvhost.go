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
// tests. Defaults are localhost:8080 / localhost:50051; WV_TEST_HOST,
// WV_TEST_REST_PORT, and WV_TEST_GRPC_PORT override host and port independently.
package wvhost

import "os"

func REST() string {
	return host() + ":" + port("WV_TEST_REST_PORT", "8080")
}

func GRPC() string {
	return host() + ":" + port("WV_TEST_GRPC_PORT", "50051")
}

func host() string {
	if h := os.Getenv("WV_TEST_HOST"); h != "" {
		return h
	}
	return "localhost"
}

func port(env, def string) string {
	if p := os.Getenv(env); p != "" {
		return p
	}
	return def
}
