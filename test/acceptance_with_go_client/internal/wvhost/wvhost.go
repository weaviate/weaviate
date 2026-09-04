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
package wvhost

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v6"
)

func REST() string {
	return host() + ":" + port("WV_TEST_REST_PORT", "8080")
}

func GRPC() string {
	return host() + ":" + port("WV_TEST_GRPC_PORT", "50051")
}

func Debug() string {
	return host() + ":" + port("WV_TEST_DEBUG_PORT", "6060")
}

func host() string {
	if h := os.Getenv("WV_TEST_HOST"); h != "" {
		return h
	}
	return "localhost"
}

func NewClient(t *testing.T) *weaviate.Client {
	rest, err := strconv.Atoi(port("WV_TEST_REST_PORT", "8080"))
	require.NoError(t, err, "rest port")

	grpc, err := strconv.Atoi(port("WV_TEST_GRPC_PORT", "8080"))
	require.NoError(t, err, "grpc port")

	c, err := weaviate.NewLocal(
		t.Context(),
		weaviate.WithHost(host()),
		weaviate.WithHTTPPort(rest),
		weaviate.WithGRPCPort(grpc),
	)
	require.NoError(t, err)
	require.NotNil(t, c)

	return c
}

func port(env, def string) string {
	if p := os.Getenv(env); p != "" {
		return p
	}
	return def
}
