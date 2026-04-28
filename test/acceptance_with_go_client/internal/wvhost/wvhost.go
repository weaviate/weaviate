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

// Package wvhost resolves the Weaviate host/port for acceptance tests.
//
// Defaults match the legacy hardcoded values (localhost:8080 / localhost:50051)
// so existing CI flows are unchanged. The parity stack overrides them via
// WV_HTTP_PORT and WV_GRPC_PORT.
package wvhost

import "os"

func REST() string {
	if p := os.Getenv("WV_HTTP_PORT"); p != "" {
		return "localhost:" + p
	}
	return "localhost:8080"
}

func GRPC() string {
	if p := os.Getenv("WV_GRPC_PORT"); p != "" {
		return "localhost:" + p
	}
	return "localhost:50051"
}
