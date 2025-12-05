//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"fmt"
	"net/http"

	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

var (
	ErrReadOnlyModeEnabled  = fmt.Errorf("read-only mode is enabled temporally at the config level: write operations are not allowed during this time")
	ErrScaleOutModeEnabled  = fmt.Errorf("scale-out mode is enabled temporally at the config level: write operations (besides /replication) are not allowed during this time")
	ErrWriteOnlyModeEnabled = fmt.Errorf("write-only mode is enabled temporally at the config level: read operations are not allowed during this time")
)

const (
	READ_WRITE = "ReadWrite"
	READ_ONLY  = "ReadOnly"
	WRITE_ONLY = "WriteOnly"
	SCALE_OUT  = "ScaleOut"
)

// A slice of endpoints to whitelist when determining if an operation is allowed in ReadOnly mode
var ReadOnlyWhitelist = []string{
	"/backups",
	"/replication",
}

// A slice of endpoints to whitelist when determining if an operation is allowed in WriteOnly mode
var WriteOnlyWhitelist = []string{
	"/backups",
	"/cluster",
	"/nodes",
	"/replication",
	"/.well-known",
	"/meta",
}

// A slice of endpoints to whitelist when determining if an operation is allowed in ScaleOut mode
var ScaleOutWhitelist = append(ReadOnlyWhitelist, "/replication")

func IsHTTPRead(method string) bool {
	return method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
}

func IsGRPCRead(method string) bool {
	return method == protocol.Weaviate_Search_FullMethodName ||
		method == protocol.Weaviate_Aggregate_FullMethodName ||
		method == protocol.Weaviate_TenantsGet_FullMethodName
}

func IsGRPCWrite(method string) bool {
	return !IsGRPCRead(method)
}
