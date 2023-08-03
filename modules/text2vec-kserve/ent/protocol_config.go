//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package ent

type Protocol = string

const (
	KSERVE_HTTP_V1 = Protocol("HttpV1")
	KSERVE_HTTP_V2 = Protocol("HttpV2")
	KSERVE_GRPC    = Protocol("gRPC")
)

var KSERVE_VALID_PROTOCOLS = []Protocol{
	KSERVE_HTTP_V1,
	KSERVE_HTTP_V2,
	KSERVE_GRPC,
}

var (
	KSERVE_DEFAULT_GRPC_CONNECTION_ARGS = map[string]interface{}{
		"dialOptions": map[string]interface{}{
			"insecure": true,
		},
		"callOptions": map[string]interface{}{},
	}
	KSERVE_DEFAULT_HTTP_V1_CONNECTION_ARGS = map[string]interface{}{}
	KSERVE_DEFAULT_HTTP_V2_CONNECTION_ARGS = map[string]interface{}{}
)
