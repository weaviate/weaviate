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
