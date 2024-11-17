package query

import v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"

type Decoder struct {
	replier *v1.Parser
}
