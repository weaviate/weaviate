package query

import (
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type Encoder struct {
	parser *v1.Replier
}

func (e *Encoder) Encode(res *SearchResponse) (*pb.SearchReply, error) {
}
