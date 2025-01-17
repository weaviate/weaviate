//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package v1

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/aggregation"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type AggregateReplier struct{}

func NewAggregateReplier() *AggregateReplier {
	return &AggregateReplier{}
}

func (r *AggregateReplier) Aggregate(res interface{}) (*pb.AggregateResult, error) {
	var groups []*pb.AggregateGroupResult

	if res != nil {
		result, ok := res.(*aggregation.Result)
		if !ok {
			return nil, fmt.Errorf("unexpected aggregate result type: %T", res)
		}

		if len(result.Groups) > 0 {
			groups = make([]*pb.AggregateGroupResult, len(result.Groups))
			for i := range result.Groups {
				groups[i] = &pb.AggregateGroupResult{
					Count: int64(result.Groups[i].Count),
				}
			}
		}
	}

	return &pb.AggregateResult{Groups: groups}, nil
}
