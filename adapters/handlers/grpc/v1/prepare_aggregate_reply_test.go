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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestGRPCAggregateReply(t *testing.T) {
	tests := []struct {
		name      string
		res       interface{}
		outRes    *pb.AggregateReply
		wantError error
	}{
		{
			name: "meta count",
			res: &aggregation.Result{
				Groups: []aggregation.Group{
					{
						Count: 11,
					},
				},
			},
			outRes: &pb.AggregateReply{
				Result: &pb.AggregateReply_GroupedResults{
					GroupedResults: &pb.AggregateReply_Grouped{
						Groups: []*pb.AggregateReply_Group{
							{
								ObjectsCount: ptInt64(11),
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replier := NewAggregateReplier(nil, nil)
			result, err := replier.Aggregate(tt.res, true)
			if tt.wantError != nil {
				require.Error(t, err)
				assert.EqualError(t, tt.wantError, err.Error())
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.outRes, result)
			}
		})
	}
}
