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

package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// TestGRPCAggregateReply_GroupByStripsNamespace pins the strip on the narrow
// vector where a group-by bucket value is itself a class name (ref-target
// grouping). The Replier had no principal before this fix, so the qualified
// "<ns>:Class" value leaked verbatim. Non-class string values (the common
// case) are unaffected — StripOwnNamespace is a no-op without the separator.
func TestGRPCAggregateReply_GroupByStripsNamespace(t *testing.T) {
	nsCaller := &models.Principal{Username: "u", Namespace: "customer1"}
	cases := []struct {
		name      string
		principal *models.Principal
		in        aggregation.GroupedBy
		wantValue any
	}{
		{
			name:      "namespaced caller: own-NS class-name bucket stripped (string)",
			principal: nsCaller,
			in:        aggregation.GroupedBy{Path: []string{"hasAnimals"}, Value: "customer1:Animal"},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Text{Text: "Animal"},
		},
		{
			name:      "namespaced caller: foreign-NS bucket preserved (string)",
			principal: nsCaller,
			in:        aggregation.GroupedBy{Path: []string{"hasAnimals"}, Value: "customer2:Animal"},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Text{Text: "customer2:Animal"},
		},
		{
			name:      "namespaced caller: plain value unchanged (string)",
			principal: nsCaller,
			in:        aggregation.GroupedBy{Path: []string{"name"}, Value: "Tigger"},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Text{Text: "Tigger"},
		},
		{
			name:      "namespaced caller: own-NS class names in slice stripped ([]string)",
			principal: nsCaller,
			in:        aggregation.GroupedBy{Path: []string{"hasAnimals"}, Value: []string{"customer1:Animal", "customer2:Plant", "Global"}},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Texts{Texts: &pb.TextArray{Values: []string{"Animal", "customer2:Plant", "Global"}}},
		},
		{
			name:      "global admin: qualified bucket preserved",
			principal: &models.Principal{Username: "admin"},
			in:        aggregation.GroupedBy{Path: []string{"hasAnimals"}, Value: "customer1:Animal"},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Text{Text: "customer1:Animal"},
		},
		{
			name:      "nil principal: pass-through (NS-disabled cluster)",
			principal: nil,
			in:        aggregation.GroupedBy{Path: []string{"hasAnimals"}, Value: "customer1:Animal"},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Text{Text: "customer1:Animal"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replier := NewAggregateReplier(tc.principal, nil, nil)
			got, err := replier.parseAggregateGroupedBy(&tc.in)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tc.wantValue, got.Value)
			assert.Equal(t, tc.in.Path, got.Path)
		})
	}
}

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
			replier := NewAggregateReplier(nil, nil, nil)
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
