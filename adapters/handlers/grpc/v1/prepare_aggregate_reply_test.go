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

// Pins the defense-in-depth strip on Reference.PointingTo: a stray
// qualified beacon must be rewritten short for namespaced callers,
// admin/nil keep the qualified view, foreign-NS preserved.
func TestGRPCAggregateReply_ReferenceAggregationStripsPointingTo(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"
	mk := func(class string) string {
		return "weaviate://localhost/" + class + "/" + uuid
	}
	cases := []struct {
		name      string
		principal *models.Principal
		in        []string
		want      []string
	}{
		{
			name:      "namespaced caller: own-NS stripped, foreign preserved",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			in:        []string{mk("customer1:Animal"), mk("customer2:Plant"), mk("Global")},
			want:      []string{mk("Animal"), mk("customer2:Plant"), mk("Global")},
		},
		{
			name:      "global principal: qualified beacons preserved",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			in:        []string{mk("customer1:Animal")},
			want:      []string{mk("customer1:Animal")},
		},
		{
			name:      "nil principal: pass-through (NS-disabled cluster)",
			principal: nil,
			in:        []string{mk("customer1:Animal")},
			want:      []string{mk("customer1:Animal")},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replier := NewAggregateReplier(tc.principal, nil, nil)
			got := replier.parseReferenceAggregation("cref",
				aggregation.Reference{PointingTo: tc.in})
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.PointingTo)
		})
	}
}

// TestGRPCAggregateReply_GroupByPassesValuesThrough pins that bucket
// values flow unchanged for string / []string. Group-by values can be
// arbitrary user text (e.g. "customer1:foo"), so we must not rewrite
// them — ref-target buckets, the one case where a class name could
// appear, surface as beacon URIs ("weaviate://.../") not bare names.
func TestGRPCAggregateReply_GroupByPassesValuesThrough(t *testing.T) {
	cases := []struct {
		name      string
		in        aggregation.GroupedBy
		wantValue any
	}{
		{
			name:      "string with namespace-shaped prefix is preserved (user data)",
			in:        aggregation.GroupedBy{Path: []string{"title"}, Value: "customer1:foo"},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Text{Text: "customer1:foo"},
		},
		{
			name:      "plain string value preserved",
			in:        aggregation.GroupedBy{Path: []string{"title"}, Value: "Tigger"},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Text{Text: "Tigger"},
		},
		{
			name:      "[]string preserves every entry verbatim",
			in:        aggregation.GroupedBy{Path: []string{"tags"}, Value: []string{"customer1:tag", "Global", "customer2:tag"}},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Texts{Texts: &pb.TextArray{Values: []string{"customer1:tag", "Global", "customer2:tag"}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replier := NewAggregateReplier(nil, nil, nil)
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
