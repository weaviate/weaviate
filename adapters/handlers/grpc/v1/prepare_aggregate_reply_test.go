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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// Pins the defense-in-depth strip on Reference.PointingTo for both shapes
// PointingTo can take: stored beacon URIs (PointingToAggregator path) and
// bare property.DataType class names (TypeAggregator path — the real leak
// vector on NS clusters since DataType stays qualified in storage).
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
			name:      "namespaced caller, beacon URIs: own-NS stripped, foreign preserved",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			in:        []string{mk("customer1:Animal"), mk("customer2:Plant"), mk("Global")},
			want:      []string{mk("Animal"), mk("customer2:Plant"), mk("Global")},
		},
		{
			name:      "namespaced caller, bare class (TypeAggregator path): own-NS stripped",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			in:        []string{"customer1:Animal", "customer2:Plant", "Global"},
			want:      []string{"Animal", "customer2:Plant", "Global"},
		},
		{
			name:      "global principal: qualified preserved (both shapes)",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			in:        []string{mk("customer1:Animal"), "customer1:Plant"},
			want:      []string{mk("customer1:Animal"), "customer1:Plant"},
		},
		{
			name:      "nil principal: pass-through (NS-disabled cluster)",
			principal: nil,
			in:        []string{mk("customer1:Animal"), "customer1:Plant"},
			want:      []string{mk("customer1:Animal"), "customer1:Plant"},
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

// Pins the ref-target group-by strip (isRef=true). The bucket value is a
// beacon URI from MultipleRef.Beacon. The grouper normalizes it to a plain
// string (`in`), but a stray strfmt.URI (local shard that skipped JSON) must
// strip identically, so each case is run through both dynamic types. The
// caller's own "<ns>:" is stripped from the embedded class; foreign prefixes
// and short beacons are left intact.
func TestGRPCAggregateReply_GroupByOnRefTargetStripsOwnNamespace(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"
	mk := func(class string) string {
		return "weaviate://localhost/" + class + "/" + uuid
	}
	cases := []struct {
		name      string
		principal *models.Principal
		in        string
		wantText  string
	}{
		{
			name:      "namespaced caller: own-NS stripped from embedded class",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			in:        mk("customer1:Animal"),
			wantText:  mk("Animal"),
		},
		{
			name:      "namespaced caller: foreign-NS preserved",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			in:        mk("customer2:Animal"),
			wantText:  mk("customer2:Animal"),
		},
		{
			name:      "namespaced caller: already-short beacon unchanged",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			in:        mk("Animal"),
			wantText:  mk("Animal"),
		},
		{
			name:      "global principal: qualified beacon preserved",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			in:        mk("customer1:Animal"),
			wantText:  mk("customer1:Animal"),
		},
		{
			name:      "nil principal: pass-through (NS-disabled cluster)",
			principal: nil,
			in:        mk("customer1:Animal"),
			wantText:  mk("customer1:Animal"),
		},
		{
			name:      "unparseable URI: passed through unchanged",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			in:        "not-a-real-beacon",
			wantText:  "not-a-real-beacon",
		},
	}
	for _, tc := range cases {
		// string is the grouper's normalized output (and the remote-shard
		// JSON shape); strfmt.URI is the defensive local-shard path.
		for _, val := range []interface{}{tc.in, strfmt.URI(tc.in)} {
			t.Run(tc.name, func(t *testing.T) {
				replier := NewAggregateReplier(tc.principal, nil, nil)
				got, err := replier.parseAggregateGroupedBy(&aggregation.GroupedBy{
					Path:  []string{"hasAnimals"},
					Value: val,
				}, true)
				require.NoError(t, err)
				require.NotNil(t, got)
				textVal, ok := got.Value.(*pb.AggregateReply_Group_GroupedBy_Text)
				require.True(t, ok, "expected Text value, got %T", got.Value)
				assert.Equal(t, tc.wantText, textVal.Text)
			})
		}
	}
}

// TestGRPCAggregateReply_GroupByPassesValuesThrough pins that non-ref bucket
// values flow unchanged (isRef=false). Group-by values can be arbitrary user
// text (e.g. "customer1:foo"), so we must not rewrite them — including a
// value that merely looks like a beacon, which proves the strip keys off
// schema ref-ness, not value shape.
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
			name:      "beacon-shaped user text is NOT stripped when prop is non-ref",
			in:        aggregation.GroupedBy{Path: []string{"title"}, Value: "weaviate://localhost/customer1:Foo/11111111-2222-3333-4444-555555555555"},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Text{Text: "weaviate://localhost/customer1:Foo/11111111-2222-3333-4444-555555555555"},
		},
		{
			name:      "[]string preserves every entry verbatim",
			in:        aggregation.GroupedBy{Path: []string{"tags"}, Value: []string{"customer1:tag", "Global", "customer2:tag"}},
			wantValue: &pb.AggregateReply_Group_GroupedBy_Texts{Texts: &pb.TextArray{Values: []string{"customer1:tag", "Global", "customer2:tag"}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replier := NewAggregateReplier(&models.Principal{Username: "u", Namespace: "customer1"}, nil, nil)
			got, err := replier.parseAggregateGroupedBy(&tc.in, false)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tc.wantValue, got.Value)
			assert.Equal(t, tc.in.Path, got.Path)
		})
	}
}

// Pins that ref-ness is resolved from the schema (so it survives the
// remote-shard JSON round-trip that collapses strfmt.URI → string), not from
// the bucket value's dynamic type.
func TestGRPCAggregateReply_GroupByIsRef(t *testing.T) {
	class := &models.Class{
		Class: "Zoo",
		Properties: []*models.Property{
			{Name: "hasAnimals", DataType: []string{"Animal"}},
			{Name: "name", DataType: []string{"text"}},
		},
	}
	getClass := func(string) (*models.Class, error) { return class, nil }
	replier := NewAggregateReplier(nil, getClass, &aggregation.Params{ClassName: "Zoo"})

	assert.True(t, replier.groupByIsRef([]string{"hasAnimals"}), "ref prop")
	assert.False(t, replier.groupByIsRef([]string{"name"}), "text prop")
	assert.False(t, replier.groupByIsRef([]string{"missing"}), "unknown prop fails closed")
	assert.False(t, replier.groupByIsRef(nil), "empty path fails closed")
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
