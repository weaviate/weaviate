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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// parse_search_request.go passes the operator through as pb enum.String(),
// relying on the enum names matching the common_filters constants exactly.
func TestSearchOperatorStringMatchesCommonFilters(t *testing.T) {
	tests := []struct {
		name     string
		operator pb.SearchOperatorOptions_Operator
		want     string
	}{
		{
			name:     "OR",
			operator: pb.SearchOperatorOptions_OPERATOR_OR,
			want:     common_filters.SearchOperatorOr,
		},
		{
			name:     "AND",
			operator: pb.SearchOperatorOptions_OPERATOR_AND,
			want:     common_filters.SearchOperatorAnd,
		},
		{
			name:     "AND_CROSS",
			operator: pb.SearchOperatorOptions_OPERATOR_AND_CROSS,
			want:     common_filters.SearchOperatorAndCross,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.operator.String())
		})
	}
}
