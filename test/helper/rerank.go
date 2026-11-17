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

package helper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// AssertRerankRequestChecks sends the gRPC searches that the server rejects
// before the search, whichever reranker the collection uses: a rerank without
// a query, one about a property that is not text, and one about a property
// the collection does not have. No request reaches the reranker's API.
func AssertRerankRequestChecks(t *testing.T, collection, textProperty, textArrayProperty string) {
	t.Helper()
	query := "any"
	tests := []struct {
		name    string
		rerank  *pb.Rerank
		wantErr string
	}{
		{
			name:    "a rerank without a query",
			rerank:  &pb.Rerank{Property: textProperty},
			wantErr: "rerank: query is required",
		},
		{
			name:    "a rerank about a text array property",
			rerank:  &pb.Rerank{Property: textArrayProperty, Query: &query},
			wantErr: "is of type text[]; a rerank reads a text or blob property",
		},
		{
			name:    "a rerank about a property the collection does not have",
			rerank:  &pb.Rerank{Property: "notAProperty", Query: &query},
			wantErr: "rerank: no such prop with name 'notAProperty' found in class",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ClientGRPC(t).Search(context.Background(), &pb.SearchRequest{
				Collection:  collection,
				Rerank:      tt.rerank,
				Uses_127Api: true,
			})

			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
