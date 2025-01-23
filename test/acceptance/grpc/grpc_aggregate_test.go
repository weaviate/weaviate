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

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/cities"
)

func TestGRPC_Aggregate(t *testing.T) {
	ctx := context.Background()

	host := "localhost:8080"
	helper.SetupClient(host)

	grpcClient, _ := newClient(t)
	require.NotNil(t, grpcClient)

	cities.CreateCountryCityAirportSchema(t, host)
	cities.InsertCountryCityAirportObjects(t, host)
	defer cities.DeleteCountryCityAirportSchema(t, host)

	t.Run("meta count", func(t *testing.T) {
		tests := []struct {
			collection string
			count      int64
		}{
			{collection: cities.Country, count: 2},
			{collection: cities.City, count: 6},
			{collection: cities.Airport, count: 4},
		}
		for _, tt := range tests {
			t.Run(tt.collection, func(t *testing.T) {
				resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
					Collection: tt.collection,
					MetaCount:  true,
				})
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.NotNil(t, resp.Result)
				require.Len(t, resp.Result.Groups, 1)
				require.Equal(t, tt.count, resp.Result.Groups[0].Count)
			})
		}
	})
}
