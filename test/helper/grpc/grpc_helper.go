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

package grpchelper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
)

func Search(t *testing.T, req *pb.SearchRequest) (*pb.SearchReply, error) {
	return helper.ClientGRPC(t).Search(context.Background(), req)
}

func SearchWithTimeout(t *testing.T, req *pb.SearchRequest, timeout time.Duration) (*pb.SearchReply, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return helper.ClientGRPC(t).Search(ctx, req)
}

func AssertSearch(t *testing.T, req *pb.SearchRequest) *pb.SearchReply {
	t.Helper()
	resp, err := Search(t, req)
	require.Nil(t, err)
	return resp
}

func AssertSearchWithTimeout(t *testing.T, req *pb.SearchRequest, timeout time.Duration) *pb.SearchReply {
	t.Helper()
	resp, err := SearchWithTimeout(t, req, timeout)
	if err != nil {
		t.Errorf("SearchWithTimeout failed: %v", err)
	}
	return resp
}

func ToPtr[T any](val T) *T {
	return &val
}
