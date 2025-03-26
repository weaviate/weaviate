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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestBatchDeleteRequest(t *testing.T) {
	collection := "TestClass"
	scheme := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: collection,
					Properties: []*models.Property{
						{Name: "name", DataType: schema.DataTypeText.PropString()},
					},
				},
			},
		},
	}

	getClass := func(name string) (*models.Class, error) {
		return scheme.GetClass(name), nil
	}

	simpleFilterOutput := &filters.LocalFilter{
		Root: &filters.Clause{
			On:       &filters.Path{Class: schema.ClassName(collection), Property: "name"},
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Value: "test", Type: schema.DataTypeText},
		},
	}
	simpleFilterInput := &pb.Filters{Operator: pb.Filters_OPERATOR_EQUAL, TestValue: &pb.Filters_ValueText{ValueText: "test"}, Target: &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "name"}}}

	tests := []struct {
		name  string
		req   *pb.BatchDeleteRequest
		out   objects.BatchDeleteParams
		error error
	}{
		{
			name: "simple filter",
			req: &pb.BatchDeleteRequest{
				Collection: collection,
				Filters:    simpleFilterInput,
			},
			out: objects.BatchDeleteParams{
				ClassName: schema.ClassName(collection),
				DryRun:    false,
				Output:    "minimal",
				Filters:   simpleFilterOutput,
			},
			error: nil,
		},
		{
			name:  "collection does not exist",
			req:   &pb.BatchDeleteRequest{Collection: "does not exist"},
			error: errors.New("could not find class does not exist in schema"),
		},
		{
			name:  "no filter",
			req:   &pb.BatchDeleteRequest{Collection: collection},
			error: fmt.Errorf("no filters in batch delete request"),
		},
		{
			name: "dry run",
			req: &pb.BatchDeleteRequest{
				Collection: collection,
				Filters:    simpleFilterInput,
				DryRun:     true,
			},
			out: objects.BatchDeleteParams{
				ClassName: schema.ClassName(collection),
				DryRun:    true,
				Output:    "minimal",
				Filters:   simpleFilterOutput,
			},
			error: nil,
		},
		{
			name: "verbose",
			req: &pb.BatchDeleteRequest{
				Collection: collection,
				Filters:    simpleFilterInput,
				DryRun:     false,
				Verbose:    true,
			},
			out: objects.BatchDeleteParams{
				ClassName: schema.ClassName(collection),
				DryRun:    false,
				Output:    "verbose",
				Filters:   simpleFilterOutput,
			},
			error: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := batchDeleteParamsFromProto(tt.req, getClass)
			require.Equal(t, tt.error, err)

			if tt.error == nil {
				require.Equal(t, tt.out, out)
			}
		})
	}
}

var (
	errorString   = "error"
	noErrorString = ""
)

func TestBatchDeleteReply(t *testing.T) {
	tests := []struct {
		name     string
		response objects.BatchDeleteResult
		verbose  bool
		out      *pb.BatchDeleteReply
	}{
		{
			name:     "single object",
			response: objects.BatchDeleteResult{Matches: 1, Objects: objects.BatchSimpleObjects{{UUID: UUID1, Err: nil}}},
			out:      &pb.BatchDeleteReply{Matches: 1, Successful: 1, Failed: 0},
		},
		{
			name:     "single object with err",
			response: objects.BatchDeleteResult{Matches: 1, Objects: objects.BatchSimpleObjects{{UUID: UUID1, Err: errors.New("error")}}},
			out:      &pb.BatchDeleteReply{Matches: 1, Successful: 0, Failed: 1},
		},
		{
			name:     "one error, one successful",
			response: objects.BatchDeleteResult{Matches: 2, Objects: objects.BatchSimpleObjects{{UUID: UUID1, Err: errors.New("error")}, {UUID: UUID2, Err: nil}}},
			out:      &pb.BatchDeleteReply{Matches: 2, Successful: 1, Failed: 1},
		},
		{
			name:     "one error, one successful - with verbosity",
			response: objects.BatchDeleteResult{Matches: 2, Objects: objects.BatchSimpleObjects{{UUID: UUID1, Err: errors.New("error")}, {UUID: UUID2, Err: nil}}},
			verbose:  true,
			out: &pb.BatchDeleteReply{Matches: 2, Successful: 1, Failed: 1, Objects: []*pb.BatchDeleteObject{
				{Uuid: idByte(string(UUID1)), Successful: false, Error: &errorString},
				{Uuid: idByte(string(UUID2)), Successful: true, Error: &noErrorString},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := batchDeleteReplyFromObjects(tt.response, tt.verbose)
			require.Nil(t, err)
			require.Equal(t, tt.out, out)
		})
	}
}
