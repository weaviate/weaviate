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
	"math/big"
	"strings"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/objects"
)

func batchDeleteParamsFromProto(req *pb.BatchDeleteRequest, authorizedGetClass classGetterWithAuthzFunc) (objects.BatchDeleteParams, error) {
	params := objects.BatchDeleteParams{}

	tenant := ""
	if req.Tenant != nil {
		tenant = *req.Tenant
	}
	// make sure collection exists
	class, err := authorizedGetClass(req.Collection)
	if err != nil {
		return params, err
	}
	if class == nil {
		return objects.BatchDeleteParams{}, fmt.Errorf("could not find class %s in schema", req.Collection)
	}

	params.ClassName = schema.ClassName(req.Collection)

	if req.Verbose {
		params.Output = "verbose"
	} else {
		params.Output = "minimal"
	}

	params.DryRun = req.DryRun

	if req.Filters == nil {
		return objects.BatchDeleteParams{}, fmt.Errorf("no filters in batch delete request")
	}

	clause, err := ExtractFilters(req.Filters, authorizedGetClass, req.Collection, tenant)
	if err != nil {
		return objects.BatchDeleteParams{}, err
	}
	filter := &filters.LocalFilter{Root: &clause}
	if err := filters.ValidateFilters(authorizedGetClass, filter); err != nil {
		return objects.BatchDeleteParams{}, err
	}
	params.Filters = filter

	return params, nil
}

func batchDeleteReplyFromObjects(response objects.BatchDeleteResult, verbose bool) (*pb.BatchDeleteReply, error) {
	var successful, failed int64

	var objs []*pb.BatchDeleteObject
	if verbose {
		objs = make([]*pb.BatchDeleteObject, 0, len(response.Objects))
	}
	for _, obj := range response.Objects {
		if obj.Err == nil {
			successful += 1
		} else {
			failed += 1
		}
		if verbose {
			hexInteger, success := new(big.Int).SetString(strings.ReplaceAll(obj.UUID.String(), "-", ""), 16)
			if !success {
				return nil, fmt.Errorf("failed to parse hex string to integer")
			}
			errorString := ""
			if obj.Err != nil {
				errorString = obj.Err.Error()
			}

			resultObj := &pb.BatchDeleteObject{
				Uuid:       hexInteger.Bytes(),
				Successful: obj.Err == nil,
				Error:      &errorString,
			}
			objs = append(objs, resultObj)
		}
	}
	reply := &pb.BatchDeleteReply{
		Successful: successful,
		Failed:     failed,
		Matches:    response.Matches,
		Objects:    objs,
	}

	return reply, nil
}
