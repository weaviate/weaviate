//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package grpc

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	utils "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/utils"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/clients/v2/grpc/codegen"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
)

func (c *GrpcClient) Validate(ctx context.Context, config ent.ModuleConfig) error {
	if config.Protocol != ent.KSERVE_GRPC {
		return fmt.Errorf("`vectorize` was called with wrong protocol %v", config.Protocol)
	}

	connArgs, err := toGrpcConnectionArgs(config.ConnectionArgs)
	if err != nil {
		return errors.Wrap(err, "error parsing gRPC connection args")
	}

	conn, ok := c.connection(config.Url)
	if !ok {
		newConn, err := c.createConnection(config.Url, connArgs.dialOpts)
		if err != nil {
			return err
		}
		conn = newConn
	}

	resp, err := c.modelMetadata(ctx, *conn, config.Model, config.Version)
	if err != nil {
		return err
	}

	return validate(*resp, config)
}

func (c *GrpcClient) modelMetadata(ctx context.Context, conn codegen.GRPCInferenceServiceClient, modelName string, modelVersion string) (*codegen.ModelMetadataResponse, error) {
	request := codegen.ModelMetadataRequest{
		Name:    modelName,
		Version: modelVersion,
	}
	resp, err := conn.ModelMetadata(ctx, &request)
	if err != nil {
		return nil, errors.Wrap(err, "model metadata request")
	}

	return resp, nil
}

func validate(metadata codegen.ModelMetadataResponse, config ent.ModuleConfig) error {
	inputTensor := findTensor(metadata.Inputs, config.Input)
	if inputTensor == nil {
		return fmt.Errorf("no input %v for model %v", config.Input, config.Model)
	}

	if !utils.TensorShapeEqual(inputTensor.Shape, []int64{-1, 1}) {
		return fmt.Errorf("incorrect shape %v on input %v, expected [-1 %v]", inputTensor.Shape, inputTensor.Name, 1)
	}

	if inputTensor.Datatype != "BYTES" {
		return fmt.Errorf("incorrect datatype %v on input tensor %v, expected BYTES", inputTensor.Datatype, inputTensor.Name)
	}

	outputTensor := findTensor(metadata.Outputs, config.Output)
	if outputTensor == nil {
		return fmt.Errorf("no output %v for model %v", config.Output, config.Model)
	}

	if !utils.TensorShapeEqual(outputTensor.Shape, []int64{-1, int64(config.EmbeddingDims)}) {
		return fmt.Errorf("incorrect shape %v on output %v, expected [-1, %v]", outputTensor.Shape, outputTensor.Name, config.EmbeddingDims)
	}

	if outputTensor.Datatype != "FP32" {
		return fmt.Errorf("incorrect datatype %v on output tensor %v, expected FP32", outputTensor.Datatype, outputTensor.Name)
	}

	return nil
}

func findTensor(tensors []*codegen.ModelMetadataResponse_TensorMetadata, tensorName string) *codegen.ModelMetadataResponse_TensorMetadata {
	for _, i := range tensors {
		if i.Name == tensorName {
			return i
		}
	}
	return nil
}
