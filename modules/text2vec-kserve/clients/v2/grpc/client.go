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
	"bytes"
	"context"
	"fmt"
	"sync"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/modules/text2vec-kserve/clients/v2/grpc/codegen"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcConnectionArgs struct {
	dialOpts []grpc.DialOption
	callOpts []grpc.CallOption
}

type GrpcClient struct {
	grpcClients map[string]*codegen.GRPCInferenceServiceClient
	logger      logrus.FieldLogger
	rwLock      sync.RWMutex
}

func NewGRPCClient(logger logrus.FieldLogger) *GrpcClient {
	return &GrpcClient{
		grpcClients: map[string]*codegen.GRPCInferenceServiceClient{},
		logger:      logger,
		rwLock:      sync.RWMutex{},
	}
}

func (c *GrpcClient) Vectorize(ctx context.Context, input string,
	config ent.ModuleConfig,
) (*ent.VectorizationResult, error) {
	if config.Protocol != ent.KSERVE_GRPC {
		return nil, fmt.Errorf("`vectorize` was called with wrong protocol %v", config.Protocol)
	}

	connArgs, err := toGrpcConnectionArgs(config.ConnectionArgs)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing gRPC connection args")
	}
	conn, ok := c.connection(config.Url)
	if !ok {
		newConn, err := c.createConnection(config.Url, connArgs.dialOpts)
		if err != nil {
			return nil, err
		}
		conn = newConn
	}
	return c.vectorize(ctx, conn, input, config)
}

func (c *GrpcClient) connection(target string) (*codegen.GRPCInferenceServiceClient, bool) {
	c.rwLock.RLock()
	conn, ok := c.grpcClients[target]
	defer c.rwLock.RUnlock()
	return conn, ok
}

func (c *GrpcClient) setConnection(target string, conn *codegen.GRPCInferenceServiceClient) {
	c.rwLock.Lock()
	c.grpcClients[target] = conn
	defer c.rwLock.Unlock()
}

func (c *GrpcClient) createConnection(target string, dialOpts []grpc.DialOption) (*codegen.GRPCInferenceServiceClient, error) {
	var (
		conn *grpc.ClientConn
		err  error
	)
	if len(dialOpts) > 1 {
		conn, err = grpc.Dial(target, dialOpts...)
	} else {
		conn, err = grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if err != nil {
		return nil, err
	}
	client := codegen.NewGRPCInferenceServiceClient(conn)

	c.setConnection(target, &client)

	return &client, err
}

func toGrpcConnectionArgs(args map[string]interface{}) (*grpcConnectionArgs, error) {
	dialOpts := []grpc.DialOption{}

	dialConfig := args["dialOptions"].(map[string]interface{})
	if dialConfig["insecure"].(bool) {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	return &grpcConnectionArgs{
		dialOpts: dialOpts,
		callOpts: []grpc.CallOption{},
	}, nil
}

func (c *GrpcClient) vectorize(ctx context.Context, client *codegen.GRPCInferenceServiceClient,
	input string, settings ent.ModuleConfig,
) (*ent.VectorizationResult, error) {
	request := makeInferRequest(input, settings.Model, settings.Version, settings.Input, input, settings.Output)
	resp, err := (*client).ModelInfer(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("gRPC request failed, %v", err)
	}

	if len(resp.RawOutputContents) > 1 {
		return nil, errors.New("response contains more than one embedding")
	}

	outputs := resp.RawOutputContents[0]
	embedding, err := byteArrayToFloatArray(outputs, settings.EmbeddingDims)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response, %v", err)
	}

	return &ent.VectorizationResult{
		Text:       input,
		Dimensions: len(*embedding),
		Vector:     *embedding,
	}, nil
}

func byteArrayToFloatArray(arr []byte, floatArrLen int32) (*[]float32, error) {
	byteArrLen := len(arr)
	if byteArrLen == 0 {
		return nil, nil
	}
	if floatArrLen*4 != int32(byteArrLen) {
		return nil, errors.New("misaligned byte array")
	}
	ptr := unsafe.Pointer(&arr[0])

	type float32Slice = [1 << 30]float32
	result := (*float32Slice)((*float32Slice)(ptr))[:floatArrLen:floatArrLen]
	return &result, nil
}

func makeInferRequest(s string, model string, version string, inputName string, inputValue string, outputName string) *codegen.ModelInferRequest {
	byte_input := stringToByteTensor(inputValue)
	request := codegen.ModelInferRequest{
		ModelName:    model,
		ModelVersion: version,
		Inputs: []*codegen.ModelInferRequest_InferInputTensor{
			{
				Name:     inputName,
				Shape:    []int64{1, 1},
				Datatype: "BYTES",
				Contents: &codegen.InferTensorContents{
					BytesContents: [][]byte{byte_input},
				},
			},
		},
		Outputs: []*codegen.ModelInferRequest_InferRequestedOutputTensor{
			{
				Name: outputName,
			},
		},
	}
	return &request
}

func stringToByteTensor(s string) []byte {
	buffer := bytes.Buffer{}

	buffer.WriteString(s)
	return buffer.Bytes()
}
