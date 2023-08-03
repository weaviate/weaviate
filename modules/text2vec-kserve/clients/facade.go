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

package clients

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	grpc "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/grpc"
	httpv1 "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/httpv1"
	httpv2 "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/httpv2"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
)

type clientFacade struct {
	grpc   *grpc.GrpcClient
	httpV1 Client
	httpV2 *httpv2.HttpV2Client
}

func NewClientFacade(logger logrus.FieldLogger) *clientFacade {
	return &clientFacade{
		grpc:   grpc.NewGRPCClient(logger),
		httpV1: httpv1.NewHTTPV1Client(logger),
		httpV2: httpv2.NewHTTPV2Client(logger),
	}
}

// Vectorize implements Client.
func (c *clientFacade) Vectorize(ctx context.Context, input string, config ent.ModuleConfig) (*ent.VectorizationResult, error) {
	switch config.Protocol {
	case ent.KSERVE_GRPC:
		return c.grpc.Vectorize(ctx, input, config)
	case ent.KSERVE_HTTP_V1:
		return c.httpV1.Vectorize(ctx, input, config)
	case ent.KSERVE_HTTP_V2:
		return c.httpV2.Vectorize(ctx, input, config)
	}

	return nil, fmt.Errorf("unsupported protocol %v", config.Protocol)
}

func (c *clientFacade) ToValidator(protocol ent.Protocol) (Validator, error) {
	switch protocol {
	case ent.KSERVE_GRPC:
		return Validator(c.grpc), nil
	case ent.KSERVE_HTTP_V2:
		return Validator(c.httpV2), nil
	}

	return nil, fmt.Errorf("validation unsupported for protocol %v", protocol)
}

var (
	_ = Client(NewClientFacade(logrus.New()))
	_ = ValidatorFactory(NewClientFacade(logrus.New()))
)
