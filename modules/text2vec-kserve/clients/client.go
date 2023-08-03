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

	"github.com/sirupsen/logrus"
	grpc "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/grpc"
	httpv1 "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/httpv1"
	httpv2 "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/httpv2"

	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
)

type Client interface {
	Vectorize(ctx context.Context, input string,
		config ent.ModuleConfig) (*ent.VectorizationResult, error)
}

type Validator interface {
	Validate(ctx context.Context, config ent.ModuleConfig) error
}

type ValidatorFactory interface {
	ToValidator(protocol ent.Protocol) (Validator, error)
}

var (
	_ = Client(grpc.NewGRPCClient(logrus.New()))
	_ = Client(httpv1.NewHTTPV1Client(logrus.New()))
	_ = Client(httpv2.NewHTTPV2Client(logrus.New()))
)

var (
	_ = Validator(grpc.NewGRPCClient(logrus.New()))
	_ = Validator(httpv2.NewHTTPV2Client(logrus.New()))
)
