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
	"fmt"

	"github.com/sirupsen/logrus"
	httpv1 "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/v1/http"
	grpc "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/v2/grpc"
	httpv2 "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/v2/http"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
)

func New(protocol ent.Protocol, logger logrus.FieldLogger) (Client, error) {
	switch protocol {
	case ent.KSERVE_GRPC:
		return Client(grpc.NewGRPCClient(logger)), nil
	case ent.KSERVE_HTTP_V1:
		return Client(httpv1.NewHTTPV1Client(logger)), nil
	case ent.KSERVE_HTTP_V2:
		return Client(httpv2.NewHTTPV2Client(logger)), nil
	}

	return nil, fmt.Errorf("protocol %v not implemented", protocol)
}
