package clients

import (
	"fmt"

	"github.com/sirupsen/logrus"
	grpc "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/grpc"
	httpv1 "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/httpv1"
	httpv2 "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/httpv2"
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
