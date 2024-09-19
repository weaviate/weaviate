package metadata

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	protoapi "github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	nodeName = "weaviate-0"
)

type MetadataSubscription struct {
	offload *modsloads3.Module
	start   func()
}

func NewMetadataSubscription(offload *modsloads3.Module, metadataGRPCHost string, metadataGRPCPort int) *MetadataSubscription {
	btd := &MetadataSubscription{
		offload: offload,
	}
	// wait to call start until gRPC serving is active
	btd.start = sync.OnceFunc(func() {
		enterrors.GoWrapper(func() {
			if metadataGRPCHost == "" {
				metadataGRPCHost = getOutboundIP()
			}
			ctx := context.TODO()
			leaderRpcConn, err := grpc.DialContext(
				ctx,
				fmt.Sprintf("%s:%d", metadataGRPCHost, metadataGRPCPort),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				panic(err)
			}
			c := protoapi.NewClusterServiceClient(leaderRpcConn)
			stream, err := c.QuerierRegister(context.Background())
			if err != nil {
				panic(err)
			}
			defer stream.CloseSend()

			// TODO need to think more about how this and server side stuff handles context, concurrency, timeouts, etc
			wg := sync.WaitGroup{}
			wg.Add(1)

			// process events from metadata
			go func() {
				defer wg.Done()
				for {
					in, err := stream.Recv()
					if err == io.EOF {
						return
					}
					if err != nil {
						log.Fatalf("Failed to receive a note : %v", err)
						panic(err)
					}
					switch in.Type {
					case protoapi.MetadataEvent_UNSPECIFIED:
						panic("unspecified")
					case protoapi.MetadataEvent_CLASS_TENANT_DATA_UPDATE:
						// TODO locking...plan to discuss with kavi, should we download to new dir or delete/overwrite or swap?
						err = btd.offload.Download(context.TODO(), in.ClassTenant.ClassName, in.ClassTenant.TenantName, nodeName)
						if err != nil {
							panic(err)
						}
					}
				}
			}()

			// currently, we're not sending any messages to the metadata nodes from the querier, we just use
			// the existence of the stream to keep the connection alive so we can receive events, we can switch to
			// a unidirectional stream later if we never want to send messages from the querier
			wg.Wait()
		}, logrus.New())
	})
	return btd
}

func (btd *MetadataSubscription) EnsureStarted() {
	// TODO handle panic
	btd.start()
}

// Get preferred outbound ip of this machine
// https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr()

	return strings.Split(localAddr.String(), ":")[0]
}
