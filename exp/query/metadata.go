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

package query

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

// MetadataSubscription allows this querier node to create a gRPC connection to a metadata node
// and subscribe to class tenant data updates. When a class tenant data update is received, the
// querier node will download the updated class tenant data via the offload module.
// The EnsureStarted method must be called to start the subscription after the gRPC server is active.
// EnsureStarted uses sync.Once to ensure that the subscription is started only once, even if it
// is called multiple times.
type MetadataSubscription struct {
	offload *modsloads3.Module
	start   func()
}

func NewMetadataSubscription(offload *modsloads3.Module, metadataGRPCHost string, metadataGRPCPort int) *MetadataSubscription {
	metadataSubscription := &MetadataSubscription{
		offload: offload,
	}
	// wait to call start until gRPC serving is active and use sync.Once to ensure that start is called only once
	metadataSubscription.start = sync.OnceFunc(func() {
		// Start a goroutine to subscribe to metadata node. Do this in a goroutine so that the
		// gRPC server can keep serving requests before we start subscribing to metadata.
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
			stream, err := c.QuerierStream(context.Background())
			if err != nil {
				panic(err)
			}
			defer stream.CloseSend()

			// TODO need to think more about how this and server side stuff handles context, concurrency, timeouts, etc
			wg := sync.WaitGroup{}
			wg.Add(1)

			// process events from the metadata node
			enterrors.GoWrapper(func() {
				defer wg.Done()
				for {
					in, err := stream.Recv()
					if err == io.EOF {
						// this error is expected when the stream is closed by the metadata node
						return
					}
					if err != nil {
						// unexpected error
						log.Fatalf("Failed to receive a note : %v", err)
						panic(err)
					}
					switch in.Type {
					case protoapi.QuerierStreamResponse_TYPE_UNSPECIFIED:
						panic("unspecified")
					case protoapi.QuerierStreamResponse_TYPE_CLASS_TENANT_DATA_UPDATE:
						// TODO locking...plan to discuss with kavi, should we download to new dir or delete/overwrite or swap or other?
						err = metadataSubscription.offload.Download(context.TODO(), in.ClassTenant.ClassName, in.ClassTenant.TenantName, nodeName)
						if err != nil {
							panic(err)
						}
					}
				}
			}, logrus.New()) // TODO replace logrus.New here and other

			// currently, we're not sending any messages to the metadata nodes from the querier, we just use
			// the existence of the stream to keep the connection alive so we can receive events, we can switch to
			// a unidirectional stream later if we never want to send messages from the querier
			wg.Wait()
		}, logrus.New())
	})
	return metadataSubscription
}

func (metadataSubscription *MetadataSubscription) EnsureStarted() {
	metadataSubscription.start()
}

// TODO get rid of this, hacky workaround to make local testing easy
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
