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
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/exp/metadataserver/proto/api"
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
	lsmEnsurer LSMEnsurer
	start      func()
	log        logrus.FieldLogger
}

// TODO
func NewMetadataSubscription(lsmEnsurer LSMEnsurer, metadataGRPCHost string, metadataGRPCPort int, log logrus.FieldLogger) *MetadataSubscription {
	metadataSubscription := &MetadataSubscription{
		lsmEnsurer: lsmEnsurer,
		log:        log,
	}
	// wait to call start until gRPC serving is active and use sync.Once to ensure that start is called only once
	metadataSubscription.start = sync.OnceFunc(func() {
		// Start a goroutine to subscribe to metadata node. Do this in a goroutine so that the
		// caller of this func does not block waiting for the subscription to metadata to complete.
		enterrors.GoWrapper(func() {
			backgroundCtx := context.Background()
			// Try 60s timeout for establishing rpc conn
			dialCtx, cancel := context.WithTimeout(backgroundCtx, 60*time.Second)
			defer cancel()
			metadataRpcConn, err := grpc.DialContext(
				dialCtx,
				fmt.Sprintf("%s:%d", metadataGRPCHost, metadataGRPCPort),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				log.Errorf("Error dialing metadata: %v", err)
				return
			}
			c := api.NewMetadataServiceClient(metadataRpcConn)
			stream, err := c.QuerierStream(backgroundCtx)
			// ctx := stream.Context()

			if err != nil {
				log.Errorf("Error creating querier stream: %v", err)
				return
			}
			defer stream.CloseSend()

			wg := sync.WaitGroup{}
			wg.Add(1)

			// loop to process events from the metadata node
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
						log.Warnf("Error metadata subscription recv: %v", err)
					}
					switch in.Type {
					case api.QuerierStreamResponse_TYPE_UNSPECIFIED:
						panic("unspecified") // TODO
					case api.QuerierStreamResponse_TYPE_CLASS_TENANT_DATA_UPDATE:
						// TODO locking...plan to discuss with kavi, should we download to new dir or delete/overwrite or swap or other?
						// TODO we shouldn't block here, instead we should offload the download to a worker pool or something
						// TODO only download tenants we care about (eg on disk already?)
						_, _, err = metadataSubscription.lsmEnsurer.EnsureLSM(context.TODO(), in.ClassTenant.ClassName, in.ClassTenant.TenantName, true)
						if err != nil {
							panic(err)
						}
					}
				}
			}, log)

			// currently, we're not sending any messages to the metadata nodes from the querier, we just use
			// the existence of the stream to keep the connection alive so we can receive events, we can switch to
			// a unidirectional stream later if we never want to send messages from the querier
			wg.Wait()
		}, log)
	})
	return metadataSubscription
}

func (metadataSubscription *MetadataSubscription) EnsureStarted() {
	metadataSubscription.start()
}
