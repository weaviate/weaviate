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
type MetadataSubscription struct {
	lsmEnsurer          LSMEnsurer
	metadataGRPCAddress string
	log                 logrus.FieldLogger
}

func NewMetadataSubscription(lsmEnsurer LSMEnsurer, metadataGRPCAddress string, log logrus.FieldLogger) *MetadataSubscription {
	return &MetadataSubscription{
		lsmEnsurer:          lsmEnsurer,
		metadataGRPCAddress: metadataGRPCAddress,
		log:                 log,
	}
}

func (m *MetadataSubscription) Start() error {
	backgroundCtx := context.Background()

	// Try 60s timeout for establishing rpc conn
	dialCtx, cancel := context.WithTimeout(backgroundCtx, 60*time.Second)
	defer cancel()
	// TODO replace DialContex with NewClient
	//nolint:staticcheck
	metadataRpcConn, err := grpc.DialContext(
		dialCtx,
		m.metadataGRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	c := api.NewMetadataServiceClient(metadataRpcConn)
	stream, err := c.QuerierStream(backgroundCtx)
	if err != nil {
		return err
	}
	defer stream.CloseSend()
	ctx := stream.Context()

	wg := sync.WaitGroup{}
	wg.Add(1)

	// loop to process events from the metadata node (TODO break this out into separate func)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// this error is expected when the stream is closed by the metadata node
				return
			}
			if err != nil {
				// unexpected error, stream aborted
				m.log.Warnf("error metadata subscription recv: %v", err)
				return
			}
			switch in.Type {
			case api.QuerierStreamResponse_TYPE_UNSPECIFIED:
				m.log.Errorf("got an unspecified type in the metadata subscription stream")
			case api.QuerierStreamResponse_TYPE_CLASS_TENANT_DATA_UPDATE:
				// TODO don't block on download, use worker pool/rate limit
				_, _, err = m.lsmEnsurer.EnsureLSM(ctx, in.ClassTenant.ClassName, in.ClassTenant.TenantName, true)
				if err != nil {
					m.log.Warnf("did not ensure lsm in metadata subscription: %s, %s, %v", in.ClassTenant.ClassName, in.ClassTenant.TenantName, err)
				}
			}
		}
	}, m.log)

	// currently, we're not sending any messages to the metadata nodes from the querier, we just use
	// the existence of the stream to keep the connection alive so we can receive events, we can switch to
	// a unidirectional stream later if we never want to send messages from the querier
	wg.Wait()
	return nil
}
