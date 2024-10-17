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
	"github.com/weaviate/weaviate/exp/metadata/proto/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MetadataSubscription allows this querier node to create a gRPC connection to a metadata node
// and subscribe to class tenant data updates. When a class tenant data update is received, the
// querier node will download the updated class tenant data via the offload module.
type MetadataSubscription struct {
	api                 *API
	metadataGRPCAddress string
	log                 logrus.FieldLogger
}

func NewMetadataSubscription(api *API, metadataGRPCAddress string, log logrus.FieldLogger) *MetadataSubscription {
	return &MetadataSubscription{
		api:                 api,
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
	m.log.WithField("metadataGRPCAddress", m.metadataGRPCAddress).Debug("connected to metadata node")
	ctx := stream.Context()

	wg := sync.WaitGroup{}
	wg.Add(1)

	// loop to process events from the metadata node (TODO break this out into separate func)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				// this error is expected when the stream is closed by the metadata node
				m.log.Debug("metadata subscription stream closed")
				return
			}
			if err != nil {
				// unexpected error, stream aborted TODO try to reconnect?
				m.log.WithError(err).Warn("error metadata subscription recv")
				return
			}
			switch resp.Type {
			case api.QuerierStreamResponse_TYPE_UNSPECIFIED:
				m.log.WithField("repsonse", resp).Error("got an unspecified type in the metadata subscription stream")
			case api.QuerierStreamResponse_TYPE_CLASS_TENANT_DATA_UPDATE:
				// TODO don't block on download, use worker pool/rate limit
				_, _, err = m.api.EnsureLSM(ctx, resp.ClassTenant.ClassName, resp.ClassTenant.TenantName, true)
				if err != nil {
					m.log.WithFields(logrus.Fields{
						"className":  resp.ClassTenant.ClassName,
						"tenantName": resp.ClassTenant.TenantName,
					}).WithError(err).Info("did not ensure lsm in metadata subscription")
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
