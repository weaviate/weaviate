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

package metadataserver

import (
	"fmt"
	"io"
	"net"
	"sync"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/exp/metadataserver/proto/api"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_sentry "github.com/johnbellone/grpc-middleware-sentry"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Server struct {
	grpcServer                *grpc.Server
	listenAddress             string
	grpcMessageMaxSize        int
	sentryEnabled             bool
	querierManager            *QuerierManager
	dataEventsChannelCapacity int
	log                       *logrus.Logger
}

func NewServer(listenAddress string, grpcMessageMaxSize int,
	sentryEnabled bool, querierManager *QuerierManager, dataEventsChannelCapacity int, log *logrus.Logger,
) *Server {
	return &Server{
		listenAddress:             listenAddress,
		grpcMessageMaxSize:        grpcMessageMaxSize,
		sentryEnabled:             sentryEnabled,
		querierManager:            querierManager,
		dataEventsChannelCapacity: dataEventsChannelCapacity,
		log:                       log,
	}
}

// Open starts the server and registers it as the cluster service server. Blocking.
// Returns an error if the configured listenAddress is invalid.
// Returns an error if the configured listenAddress is un-usable to listen on.
func (s *Server) Open() error {
	s.log.WithField("address", s.listenAddress).Info("starting cloud rpc server ...")
	if s.listenAddress == "" {
		return fmt.Errorf("address of rpc server cannot be empty")
	}

	// Note listen uses context.background by default, can set if needed
	listener, err := net.Listen("tcp", s.listenAddress)
	if err != nil {
		return fmt.Errorf("server tcp net.listen: %w", err)
	}

	var options []grpc.ServerOption
	options = append(options, grpc.MaxRecvMsgSize(s.grpcMessageMaxSize))
	if s.sentryEnabled {
		options = append(options,
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				grpc_sentry.UnaryServerInterceptor(),
			)))
	}
	s.grpcServer = grpc.NewServer(options...)
	api.RegisterMetadataServiceServer(s.grpcServer, s)
	if err := s.grpcServer.Serve(listener); err != nil {
		return err
	}
	return nil
}

// QuerierStream is experimental. QuerierStream is triggered when a querier node connects
// to this metadata node via this bidirectional gRPC stream. We register the querier node
// with the querier manager and send class tenant data updates to the querier node when
// they appear on this querier's class tenant data updates channel.
// We don't currently expect any messages from the querier node, but just use the existence
// of the stream to keep the connection alive so we can send events to the querier node.
// This function blocks until the stream is closed by the querier node.
func (s *Server) QuerierStream(stream api.MetadataService_QuerierStreamServer) error {
	// Note, if you add any potentially blocking calls within QuerierStream, you should
	// use/derive their context from stream.Context()

	// set up a querier and register it
	q := NewQuerier(s.dataEventsChannelCapacity)
	if s.querierManager != nil {
		s.querierManager.Register(q)
		defer s.querierManager.Unregister(q)
	}

	// returnErr should be set if there is an error below that should be returned to the caller
	var returnErr error

	// We start two goroutines below, one to maintain the connection with the client, and one to
	// send outgoing messages to the client
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Start a goroutine to wait until the stream is closed by the querier node.
	// If we extend the protocol to send messages from the querier to the metadata node,
	// then they'll be handled in this goroutine
	enterrors.GoWrapper(func() {
		defer wg.Done()

		for {
			// Wait here until the stream is done (for now, we aren't expecting any messages from the
			// querier but we have the for loop to retry in case we get transient errors).
			_, err := stream.Recv()
			if err == io.EOF {
				// io.EOF is expected when the stream is closed by the querier
				return
			}
			if err != nil {
				// stream aborted
				s.log.Warnf("metadataserver/Server.QuerierStream unexpected error: %v", err)
				return
			}
		}
	}, s.log)

	// This channel will receive a message when a class/tenant's data has been updated
	classTenantDataUpdates := q.ClassTenantDataEvents()

	// Start a goroutine to send class tenant data updates to the querier node.
	enterrors.GoWrapper(func() {
		defer wg.Done()
		for {
			// Pass classTenantDataUpdates to the client as they become available, until this
			// stream's context is done (eg the client closes the stream).
			select {
			case <-stream.Context().Done():
				return
			case ct := <-classTenantDataUpdates:
				err := stream.Send(&api.QuerierStreamResponse{
					Type: api.QuerierStreamResponse_TYPE_CLASS_TENANT_DATA_UPDATE,
					ClassTenant: &api.ClassTenant{
						ClassName:  ct.ClassName,
						TenantName: ct.TenantName,
					},
				})
				if err != nil {
					// unexpected error
					returnErr = fmt.Errorf("querier register stream send: %w", err)
					return
				}
			}
		}
	}, s.log)

	// Block until the stream is closed and the goroutines above are done.
	wg.Wait()

	return returnErr
}

// Close closes the server and free any used ressources.
func (s *Server) Close() {
	if s.grpcServer != nil {
		// don't use GracefulStop here yet because we don't currently tell the client when to
		// disconnect and it's fine if we drop some tenant cache invalidation messages for now
		s.grpcServer.Stop()
	}
}
