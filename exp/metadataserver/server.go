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
	grpcServer         *grpc.Server
	listenAddress      string
	grpcMessageMaxSize int
	sentryEnabled      bool
	querierManager     *QuerierManager
	log                *logrus.Logger
}

func NewServer(listenAddress string, grpcMessageMaxSize int,
	sentryEnabled bool, querierManager *QuerierManager, log *logrus.Logger) *Server {
	return &Server{
		listenAddress:      listenAddress,
		grpcMessageMaxSize: grpcMessageMaxSize,
		sentryEnabled:      sentryEnabled,
		querierManager:     querierManager,
		log:                log,
	}
}

// Open starts the server and registers it as the cluster service server.
// Returns asynchronously once the server has started.
// Returns an error if the configured listenAddress is invalid.
// Returns an error if the configured listenAddress is un-usable to listen on.
func (s *Server) Open() error {
	s.log.WithField("address", s.listenAddress).Info("starting cloud rpc server ...")
	if s.listenAddress == "" {
		return fmt.Errorf("address of rpc server cannot be empty")
	}

	// NOTE listen uses context.background by default, can set if needed
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
	enterrors.GoWrapper(func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			s.log.WithError(err).Error("serving incoming requests")
			panic("error accepting incoming requests")
		}
	}, s.log)
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
	// TODO context https://stackoverflow.com/questions/76724124/does-a-go-grpc-server-streaming-method-not-have-a-context-argument
	// https://github.com/pahanini/go-grpc-bidirectional-streaming-example/blob/master/src/server/server.go
	// read and understand this https://github.com/grpc/grpc-go/issues/4578
	// https://dev.to/techschoolguru/implement-bidirectional-streaming-grpc-go-4kgn
	//   LaptopService_RateLaptopServer

	// set up a querier and register it
	q := NewQuerier()
	// TODO verify nil ptrs handled well (dont exit on panic?)
	s.querierManager.Register(q)
	defer s.querierManager.Unregister(q)

	// returnErr should be set if there is an error in the goroutines below that should
	// be returned to the caller.
	var returnErr error

	wg := sync.WaitGroup{}
	wg.Add(2)

	// Start a goroutine to wait until the stream is closed by the querier node.
	// If we extend the protocol to send messages from the querier to the metadata node,
	// then they'll be handled in this goroutine.
	enterrors.GoWrapper(func() {
		defer wg.Done()

		// Wait here until the stream is done (for now, we aren't expecting any messages from the
		// querier but we have the for loop to retry in case we get transient errors)
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				// io.EOF is expected when the stream is closed by the querier
				return
			}
			if err != nil {
				// unexpected error TODO log
			}
		}
	}, s.log)

	// This channel will receive a message when a class/tenant's data has been updated
	classTenantDataUpdates := q.ClassTenantDataEvents()

	// Start a goroutine to send class tenant data updates to the querier node.
	enterrors.GoWrapper(func() {
		defer wg.Done()
		for {
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
				// TODO does Send actually ever return io.EOF?
				if err == io.EOF {
					// io.EOF is expected when the stream is closed by the querier
					return
				}
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
		s.grpcServer.Stop()
	}
}
