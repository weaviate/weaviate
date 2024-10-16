package cli

import (
	"fmt"
	"net"

	pb "github.com/weaviate/weaviate/cli/proto/cli"
	"google.golang.org/grpc"
)

type CliServer struct {
	Addr          string
	schemaService *schemaService
}

func NewCliServer() *CliServer {
	return &CliServer{
		schemaService: &schemaService{},
	}
}

func (c *CliServer) Start() error {
	listener, err := net.Listen("tcp", c.Addr)
	if err != nil {
		return fmt.Errorf("could not start listener on %s: %w", c.Addr, err)
	}
	s := grpc.NewServer()
	pb.RegisterSchemaServiceServer(s, c.schemaService)
	if err := s.Serve(listener); err != nil {
		return fmt.Errorf("could not serve on %s: %w", c.Addr, err)
	}
	return nil
}
