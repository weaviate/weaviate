package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/searchparams"
	pb "github.com/weaviate/weaviate/grpc"
	"github.com/weaviate/weaviate/usecases/traverser"
	"google.golang.org/grpc"
)

func StartAndListen(port int, state *state.State) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterWeaviateServer(s, &Server{
		traverser: state.Traverser,
	})
	// TODO: use proper logger
	log.Printf("grpc server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}

	return nil
}

type Server struct {
	pb.UnimplementedWeaviateServer
	traverser *traverser.Traverser
}

func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	before := time.Now()
	// TODO: auth
	_, err := s.traverser.GetClass(ctx, nil, searchParamsFromProto(req))
	if err != nil {
		return nil, err
	}

	tookSeconds := float64(time.Since(before)) / float64(time.Second)
	return nil, fmt.Errorf("showing results not implemented yet, search took %fs", tookSeconds)
}

func searchParamsFromProto(req *pb.SearchRequest) dto.GetParams {
	out := dto.GetParams{}
	out.ClassName = req.ClassName
	if req.NearVector != nil {
		out.NearVector = &searchparams.NearVector{
			Vector: req.NearVector.Vector,
		}
	}

	out.Pagination = &filters.Pagination{}
	if req.Limit > 0 {
		out.Pagination.Limit = int(req.Limit)
	} else {
		// TODO: align default with other APIs
		out.Pagination.Limit = 10
	}

	return out
}
