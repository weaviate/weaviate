package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/go-openapi/strfmt"
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
	res, err := s.traverser.GetClass(ctx, nil, searchParamsFromProto(req))
	if err != nil {
		return nil, err
	}

	return searchResultsToProto(res, before), nil
}

func searchResultsToProto(res []any, start time.Time) *pb.SearchReply {
	tookSeconds := float64(time.Since(start)) / float64(time.Second)
	out := &pb.SearchReply{
		Took:    float32(tookSeconds),
		Results: make([]*pb.SearchResult, len(res)),
	}

	for i, raw := range res {
		asMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		idRaw, ok := asMap["id"]
		if !ok {
			continue
		}

		idStrfmt, ok := idRaw.(strfmt.UUID)
		if !ok {
			continue
		}

		out.Results[i] = &pb.SearchResult{
			Id: idStrfmt.String(),
		}
	}

	return out
}

func searchParamsFromProto(req *pb.SearchRequest) dto.GetParams {
	out := dto.GetParams{}
	out.ClassName = req.ClassName
	if req.NearVector != nil {
		out.NearVector = &searchparams.NearVector{
			Vector: req.NearVector.Vector,
		}
	}

	if req.NearObject != nil {
		out.NearObject = &searchparams.NearObject{
			ID: req.NearObject.Id,
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
