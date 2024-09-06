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

package main

import (
	"net"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/exp/query"
	"github.com/weaviate/weaviate/exp/querytenant"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	TargetQuerier = "querier"
)

// TODO: We want this to be part of original `cmd/weaviate-server`.
// But for some reason that binary is auto-generated and I couldn't modify as I need. Hence separate binary for now
func main() {
	var (
		opts Options
		log  logrus.FieldLogger
	)

	log = logrus.WithFields(logrus.Fields{"app": "weaviate"})

	_, err := flags.Parse(&opts)
	if err != nil {
		if err.(*flags.Error).Type == flags.ErrHelp {
			os.Exit(1)
		}
		log.WithField("cause", err).Fatal("failed to parse command line args")
	}

	switch opts.Target {
	case "querier":
		log = log.WithField("target", "querier")
		s3module := modsloads3.New()
		s3module.DataPath = opts.Query.DataPath
		s3module.Endpoint = opts.Query.S3Endpoint

		// This functionality is already in `go-client` of weaviate.
		// TODO(kavi): Find a way to share this functionality in both go-client and in querytenant.
		tenantInfo := querytenant.NewTenantInfo(opts.Query.SchemaAddr, querytenant.DefaultSchemaPath)

		a := query.NewAPI(
			tenantInfo,
			s3module,
			&opts.Query,
			log,
		)
		grpcQuerier := query.NewGRPC(a, log)
		listener, err := net.Listen("tcp", opts.Query.GRPCListenAddr)
		if err != nil {
			log.WithFields(logrus.Fields{
				"cause": err,
				"addrs": opts.Query.GRPCListenAddr,
			}).Fatal("failed to bind grpc server addr")
		}

		grpcServer := grpc.NewServer()
		reflection.Register(grpcServer)
		protocol.RegisterWeaviateServer(grpcServer, grpcQuerier)

		log.WithField("addr", opts.Query.GRPCListenAddr).Info("starting querier over grpc")
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal("failed to start grpc server", err)
		}

	default:
		log.Fatal("--target empty or unknown")
	}
}

// Options represents Command line options passed to weaviate binary
type Options struct {
	Target string       `long:"target" description:"how should weaviate-server be running as e.g: querier, ingester, etc"`
	Query  query.Config `group:"query" namespace:"query"`
}
