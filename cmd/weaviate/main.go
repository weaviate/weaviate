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
	"context"
	"net"

	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/exp/query"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/auth/authentication/oidc"
	"github.com/weaviate/weaviate/usecases/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	TargetQuerier = "querier"

	// For mocking purpose
	authAnonymousEnabled = true
)

// TODO: We want this to be part of original `cmd/weaviate-server`.
// But for some reason that binary is auto-generated and I couldn't modify as I need. Hence separate binary for now
func main() {
	var opts Options
	log := logrus.WithFields(logrus.Fields{"app": "weaviate"}).Logger

	_, err := flags.Parse(&opts)
	if err != nil {
		log.Fatal("failed to parse command line args", err)
	}

	switch opts.Target {
	case "querier":
		myConfig := config.GetConfigOptionGroup()
		appState := rest.MakeAppState(context.TODO(), myConfig)

		cfg := config.Config{
			QueryDefaults: config.QueryDefaults{Limit: 25},
		}
		weaviateOIDC, err := oidc.New(cfg)
		if err != nil {
			panic(err)
		}

		weaviateApiKey, err := apikey.New(cfg)
		if err != nil {
			panic(err)
		}
		a := query.NewAPI(
			appState,
			composer.New(cfg.Authentication, weaviateOIDC, weaviateApiKey),
			&cfg,
			log,
		)
		grpcQuerier := query.NewGRPC(a, log)
		listener, err := net.Listen("tcp", opts.GRPCListenAddr)
		if err != nil {
			log.Fatal("failed to bind grpc server port", err)
		}

		grpcServer := grpc.NewServer()
		reflection.Register(grpcServer)
		protocol.RegisterWeaviateServer(grpcServer, grpcQuerier)

		log.WithField("port", opts.GRPCListenAddr).Info("starting querier grpc")
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal("failed to start grpc server", err)
		}

	default:
		log.Fatal("--target empty or unknown")
	}
}

// Options represents Command line options
type Options struct {
	Target            string `long:"target" description:"how should weaviate-server be running as e.g: querier, ingester, etc"`
	GRPCListenAddr    string `long:"query.grpc.listen" description:"gRPC address that query node listens at" default:"0.0.0.0:9090"`
	SchemaManagerAddr string `long:"schema.grpc.addr" description:"gRPC address to get schema information" default:"0.0.0.0:50051"`
}

type DummyLock struct{}

func (d *DummyLock) LockConnector() (func() error, error) {
	return func() error { return nil }, nil
}

func (d *DummyLock) LockSchema() (func() error, error) {
	return func() error { return nil }, nil
}
