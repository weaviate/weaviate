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
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/exp/query"
	"github.com/weaviate/weaviate/exp/queryschema"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	"github.com/weaviate/weaviate/modules/text2vec-contextionary/client"
	"github.com/weaviate/weaviate/modules/text2vec-contextionary/vectorizer"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/traverser"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	TargetQuerier = "querier"
	GBtoByes      = 1 << 30
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
		log.WithField("err", err).Fatal("failed to parse command line args")
	}

	// Set version from swagger spec.
	build.Version = rest.ParseVersionFromSwaggerSpec()

	switch opts.Target {
	case "querier":
		log = log.WithField("target", "querier")
		s3module := modsloads3.New()
		s3module.DataPath = opts.Query.DataPath
		s3module.Endpoint = opts.Query.S3Endpoint

		// This functionality is already in `go-client` of weaviate.
		// TODO(kavi): Find a way to share this functionality in both go-client and in querytenant.
		schemaInfo := queryschema.NewSchemaInfo(opts.Query.SchemaAddr, queryschema.DefaultSchemaPrefix)

		vclient, err := client.NewClient(opts.Query.VectorizerAddr, log)
		if err != nil {
			log.WithFields(logrus.Fields{
				"err":   err,
				"addrs": opts.Query.VectorizerAddr,
			}).Fatal("failed to talk to vectorizer")
		}

		detectStopwords, err := stopwords.NewDetectorFromPreset(stopwords.EnglishPreset)
		if err != nil {
			log.WithFields(logrus.Fields{
				"err": err,
			}).Fatal("failed to create stopwords detector for querier")
		}

		var lsm *db.LSMFetcher
		if opts.Query.NoCache {
			lsm = db.NewLSMFetcher(opts.Query.DataPath, s3module, log)
		} else {
			metrics := db.NewCacheMetrics(opts.Monitoring.MetricsNamespace, prometheus.DefaultRegisterer)
			cache := db.NewDiskCache(opts.Query.DataPath, opts.Query.CacheMaxSizeGB*GBtoByes, metrics)
			lsm = db.NewLSMFetcherWithCache(opts.Query.DataPath, s3module, cache, log)
		}

		searcher := db.NewSearcher(&db.SearchConfig{QueryMaximumResults: 1000, QueryLimit: 1000}, log, opts.Query.DataPath, lsm, schemaInfo)

		e := traverser.NewExplorer(searcher, log, modules.NewProvider(log), nil, &traverser.ExplorerConfig{
			QueryMaxResults:   1000,
			QueryDefaultLimit: 100,
		})

		a := query.NewAPI(
			schemaInfo,
			vectorizer.New(vclient),
			detectStopwords,
			&opts.Query,
			e,
			log,
		)

		grpcQuerier := query.NewGRPC(a, schemaInfo, log)
		listener, err := net.Listen("tcp", opts.Query.GRPCListenAddr)
		if err != nil {
			log.WithFields(logrus.Fields{
				"err":   err,
				"addrs": opts.Query.GRPCListenAddr,
			}).Fatal("failed to bind grpc server addr")
		}
		svrMetrics := monitoring.NewServerMetrics(opts.Monitoring, prometheus.DefaultRegisterer)
		listener = monitoring.CountingListener(listener, svrMetrics.TCPActiveConnections.WithLabelValues("grpc"))
		grpcServer := grpc.NewServer(GrpcOptions(*svrMetrics)...)
		reflection.Register(grpcServer)
		protocol.RegisterWeaviateServer(grpcServer, grpcQuerier)

		log.WithField("addr", opts.Query.GRPCListenAddr).Info("starting querier over grpc")
		enterrors.GoWrapper(func() {
			if err := grpcServer.Serve(listener); err != nil {
				log.Fatal("failed to start grpc server", err)
			}
		}, log)

		// serve /metrics
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.WithField("addr", opts.Monitoring.Port).Info("starting /metrics server over http")
		http.ListenAndServe(fmt.Sprintf(":%d", opts.Monitoring.Port), mux)

	default:
		log.Fatal("--target empty or unknown")
	}
}

// Options represents Command line options passed to weaviate binary
type Options struct {
	Target     string            `long:"target" description:"how should weaviate-server be running as e.g: querier, ingester, etc"`
	Query      query.Config      `group:"query" namespace:"query"`
	Monitoring monitoring.Config `group:"monitoring" namespace:"monitoring"`
}

func GrpcOptions(svrMetrics monitoring.ServerMetrics) []grpc.ServerOption {
	grpcOptions := []grpc.ServerOption{
		grpc.StatsHandler(monitoring.NewGrpcStatsHandler(
			svrMetrics.InflightRequests,
			svrMetrics.RequestBodySize,
			svrMetrics.ResponseBodySize,
		)),
	}

	grpcInterceptUnary := grpc.ChainUnaryInterceptor(
		monitoring.UnaryServerInstrument(svrMetrics.RequestDuration),
	)
	grpcOptions = append(grpcOptions, grpcInterceptUnary)

	grpcInterceptStream := grpc.ChainStreamInterceptor(
		monitoring.StreamServerInstrument(svrMetrics.RequestDuration),
	)
	grpcOptions = append(grpcOptions, grpcInterceptStream)

	return grpcOptions
}
