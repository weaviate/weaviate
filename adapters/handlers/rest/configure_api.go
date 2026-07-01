//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"regexp"
	goruntime "runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	armonmetrics "github.com/armon/go-metrics"
	armonprometheus "github.com/armon/go-metrics/prometheus"
	"github.com/getsentry/sentry-go"
	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/swag"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/pbnjay/memory"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/weaviate/fgprof"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/authz"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	clusterapigrpc "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/handlers/rest/db_users"
	rest_namespaces "github.com/weaviate/weaviate/adapters/handlers/rest/namespaces"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	replicationHandlers "github.com/weaviate/weaviate/adapters/handlers/rest/replication"
	"github.com/weaviate/weaviate/adapters/handlers/rest/restcompat"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/handlers/rest/tenantactivity"
	"github.com/weaviate/weaviate/adapters/repos/classifications"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	modulestorage "github.com/weaviate/weaviate/adapters/repos/modules"
	schemarepo "github.com/weaviate/weaviate/adapters/repos/schema"
	rCluster "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/cluster/replication/selfrecovery"
	"github.com/weaviate/weaviate/cluster/usage"
	entconfig "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/replication"
	entschema "github.com/weaviate/weaviate/entities/schema"
	vectorIndex "github.com/weaviate/weaviate/entities/vectorindex"
	grpcconn "github.com/weaviate/weaviate/grpc/conn"
	modstgazure "github.com/weaviate/weaviate/modules/backup-azure"
	modstgfs "github.com/weaviate/weaviate/modules/backup-filesystem"
	modstggcs "github.com/weaviate/weaviate/modules/backup-gcs"
	modstgs3 "github.com/weaviate/weaviate/modules/backup-s3"
	modgenerativeanthropic "github.com/weaviate/weaviate/modules/generative-anthropic"
	modgenerativeanyscale "github.com/weaviate/weaviate/modules/generative-anyscale"
	modgenerativeaws "github.com/weaviate/weaviate/modules/generative-aws"
	modgenerativecohere "github.com/weaviate/weaviate/modules/generative-cohere"
	modgenerativecontextualai "github.com/weaviate/weaviate/modules/generative-contextualai"
	modgenerativedatabricks "github.com/weaviate/weaviate/modules/generative-databricks"
	modgenerativedeepseek "github.com/weaviate/weaviate/modules/generative-deepseek"
	modgenerativedummy "github.com/weaviate/weaviate/modules/generative-dummy"
	modgenerativefriendliai "github.com/weaviate/weaviate/modules/generative-friendliai"
	modgenerativegoogle "github.com/weaviate/weaviate/modules/generative-google"
	modgenerativemistral "github.com/weaviate/weaviate/modules/generative-mistral"
	modgenerativenvidia "github.com/weaviate/weaviate/modules/generative-nvidia"
	modgenerativeoctoai "github.com/weaviate/weaviate/modules/generative-octoai"
	modgenerativeollama "github.com/weaviate/weaviate/modules/generative-ollama"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modgenerativexai "github.com/weaviate/weaviate/modules/generative-xai"
	modimage "github.com/weaviate/weaviate/modules/img2vec-neural"
	modmulti2multivecjinaai "github.com/weaviate/weaviate/modules/multi2multivec-jinaai"
	modmulti2multivecweaviate "github.com/weaviate/weaviate/modules/multi2multivec-weaviate"
	modmulti2vecaws "github.com/weaviate/weaviate/modules/multi2vec-aws"
	modbind "github.com/weaviate/weaviate/modules/multi2vec-bind"
	modclip "github.com/weaviate/weaviate/modules/multi2vec-clip"
	modmulti2veccohere "github.com/weaviate/weaviate/modules/multi2vec-cohere"
	modmulti2vecgoogle "github.com/weaviate/weaviate/modules/multi2vec-google"
	modmulti2vecjinaai "github.com/weaviate/weaviate/modules/multi2vec-jinaai"
	modmulti2vecnvidia "github.com/weaviate/weaviate/modules/multi2vec-nvidia"
	modmulti2vecvoyageai "github.com/weaviate/weaviate/modules/multi2vec-voyageai"
	modner "github.com/weaviate/weaviate/modules/ner-transformers"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	modqnaopenai "github.com/weaviate/weaviate/modules/qna-openai"
	modqna "github.com/weaviate/weaviate/modules/qna-transformers"
	modcentroid "github.com/weaviate/weaviate/modules/ref2vec-centroid"
	modrerankercohere "github.com/weaviate/weaviate/modules/reranker-cohere"
	modrerankercontextualai "github.com/weaviate/weaviate/modules/reranker-contextualai"
	modrerankerdummy "github.com/weaviate/weaviate/modules/reranker-dummy"
	modrerankerjinaai "github.com/weaviate/weaviate/modules/reranker-jinaai"
	modrerankernvidia "github.com/weaviate/weaviate/modules/reranker-nvidia"
	modrerankertransformers "github.com/weaviate/weaviate/modules/reranker-transformers"
	modrerankervoyageai "github.com/weaviate/weaviate/modules/reranker-voyageai"
	modsum "github.com/weaviate/weaviate/modules/sum-transformers"
	modspellcheck "github.com/weaviate/weaviate/modules/text-spellcheck"
	modtext2multivecjinaai "github.com/weaviate/weaviate/modules/text2multivec-jinaai"
	modtext2vecaws "github.com/weaviate/weaviate/modules/text2vec-aws"
	modt2vbigram "github.com/weaviate/weaviate/modules/text2vec-bigram"
	modcohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	modcontextionary "github.com/weaviate/weaviate/modules/text2vec-contextionary"
	moddatabricks "github.com/weaviate/weaviate/modules/text2vec-databricks"
	moddigitalocean "github.com/weaviate/weaviate/modules/text2vec-digitalocean"
	modtext2vecgoogle "github.com/weaviate/weaviate/modules/text2vec-google"
	modgpt4all "github.com/weaviate/weaviate/modules/text2vec-gpt4all"
	modhuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
	modjinaai "github.com/weaviate/weaviate/modules/text2vec-jinaai"
	modmistral "github.com/weaviate/weaviate/modules/text2vec-mistral"
	modt2vmodel2vec "github.com/weaviate/weaviate/modules/text2vec-model2vec"
	modmorph "github.com/weaviate/weaviate/modules/text2vec-morph"
	modnvidia "github.com/weaviate/weaviate/modules/text2vec-nvidia"
	modtext2vecoctoai "github.com/weaviate/weaviate/modules/text2vec-octoai"
	modollama "github.com/weaviate/weaviate/modules/text2vec-ollama"
	modopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modtransformers "github.com/weaviate/weaviate/modules/text2vec-transformers"
	modvoyageai "github.com/weaviate/weaviate/modules/text2vec-voyageai"
	modweaviateembed "github.com/weaviate/weaviate/modules/text2vec-weaviate"
	modusagegcs "github.com/weaviate/weaviate/modules/usage-gcs"
	modusages3 "github.com/weaviate/weaviate/modules/usage-s3"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/classification"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	exportusecase "github.com/weaviate/weaviate/usecases/export"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	namespacecleanup "github.com/weaviate/weaviate/usecases/namespace_cleanup"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/telemetry"
	"github.com/weaviate/weaviate/usecases/telemetry/opentelemetry"
	"github.com/weaviate/weaviate/usecases/traverser"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

const MinimumRequiredContextionaryVersion = "1.0.2"

func makeConfigureServer(appState *state.State) func(*http.Server, string, string) {
	return func(s *http.Server, scheme, addr string) {
		// Add properties to the config
		appState.ServerConfig.Hostname = addr
		appState.ServerConfig.Scheme = scheme
	}
}

type vectorRepo interface {
	objects.BatchVectorRepo
	traverser.VectorSearcher
	classification.VectorRepo
	SetSchemaGetter(schema.SchemaGetter)
	WaitForStartup(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

func getCores() (int, error) {
	cpuset, err := os.ReadFile("/sys/fs/cgroup/cpuset/cpuset.cpus")
	if err != nil {
		return 0, errors.Wrap(err, "read cpuset")
	}
	return calcCPUs(strings.TrimSpace(string(cpuset)))
}

func calcCPUs(cpuString string) (int, error) {
	cores := 0
	if cpuString == "" {
		return 0, nil
	}

	// Split by comma to handle multiple ranges
	ranges := strings.Split(cpuString, ",")
	for _, r := range ranges {
		// Check if it's a range (contains a hyphen)
		if strings.Contains(r, "-") {
			parts := strings.Split(r, "-")
			if len(parts) != 2 {
				return 0, fmt.Errorf("invalid CPU range format: %s", r)
			}
			start, err := strconv.Atoi(parts[0])
			if err != nil {
				return 0, fmt.Errorf("invalid start of CPU range: %s", parts[0])
			}
			end, err := strconv.Atoi(parts[1])
			if err != nil {
				return 0, fmt.Errorf("invalid end of CPU range: %s", parts[1])
			}
			cores += end - start + 1
		} else {
			// Single CPU
			cores++
		}
	}

	return cores, nil
}

func MakeAppState(ctx, serverShutdownCtx context.Context, options *swag.CommandLineOptionsGroup) *state.State {
	build.Version = ParseVersionFromSwaggerSpec() // Version is always static and loaded from swagger spec.

	// config.ServerVersion is deprecated: It's there to be backward compatible
	// use build.Version instead.
	config.ServerVersion = build.Version

	appState := startupRoutine(ctx, serverShutdownCtx, options)

	// Initialize OpenTelemetry tracing
	if err := opentelemetry.Init(appState.Logger); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Error("failed to initialize OpenTelemetry")
	}

	if appState.ServerConfig.Config.Monitoring.Enabled {
		appState.HTTPServerMetrics = monitoring.NewHTTPServerMetrics(monitoring.DefaultMetricsNamespace, prometheus.DefaultRegisterer)
		appState.GRPCServerMetrics = monitoring.NewGRPCServerMetrics(monitoring.DefaultMetricsNamespace, prometheus.DefaultRegisterer)

		appState.TenantActivity = tenantactivity.NewHandler()

		// Since we are scraping prometheus.DefaultRegisterer, it already has
		// a go collector configured by default in internal module init().
		// However, the go collector configured by default is missing some interesting metrics,
		// therefore, we have to first unregister it so there are no duplicate metric declarations
		// and then register extended collector once again.
		prometheus.Unregister(collectors.NewGoCollector())
		prometheus.MustRegister(collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(collectors.GoRuntimeMetricsRule{
				Matcher: regexp.MustCompile(`/sched/latencies:seconds`),
			}),
		))

		// export build tags to prometheus metric
		build.SetPrometheusBuildInfo()
		prometheus.MustRegister(version.NewCollector(build.AppName))

		opts := armonprometheus.PrometheusOpts{
			Expiration: 0, // never expire any metrics,
			Registerer: prometheus.DefaultRegisterer,
		}

		sink, err := armonprometheus.NewPrometheusSinkFrom(opts)
		if err != nil {
			appState.Logger.WithField("action", "startup").WithError(err).Fatal("failed to create prometheus sink for raft metrics")
		}

		cfg := armonmetrics.DefaultConfig("weaviate_internal") // to differentiate it's coming from internal/dependency packages.
		cfg.EnableHostname = false                             // no `host` label
		cfg.EnableHostnameLabel = false                        // no `hostname` label
		cfg.EnableServiceLabel = false                         // no `service` label
		cfg.EnableRuntimeMetrics = false                       // runtime metrics already provided by prometheus
		cfg.EnableTypePrefix = true                            // to have some meaningful suffix to identify type of metrics.
		cfg.TimerGranularity = time.Second                     // time should always in seconds

		_, err = armonmetrics.NewGlobal(cfg, sink)
		if err != nil {
			appState.Logger.WithField("action", "startup").WithError(err).Fatal("failed to create metric registry raft metrics")
		}

		// only monitoring tool supported at the moment is prometheus
		enterrors.GoWrapper(func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			mux.Handle("/tenant-activity", appState.TenantActivity)
			http.ListenAndServe(fmt.Sprintf(":%d", appState.ServerConfig.Config.Monitoring.Port), mux)
		}, appState.Logger)
	}

	if appState.ServerConfig.Config.Sentry.Enabled {
		err := sentry.Init(sentry.ClientOptions{
			// Setup related config
			Dsn:         appState.ServerConfig.Config.Sentry.DSN,
			Debug:       appState.ServerConfig.Config.Sentry.Debug,
			Release:     "weaviate-core@" + build.Version,
			Environment: appState.ServerConfig.Config.Sentry.Environment,
			// Enable tracing if requested
			EnableTracing:    !appState.ServerConfig.Config.Sentry.TracingDisabled,
			AttachStacktrace: true,
			// Sample rates based on the config
			SampleRate:         appState.ServerConfig.Config.Sentry.ErrorSampleRate,
			ProfilesSampleRate: appState.ServerConfig.Config.Sentry.ProfileSampleRate,
			TracesSampler: sentry.TracesSampler(func(ctx sentry.SamplingContext) float64 {
				// Inherit decision from parent transaction (if any) if it is sampled or not
				if ctx.Parent != nil && ctx.Parent.Sampled != sentry.SampledUndefined {
					return 1.0
				}

				// Filter out uneeded traces
				switch ctx.Span.Name {
				// We are not interested in traces related to metrics endpoint
				case "GET /metrics":
				// These are some usual internet bot that will spam the server. Won't catch them all but we can reduce
				// the number a bit
				case "GET /favicon.ico":
				case "GET /t4":
				case "GET /ab2g":
				case "PRI *":
				case "GET /api/sonicos/tfa":
				case "GET /RDWeb/Pages/en-US/login.aspx":
				case "GET /_profiler/phpinfo":
				case "POST /wsman":
				case "POST /dns-query":
				case "GET /dns-query":
					return 0.0
				}

				// Filter out graphql queries, currently we have no context intrumentation around it and it's therefore
				// just a blank line with 0 info except graphql resolve -> do -> return.
				if ctx.Span.Name == "POST /v1/graphql" {
					return 0.0
				}

				// Return the configured sample rate otherwise
				return appState.ServerConfig.Config.Sentry.TracesSampleRate
			}),
		})
		if err != nil {
			appState.Logger.
				WithField("action", "startup").WithError(err).
				Fatal("sentry initialization failed")
		}

		sentry.ConfigureScope(func(scope *sentry.Scope) {
			// Set cluster ID and cluster owner using sentry user feature to distinguish multiple clusters in the UI
			scope.SetUser(sentry.User{
				ID:       appState.ServerConfig.Config.Sentry.ClusterId,
				Username: appState.ServerConfig.Config.Sentry.ClusterOwner,
			})
			// Set any tags defined
			for key, value := range appState.ServerConfig.Config.Sentry.Tags {
				scope.SetTag(key, value)
			}
		})
	}

	limitResources(appState)

	appState.ClusterHttpClient = reasonableHttpClient(appState.ServerConfig.Config.Cluster.AuthConfig, appState.ServerConfig.Config.MinimumInternalTimeout)
	appState.MemWatch = memwatch.NewMonitor(memwatch.LiveHeapReader, debug.SetMemoryLimit, 0.97)
	appState.ObjectTTLLocalStatus = objectttl.NewLocalStatus()
	// ReindexSubmitLocks is shared between the reindex-submit REST
	// handler and the DELETE property-index REST handler so they
	// serialize on the same per-(collection, property) mutex. See
	// the field godoc on state.State for the race this closes.
	appState.ReindexSubmitLocks = state.NewReindexSubmitLocks()

	var vectorRepo vectorRepo
	// var vectorMigrator schema.Migrator
	// var migrator schema.Migrator

	metricsRegisterer := monitoring.NoopRegisterer
	if appState.ServerConfig.Config.Monitoring.Enabled {
		promMetrics := monitoring.GetMetrics()
		metricsRegisterer = promMetrics.Registerer
		appState.Metrics = promMetrics
	}

	// TODO: configure http transport for efficient intra-cluster comm
	remoteIndexClient := clients.NewRemoteIndex(appState.ClusterHttpClient)
	remoteNodesClient := clients.NewRemoteNode(appState.ClusterHttpClient)
	restReplicationClient, err := clients.NewReplicationClient(appState.ClusterHttpClient)
	if err != nil {
		appState.Logger.WithField("action", "startup").Fatalf("failed to create replication client: %v", err)
	}

	// Set up gRPC connection manager (needed for both file replication and replication gRPC client)
	grpcConfig := appState.ServerConfig.Config.GRPC
	authConfig := appState.ServerConfig.Config.Cluster.AuthConfig

	var creds credentials.TransportCredentials
	useTLS := len(grpcConfig.CertFile) > 0
	if useTLS {
		creds = credentials.NewClientTLSFromCert(nil, "")
	} else {
		creds = insecure.NewCredentials()
	}

	grpcDialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if authConfig.BasicAuth.Enabled() {
		authHeader := grpcconn.BasicAuthHeader(authConfig.BasicAuth.Username, authConfig.BasicAuth.Password)
		grpcDialOpts = append(grpcDialOpts,
			grpc.WithChainUnaryInterceptor(grpcconn.BasicAuthUnaryInterceptor(authHeader)),
			grpc.WithChainStreamInterceptor(grpcconn.BasicAuthStreamInterceptor(authHeader)),
		)
	}

	maxSize := clusterapigrpc.GetMaxMessageSize(appState)
	initialConnWindowSize := clusterapigrpc.GetInitialConnWindowSize(appState)
	grpcDialOpts = append(grpcDialOpts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize),
		),
		grpc.WithInitialWindowSize(int32(initialConnWindowSize)),
		grpc.WithInitialConnWindowSize(int32(initialConnWindowSize)),
		grpc.WithReadBufferSize(clusterapigrpc.READ_BUFFER_SIZE),
		grpc.WithWriteBufferSize(clusterapigrpc.WRITE_BUFFER_SIZE),
	)

	grpcConnManager, err := grpcconn.NewConnManager(
		appState.ServerConfig.Config.GRPC.MaxOpenConns,
		appState.ServerConfig.Config.GRPC.IdleConnTimeout,
		metricsRegisterer, appState.Logger, grpcDialOpts...)
	if err != nil {
		appState.Logger.WithField("action", "startup").
			WithError(err).
			Fatal("failed to create gRPC connection manager")
	}
	appState.GRPCConnManager = grpcConnManager

	// Create replication-specific ConnManager with retry interceptor.
	// The base grpcConnManager (no retry) is used for file-replication;
	// the replConnManager adds retry on top for replication RPCs.
	replRetryInterceptor := grpc_retry.UnaryClientInterceptor(
		grpc_retry.WithMax(9),
		grpc_retry.WithBackoff(grpc_retry.BackoffExponentialWithJitter(100*time.Millisecond, 0.1)),
		grpc_retry.WithCodes(codes.Internal, codes.Unavailable, codes.ResourceExhausted),
	)
	replDialOpts := make([]grpc.DialOption, len(grpcDialOpts)+1)
	copy(replDialOpts, grpcDialOpts)
	replDialOpts[len(grpcDialOpts)] = grpc.WithChainUnaryInterceptor(replRetryInterceptor)

	replMetricsReg := prometheus.WrapRegistererWithPrefix("repl_", metricsRegisterer)
	replConnManager, err := grpcconn.NewConnManager(
		appState.ServerConfig.Config.GRPC.MaxOpenConns,
		appState.ServerConfig.Config.GRPC.IdleConnTimeout,
		replMetricsReg, appState.Logger, replDialOpts...)
	if err != nil {
		appState.Logger.WithField("action", "startup").
			WithError(err).
			Fatal("failed to create replication gRPC connection manager")
	}

	appState.ReplGRPCConnManager = replConnManager

	// Create switch replication client (gRPC or REST based on replication_grpc_enabled runtime config)
	grpcReplicationClient := clients.NewGRPCReplicationClient(replConnManager)
	replicationClient := clients.NewSwitchReplicationClient(
		grpcReplicationClient,
		restReplicationClient,
		appState.ServerConfig.Config.Replication.ReplicationGRPCEnabled.Get,
	)
	repo, err := db.New(appState.Logger, appState.Cluster.LocalName(), db.Config{
		ServerVersion:                       config.ServerVersion,
		GitHash:                             build.Revision,
		MemtablesFlushDirtyAfter:            appState.ServerConfig.Config.Persistence.MemtablesFlushDirtyAfter,
		MemtablesInitialSizeMB:              10,
		MemtablesMaxSizeMB:                  appState.ServerConfig.Config.Persistence.MemtablesMaxSizeMB,
		MemtablesMinActiveSeconds:           appState.ServerConfig.Config.Persistence.MemtablesMinActiveDurationSeconds,
		MemtablesMaxActiveSeconds:           appState.ServerConfig.Config.Persistence.MemtablesMaxActiveDurationSeconds,
		MinMMapSize:                         appState.ServerConfig.Config.Persistence.MinMMapSize,
		LazySegmentsDisabled:                appState.ServerConfig.Config.Persistence.LazySegmentsDisabled,
		SegmentInfoIntoFileNameEnabled:      appState.ServerConfig.Config.Persistence.SegmentInfoIntoFileNameEnabled,
		WriteMetadataFilesEnabled:           appState.ServerConfig.Config.Persistence.WriteMetadataFilesEnabled,
		MaxReuseWalSize:                     appState.ServerConfig.Config.Persistence.MaxReuseWalSize,
		SegmentsCleanupIntervalSeconds:      appState.ServerConfig.Config.Persistence.LSMSegmentsCleanupIntervalSeconds,
		SeparateObjectsCompactions:          appState.ServerConfig.Config.Persistence.LSMSeparateObjectsCompactions,
		MaxSegmentSize:                      appState.ServerConfig.Config.Persistence.LSMMaxSegmentSize,
		CycleManagerRoutinesFactor:          appState.ServerConfig.Config.Persistence.LSMCycleManagerRoutinesFactor,
		IndexRangeableInMemory:              appState.ServerConfig.Config.Persistence.IndexRangeableInMemory,
		RootPath:                            appState.ServerConfig.Config.Persistence.DataPath,
		QueryLimit:                          appState.ServerConfig.Config.QueryDefaults.Limit,
		QueryMaximumResults:                 appState.ServerConfig.Config.QueryMaximumResults,
		QueryHybridMaximumResults:           appState.ServerConfig.Config.QueryHybridMaximumResults,
		QueryNestedRefLimit:                 appState.ServerConfig.Config.QueryNestedCrossReferenceLimit,
		MaxImportGoroutinesFactor:           appState.ServerConfig.Config.MaxImportGoroutinesFactor,
		TrackVectorDimensions:               appState.ServerConfig.Config.TrackVectorDimensions || appState.Modules.UsageEnabled(),
		TrackVectorDimensionsInterval:       appState.ServerConfig.Config.TrackVectorDimensionsInterval,
		UsageEnabled:                        appState.Modules.UsageEnabled(),
		ResourceUsage:                       appState.ServerConfig.Config.ResourceUsage,
		AvoidMMap:                           appState.ServerConfig.Config.AvoidMmap,
		EnableLazyLoadShards:                appState.ServerConfig.Config.EnableLazyLoadShards,
		LazyLoadShardCountThreshold:         appState.ServerConfig.Config.LazyLoadShardCountThreshold,
		LazyLoadShardSizeThresholdGB:        appState.ServerConfig.Config.LazyLoadShardSizeThresholdGB,
		ForceFullReplicasSearch:             appState.ServerConfig.Config.ForceFullReplicasSearch,
		TransferInactivityTimeout:           appState.ServerConfig.Config.TransferInactivityTimeout,
		ObjectsTTLBatchSize:                 appState.ServerConfig.Config.ObjectsTTLBatchSize,
		ObjectsTTLPauseEveryNoBatches:       appState.ServerConfig.Config.ObjectsTTLPauseEveryNoBatches,
		ObjectsTTLPauseDuration:             appState.ServerConfig.Config.ObjectsTTLPauseDuration,
		ObjectsTTLConcurrencyFactor:         appState.ServerConfig.Config.ObjectsTTLConcurrencyFactor,
		LSMEnableSegmentsChecksumValidation: appState.ServerConfig.Config.Persistence.LSMEnableSegmentsChecksumValidation,
		LSMSkipWriteClassNameEnabled:        appState.ServerConfig.Config.Persistence.LSMSkipWriteClassNameEnabled,
		NamespacesEnabled:                   appState.ServerConfig.Config.Namespaces.Enabled,
		// Pass dummy replication config with minimum factor 1. Otherwise the
		// setting is not backward-compatible. The user may have created a class
		// with factor=1 before the change was introduced. Now their setup would no
		// longer start up if the required minimum is now higher than 1. We want
		// the required minimum to only apply to newly created classes - not block
		// loading existing ones.
		Replication: replication.GlobalConfig{
			MinimumFactor:                             1,
			AsyncReplicationDisabled:                  appState.ServerConfig.Config.Replication.AsyncReplicationDisabled,
			AsyncReplicationSchedulerWorkers:          appState.ServerConfig.Config.Replication.AsyncReplicationSchedulerWorkers,
			AsyncReplicationHashtreeInitConcurrency:   appState.ServerConfig.Config.Replication.AsyncReplicationHashtreeInitConcurrency,
			AsyncReplicationHashtreeHeight:            appState.ServerConfig.Config.Replication.AsyncReplicationHashtreeHeight,
			AsyncReplicationFrequency:                 appState.ServerConfig.Config.Replication.AsyncReplicationFrequency,
			AsyncReplicationFrequencyWhilePropagating: appState.ServerConfig.Config.Replication.AsyncReplicationFrequencyWhilePropagating,
			AsyncReplicationLoggingFrequency:          appState.ServerConfig.Config.Replication.AsyncReplicationLoggingFrequency,
			AsyncReplicationDiffBatchSize:             appState.ServerConfig.Config.Replication.AsyncReplicationDiffBatchSize,
			AsyncReplicationDiffPerNodeTimeout:        appState.ServerConfig.Config.Replication.AsyncReplicationDiffPerNodeTimeout,
			AsyncReplicationPrePropagationTimeout:     appState.ServerConfig.Config.Replication.AsyncReplicationPrePropagationTimeout,
			AsyncReplicationPropagationTimeout:        appState.ServerConfig.Config.Replication.AsyncReplicationPropagationTimeout,
			AsyncReplicationPropagationLimit:          appState.ServerConfig.Config.Replication.AsyncReplicationPropagationLimit,
			AsyncReplicationPropagationConcurrency:    appState.ServerConfig.Config.Replication.AsyncReplicationPropagationConcurrency,
			AsyncReplicationPropagationBatchSize:      appState.ServerConfig.Config.Replication.AsyncReplicationPropagationBatchSize,
			AsyncReplicationPropagationDelay:          appState.ServerConfig.Config.Replication.AsyncReplicationPropagationDelay,
		},
		MaximumConcurrentShardLoads:                  appState.ServerConfig.Config.MaximumConcurrentShardLoads,
		MaximumConcurrentBucketLoads:                 appState.ServerConfig.Config.MaximumConcurrentBucketLoads,
		HNSWMaxLogSize:                               appState.ServerConfig.Config.Persistence.HNSWMaxLogSize,
		HNSWDisableSnapshots:                         appState.ServerConfig.Config.Persistence.HNSWDisableSnapshots,
		HNSWSnapshotIntervalSeconds:                  appState.ServerConfig.Config.Persistence.HNSWSnapshotIntervalSeconds,
		HNSWSnapshotOnStartup:                        appState.ServerConfig.Config.Persistence.HNSWSnapshotOnStartup,
		HNSWSnapshotMinDeltaCommitlogsNumber:         appState.ServerConfig.Config.Persistence.HNSWSnapshotMinDeltaCommitlogsNumber,
		HNSWSnapshotMinDeltaCommitlogsSizePercentage: appState.ServerConfig.Config.Persistence.HNSWSnapshotMinDeltaCommitlogsSizePercentage,
		HNSWWaitForCachePrefill:                      appState.ServerConfig.Config.HNSWStartupWaitForVectorCache,
		HNSWFlatSearchConcurrency:                    appState.ServerConfig.Config.HNSWFlatSearchConcurrency,
		HNSWAcornFilterRatio:                         appState.ServerConfig.Config.HNSWAcornFilterRatio,
		HNSWGeoIndexEF:                               appState.ServerConfig.Config.HNSWGeoIndexEF,
		VisitedListPoolMaxSize:                       appState.ServerConfig.Config.HNSWVisitedListPoolMaxSize,
		TenantActivityReadLogLevel:                   appState.ServerConfig.Config.TenantActivityReadLogLevel,
		TenantActivityWriteLogLevel:                  appState.ServerConfig.Config.TenantActivityWriteLogLevel,
		QuerySlowLogEnabled:                          appState.ServerConfig.Config.QuerySlowLogEnabled,
		QuerySlowLogThreshold:                        appState.ServerConfig.Config.QuerySlowLogThreshold,
		InvertedSorterDisabled:                       appState.ServerConfig.Config.InvertedSorterDisabled,
		LazyPropertyLengthsEnabled:                   appState.ServerConfig.Config.LazyPropertyLengthsEnabled,
		MaintenanceModeEnabled:                       appState.Cluster.MaintenanceModeEnabledForLocalhost,
		AsyncIndexingEnabled:                         appState.ServerConfig.Config.AsyncIndexingEnabled,
		HFreshEnabled:                                appState.ServerConfig.Config.HFreshEnabled,
		OperationalMode:                              appState.ServerConfig.Config.OperationalMode,
		DisableDimensionMetrics:                      appState.ServerConfig.Config.DisableDimensionMetrics,
	}, remoteIndexClient, appState.Cluster, remoteNodesClient, replicationClient, appState.Metrics, appState.MemWatch, nil, nil, nil) // TODO client
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("invalid new DB")
	}

	appState.DB = repo
	// Seed the REST asyncEnabled shim; the runtime-config hook below keeps it live.
	restcompat.SetAsyncReplicationGloballyDisabled(appState.ServerConfig.Config.Replication.AsyncReplicationDisabled.Get())
	// Construct the usage-limits Manager now that its ObjectCounter (the
	// DB) exists, then install it on the DB so each Index inherits it
	// when loaded (init.go) or created at runtime (migrator.go). Both
	// must happen *before* WaitForStartup loads the existing indices.
	// See docs/usage_limits.md.
	appState.UsageLimits = usagelimits.NewManager(usagelimits.Config{
		ErrorMessage:    appState.ServerConfig.Config.UsageLimits.ErrorMessage,
		MaxObjectsCount: appState.ServerConfig.Config.UsageLimits.MaxObjectsCount,
	}, repo)
	repo.SetUsageLimits(appState.UsageLimits)
	if appState.ServerConfig.Config.Monitoring.Enabled {
		appState.TenantActivity.SetSource(appState.DB)
	}

	setupDebugHandlers(appState)
	setupGoProfiling(appState)
	setupRuntimeProfiling(appState)

	migrator := db.NewMigrator(repo, appState.Logger, appState.Cluster.LocalName())
	migrator.SetNode(appState.Cluster.LocalName())
	// TODO-offload: "offload-s3" has to come from config when enable modules more than S3
	migrator.SetOffloadProvider(appState.Modules, "offload-s3")
	appState.Migrator = migrator

	vectorRepo = repo
	// migrator = vectorMigrator
	explorer := traverser.NewExplorer(repo, appState.Logger, appState.Modules, traverser.NewMetrics(appState.Metrics), appState.ServerConfig.Config)
	schemaRepo := schemarepo.NewStore(appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err = schemaRepo.Open(); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema repo")
		os.Exit(1)
	}

	localClassifierRepo, err := classifications.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize classifications repo")
		os.Exit(1)
	}

	// TODO: configure http transport for efficient intra-cluster comm
	classificationsTxClient := clients.NewClusterClassifications(appState.ClusterHttpClient)
	classifierRepo := classifications.NewDistributeRepo(classificationsTxClient,
		appState.Cluster, localClassifierRepo, appState.Logger)
	appState.ClassificationRepo = classifierRepo

	server2port, err := parseNode2Port(appState)
	if len(server2port) == 0 || err != nil {
		appState.Logger.
			WithField("action", "startup").
			WithField("raft-join", appState.ServerConfig.Config.Raft.Join).
			WithError(err).
			Fatal("parsing raft-join")
		os.Exit(1)
	}

	nodeName := appState.Cluster.LocalName()
	dataPath := appState.ServerConfig.Config.Persistence.DataPath

	schemaParser := schema.NewParser(appState.Cluster, vectorIndex.ParseAndValidateConfig, migrator, appState.Modules, appState.ServerConfig.Config.DefaultQuantization, appState.ServerConfig.Config.DefaultShardingCount)

	remoteClientFactory := func(ctx context.Context, address string) (copier.FileReplicationServiceClient, error) {
		clientConn, err := appState.GRPCConnManager.GetConn(address)
		if err != nil {
			return nil, fmt.Errorf("failed to get gRPC connection: %w", err)
		}
		return protocol.NewFileReplicationServiceClient(clientConn), nil
	}

	var nodeSelector cluster.NodeSelector = appState.Cluster

	replicaCopier := copier.New(remoteClientFactory, remoteIndexClient, nodeSelector,
		appState.ServerConfig.Config.ReplicationEngineFileCopyWorkers, dataPath, appState.DB, nodeName, appState.Logger)

	namespacesController := appState.NamespacesController

	rConfig := rCluster.Config{
		WorkDir:                     filepath.Join(dataPath, config.DefaultRaftDir),
		NodeID:                      nodeName,
		Host:                        appState.Cluster.LocalAddr(),
		BindAddr:                    appState.Cluster.LocalBindAddr(),
		RaftPort:                    appState.ServerConfig.Config.Raft.Port,
		RPCPort:                     appState.ServerConfig.Config.Raft.InternalRPCPort,
		RaftRPCMessageMaxSize:       appState.ServerConfig.Config.Raft.RPCMessageMaxSize,
		BootstrapTimeout:            appState.ServerConfig.Config.Raft.BootstrapTimeout,
		BootstrapExpect:             appState.ServerConfig.Config.Raft.BootstrapExpect,
		HeartbeatTimeout:            appState.ServerConfig.Config.Raft.HeartbeatTimeout,
		ElectionTimeout:             appState.ServerConfig.Config.Raft.ElectionTimeout,
		LeaderLeaseTimeout:          appState.ServerConfig.Config.Raft.LeaderLeaseTimeout,
		TimeoutsMultiplier:          appState.ServerConfig.Config.Raft.TimeoutsMultiplier.Get(),
		SnapshotInterval:            appState.ServerConfig.Config.Raft.SnapshotInterval,
		SnapshotThreshold:           appState.ServerConfig.Config.Raft.SnapshotThreshold,
		TrailingLogs:                appState.ServerConfig.Config.Raft.TrailingLogs,
		ConsistencyWaitTimeout:      appState.ServerConfig.Config.Raft.ConsistencyWaitTimeout,
		MetadataOnlyVoters:          appState.ServerConfig.Config.Raft.MetadataOnlyVoters,
		EnableOneNodeRecovery:       appState.ServerConfig.Config.Raft.EnableOneNodeRecovery,
		ForceOneNodeRecovery:        appState.ServerConfig.Config.Raft.ForceOneNodeRecovery,
		SelfRecoveryEnabled:         appState.ServerConfig.Config.Replication.SelfRecoveryEnabled,
		WipedJoinerBarrierTimeout:   appState.ServerConfig.Config.Replication.SelfRecoveryBarrierTimeout,
		DB:                          nil,
		Parser:                      schemaParser,
		NodeNameToPortMap:           server2port,
		NodeSelector:                appState.Cluster,
		Logger:                      appState.Logger,
		IsLocalHost:                 appState.ServerConfig.Config.Cluster.Localhost,
		LoadLegacySchema:            schemaRepo.LoadLegacySchema,
		SentryEnabled:               appState.ServerConfig.Config.Sentry.Enabled,
		AuthzController:             appState.AuthzController,
		RBAC:                        appState.RBAC,
		DynamicUserController:       appState.APIKey.Dynamic,
		NamespacesController:        namespacesController,
		NamespacesEnabled:           appState.ServerConfig.Config.Namespaces.Enabled,
		ReplicaCopier:               replicaCopier,
		AuthNConfig:                 appState.ServerConfig.Config.Authentication,
		ReplicationEngineMaxWorkers: appState.ServerConfig.Config.ReplicationEngineMaxWorkers,
		DistributedTasks:            appState.ServerConfig.Config.DistributedTasks,
		DistributedTaskCollectionExtractors: map[string]distributedtask.CollectionExtractor{
			db.ReindexNamespace: db.ExtractReindexTaskCollection,
		},
		ReplicaMovementEnabled:  appState.ServerConfig.Config.ReplicaMovementEnabled,
		DrainSleep:              appState.ServerConfig.Config.Raft.DrainSleep.Get(),
		MaxTenantsPerCollection: appState.ServerConfig.Config.UsageLimits.MaxTenantsPerCollection,
		UsageLimitsErrorMessage: appState.ServerConfig.Config.UsageLimits.ErrorMessage,
	}
	for _, name := range appState.ServerConfig.Config.Raft.Join[:rConfig.BootstrapExpect] {
		if strings.Contains(name, rConfig.NodeID) {
			rConfig.Voter = true
			break
		}
	}

	appState.ClusterService = rCluster.New(rConfig, appState.AuthzController, appState.AuthzSnapshotter, appState.GRPCServerMetrics)
	migrator.SetCluster(appState.ClusterService.Raft)
	appState.ClusterService.SetInflightDrainer(repo.WaitForLocalInflightWrites)

	// Wrap RestoreClassDir so each post-RAFT-apply class-dir move also
	// fires the orphan-reindex audit on the restored on-disk state.
	// AuditOrphanReindexTrackersIfReady returns a Skipped outcome until
	// the deps closure is installed (below, from the Scheduler.Start
	// goroutine); SetReindexAuditDeps replays any audits requested
	// during the install race window so per-class restores that win
	// the race against deps install do not silently no-op (B2).
	classDirMover := backup.RestoreClassDir(dataPath)
	restoreClassDirWithAudit := func(class string) error {
		if err := classDirMover(class); err != nil {
			return err
		}
		// Background ctx: invoked from the RAFT FSM apply path,
		// which does not propagate an audit-scoped ctx.
		outcome, err := repo.AuditOrphanReindexTrackersIfReady(context.Background())
		if err != nil {
			appState.Logger.WithField("action", "reindex_orphan_audit_post_class_dir_restore").
				WithField("class", class).
				Warnf("reindex orphan audit failed after class-dir restore; the next process restart will retry: %v", err)
		} else if outcome.Status == db.AuditStatusSkipped {
			// Skipped is benign during normal startup (the install
			// goroutine hasn't run yet) but the post-install replay
			// path in SetReindexAuditDeps will pick this up.
			appState.Logger.WithField("action", "reindex_orphan_audit_post_class_dir_restore").
				WithField("class", class).
				WithField("skip_reason", outcome.SkipReason).
				Info("reindex orphan audit skipped after class-dir restore; deferred for post-install replay")
		}
		return nil
	}

	// Wired after Cluster (Raft dep) and before WaitForStartup so schema-replay shard-init can hand off missing shards.
	selfRecoveryOrch := selfrecovery.New(selfrecovery.Config{
		Raft:                   appState.ClusterService.Raft,
		Schema:                 selfRecoverySchemaReader{r: appState.ClusterService.SchemaReader()},
		PathResolver:           selfRecoveryDBPathResolver{db: appState.DB, root: dataPath},
		ClientFactory:          remoteClientFactory,
		NodeSelector:           nodeSelector,
		NodeName:               nodeName,
		Enabled:                appState.ServerConfig.Config.Replication.SelfRecoveryEnabled,
		Concurrency:            appState.ServerConfig.Config.Replication.SelfRecoveryConcurrency,
		MaintenanceModeEnabled: appState.Cluster.MaintenanceModeEnabledForLocalhost,
		OnRecoveryComplete: func(ctx context.Context, collection, shard string) error {
			idx := appState.DB.GetIndex(entschema.ClassName(collection))
			if idx == nil {
				return fmt.Errorf("self-recovery promote: index %q not found", collection)
			}
			return idx.LoadLocalShard(ctx, shard, false)
		},
		Logger: appState.Logger,
	})
	appState.DB.SetSelfRecoveryOrchestrator(selfRecoveryOrch)
	// Expose debug endpoints (incl. test-only force-snapshot) only when the feature is on.
	if appState.ServerConfig.Config.Replication.SelfRecoveryEnabled {
		setupSelfRecoveryHandlers(appState, selfRecoveryOrch)
		setupRaftDebugHandlers(appState, appState.ClusterService.Raft)
	}
	// One-shot reclaim of *.recovering/ leftovers from a downgrade.
	if removed, err := selfRecoveryOrch.CleanupOrphanRecoveryDirs(dataPath); err != nil {
		appState.Logger.WithError(err).Warn("self-recovery orphan cleanup failed")
	} else if len(removed) > 0 {
		appState.Logger.WithField("count", len(removed)).Info("self-recovery: removed orphan recovery dirs")
	}

	executor := schema.NewExecutor(migrator,
		appState.ClusterService.SchemaReader(),
		appState.Logger, restoreClassDirWithAudit,
	)

	offloadmod, _ := appState.Modules.OffloadBackend("offload-s3")

	collectionRetrievalStrategyConfigFlag := configRuntime.NewFeatureFlag(
		configRuntime.CollectionRetrievalStrategyLDKey,
		string(configRuntime.LeaderOnly),
		appState.LDIntegration,
		configRuntime.CollectionRetrievalStrategyEnvVariable,
		appState.Logger,
	)

	schemaManager, err := schema.NewManager(migrator,
		appState.ClusterService.Raft,
		appState.ClusterService.SchemaReader(),
		schemaRepo,
		appState.Logger, appState.Authorizer, &appState.ServerConfig.Config.SchemaHandlerConfig, appState.ServerConfig.Config,
		vectorIndex.ParseAndValidateConfig, appState.Modules, inverted.ValidateConfig,
		appState.Modules, appState.Cluster,
		offloadmod, *schemaParser,
		collectionRetrievalStrategyConfigFlag,
		appState.NamespacesController,
	)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("could not initialize schema manager")
		os.Exit(1)
	}

	appState.SchemaManager = schemaManager
	repo.SetNodeSelector(appState.ClusterService.NodeSelector())
	repo.SetSchemaReader(appState.ClusterService.SchemaReader())
	repo.SetReplicationFSM(appState.ClusterService.ReplicationFsm())
	repo.SetSchemaGetter(appState.SchemaManager)
	repo.SetTenantsActivityManager(appState.SchemaManager)

	// initialize needed services after all components are ready
	postInitModules(appState)

	appState.RemoteIndexIncoming = sharding.NewRemoteIndexIncoming(repo, appState.ClusterService.SchemaReader(), appState.Modules)
	appState.RemoteNodeIncoming = sharding.NewRemoteNodeIncoming(repo)

	backupManager := backup.NewHandler(appState.Logger, appState.ServerConfig.Config.Backup, appState.Authorizer,
		schemaManager, repo, appState.Modules, appState.RBAC, appState.APIKey.Dynamic)
	appState.BackupManager = backupManager

	// Create export participant early so the cluster API server can register it
	exportClient := clients.NewClusterExports(appState.ClusterHttpClient)
	appState.ExportMetrics = exportusecase.NewExportMetrics(prometheus.DefaultRegisterer)
	appState.ExportParticipant = exportusecase.NewParticipant(
		appState.DB, appState.Modules, appState.Logger,
		exportClient, appState.Cluster, appState.Cluster.LocalName(),
		appState.ExportMetrics,
		appState.ServerConfig.Config.ExportParallelism,
	)

	appState.InternalServer = clusterapi.NewServer(appState)
	enterrors.GoWrapper(func() { appState.InternalServer.Serve() }, appState.Logger)

	vectorRepo.SetSchemaGetter(schemaManager)
	explorer.SetSchemaGetter(schemaManager)
	appState.Modules.SetSchemaGetter(schemaManager)

	appState.Traverser = traverser.NewTraverser(appState.ServerConfig,
		appState.Logger, appState.Authorizer, vectorRepo, explorer, schemaManager,
		appState.Modules, traverser.NewMetrics(appState.Metrics),
		appState.ServerConfig.Config.MaximumConcurrentGetRequests)

	updateSchemaCallback := makeUpdateSchemaCall(appState)
	executor.RegisterSchemaUpdateCallback(updateSchemaCallback)

	bitmapBufPool, bitmapBufPoolClose := configureBitmapBufPool(appState)
	repo.SetBitmapBufPool(bitmapBufPool, bitmapBufPoolClose)

	var reindexCtx context.Context
	reindexCtx, appState.ReindexCtxCancel = context.WithCancelCause(serverShutdownCtx)
	// Discover in-flight runtime reindex tasks from disk so the static
	// ShardReindexerV3 can re-register their double-write callbacks via
	// OnAfterLsmInit during shard load — BEFORE any post-restart write
	// reaches the shard. Without this, writes between shard init and the
	// deferred swap go only to the old main bucket and are silently lost.
	// Reads: <data>/<index>/<shard>/lsm/.migrations/<dir>/payload.mig
	// (written by ReindexProvider.persistRecoveryRecord before reindex
	// starts), plus the existing started.mig / tidied.mig sentinels to
	// decide which migrations are still in flight.
	recoveredReindexes, recoveryErr := db.DiscoverInFlightReindexTasks(
		appState.ServerConfig.Config.Persistence.DataPath,
		appState.Logger,
		appState.SchemaManager,
	)
	if recoveryErr != nil {
		appState.Logger.WithError(recoveryErr).
			Warn("reindex recovery: disk scan failed; writes during the swap-recovery window may be lost")
	}
	reindexer := configureReindexer(recoveredReindexes, appState.Logger)
	repo.SetReindexer(reindexer)

	metaStoreReady := newMetaStoreReady()
	enterrors.GoWrapper(func() {
		if err := appState.ClusterService.Open(context.Background(), executor); err != nil {
			appState.Logger.
				WithField("action", "startup").
				WithError(err).
				Fatal("could not open cloud meta store")
			metaStoreReady.failure(err)
		} else {
			// Past initial FSM replay: further AddClass calls are runtime additions, not data-loss candidates.
			appState.DB.MarkRaftBootstrapComplete()
			metaStoreReady.success()
		}
	}, appState.Logger)

	// TODO-RAFT: refactor remove this sleep
	// this sleep was used to block GraphQL and give time to RAFT to start.
	time.Sleep(2 * time.Second)

	appState.AutoSchemaManager = objects.NewAutoSchemaManager(schemaManager, vectorRepo, appState.ServerConfig,
		appState.Logger, prometheus.DefaultRegisterer)
	batchManager := objects.NewBatchManager(vectorRepo, appState.Modules,
		schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.Metrics, appState.AutoSchemaManager)
	appState.BatchManager = batchManager

	err = migrator.AdjustFilterablePropSettings(ctx)
	if err != nil {
		appState.Logger.
			WithError(err).
			WithField("action", "adjustFilterablePropSettings").
			Fatal("migration failed")
		os.Exit(1)
	}

	// FIXME to avoid import cycles, tasks are passed as strings
	reindexTaskNamesWithArgs := map[string]any{}
	reindexFinished := make(chan error, 1)

	if appState.ServerConfig.Config.ReindexSetToRoaringsetAtStartup {
		reindexTaskNamesWithArgs["ShardInvertedReindexTaskSetToRoaringSet"] = nil
	}
	if appState.ServerConfig.Config.IndexMissingTextFilterableAtStartup {
		reindexTaskNamesWithArgs["ShardInvertedReindexTaskMissingTextFilterable"] = nil
	}
	if len(appState.ServerConfig.Config.ReindexIndexesAtStartup) > 0 {
		reindexTaskNamesWithArgs["ShardInvertedReindexTask_SpecifiedIndex"] = appState.ServerConfig.Config.ReindexIndexesAtStartup
	}

	if len(reindexTaskNamesWithArgs) > 0 {
		// start reindexing inverted indexes (if requested by user) in the background
		// allowing db to complete api configuration and start handling requests
		enterrors.GoWrapper(func() {
			l := appState.Logger.WithField("action", "startup")
			if err := metaStoreReady.waitForMetaStore(); err != nil {
				l.WithError(err).Error("Reindexing inverted indexes skipped")
				return
			}
			l.Info("Reindexing inverted indexes")
			reindexFinished <- migrator.InvertedReindex(reindexCtx, reindexTaskNamesWithArgs)
		}, appState.Logger)
	}

	appState.ObjectTTLCoordinator = objectttl.NewCoordinator(appState.ClusterService.SchemaReader(), appState.SchemaManager, appState.DB,
		appState.Logger, appState.ClusterHttpClient, appState.Cluster, appState.ObjectTTLLocalStatus)

	namespaceCleanupCoordinator := namespacecleanup.NewCoordinator(
		appState.NamespacesController,
		rCluster.NewSchemaNamespaceLister(appState.ClusterService.SchemaReader()),
		appState.APIKey.Dynamic,
		appState.ClusterService.Raft,
		appState.ClusterService.IsLeader,
		appState.Logger,
	)

	enterrors.GoWrapper(func() {
		l := appState.Logger.WithField("action", "startup")
		if err := metaStoreReady.waitForMetaStore(); err != nil {
			l.WithError(err).Error("Configuring crons skipped")
			return
		}
		l.Info("Configuring crons")
		if err := appState.Crons.Init(appState.ClusterService, appState.ObjectTTLCoordinator, namespaceCleanupCoordinator); err != nil {
			l.WithError(err).Fatal("Configuring crons failed")
		}
	}, appState.Logger)

	// Namespace startup invariant: verify the NAMESPACES_ENABLED flag is
	// compatible with cluster state (schema classes + namespace entities).
	// Runs after the meta store is ready so both sources are authoritative;
	// placed in a goroutine to avoid blocking other init work that does not
	// depend on the meta store. Fatals on mismatch — a misconfigured cluster
	// must not come up.
	enterrors.GoWrapper(func() {
		l := appState.Logger.WithField("action", "startup")
		if err := metaStoreReady.waitForMetaStore(); err != nil {
			l.WithError(err).Fatal("meta store failed to become ready; cannot verify namespace startup invariants")
		}
		schemaSnapshot := appState.SchemaManager.GetSchemaSkipAuth()
		var classNames []string
		if schemaSnapshot.Objects != nil {
			for _, c := range schemaSnapshot.Objects.Classes {
				classNames = append(classNames, c.Class)
			}
		}
		if err := enforceNamespaceStartupInvariants(
			appState.ServerConfig.Config.Namespaces.Enabled,
			appState.ServerConfig.Config.Persistence.LSMSkipWriteClassNameEnabled,
			appState.ServerConfig.Config.Replication.MaximumFactor,
			classNames,
			appState.ClusterService.NamespaceCount(),
		); err != nil {
			l.Fatal(err)
		}
	}, appState.Logger)

	configureServer = makeConfigureServer(appState)

	// Add dimensions to all the objects in the database, if requested by the user
	if appState.ServerConfig.Config.ReindexVectorDimensionsAtStartup && repo.GetConfig().TrackVectorDimensions {
		appState.Logger.
			WithField("action", "startup").
			Info("Reindexing dimensions")
		migrator.RecalculateVectorDimensions(ctx)
	}

	// Add recount properties of all the objects in the database, if requested by the user
	if appState.ServerConfig.Config.RecountPropertiesAtStartup {
		migrator.RecountProperties(ctx)
	}

	providers := map[string]distributedtask.Provider{}

	if entconfig.Enabled(os.Getenv("SHARD_NOOP_PROVIDER_ENABLED")) {
		shardNoopProvider := distributedtask.NewShardNoopProvider(
			appState.Cluster.LocalName(), appState.Logger, repo,
			appState.ServerConfig.Config.Persistence.DataPath,
		)
		providers[distributedtask.ShardNoopProviderNamespace] = shardNoopProvider
		setupShardNoopDebugHandler(appState, shardNoopProvider)
	}

	initReindexAndDistributedTasks(appState, repo, providers, recoveredReindexes, metricsRegisterer, serverShutdownCtx)
	enterrors.GoWrapper(func() {
		// Do not launch scheduler until the full RAFT state is restored to avoid needlessly starting
		// and stopping tasks.
		// Additionally, not-ready RAFT state could lead to lose of local task metadata.
		if metaStoreReady.waitForMetaStore() != nil {
			return
		}
		if err = appState.DistributedTaskScheduler.Start(ctx); err != nil {
			appState.Logger.WithError(err).WithField("action", "startup").
				Error("failed to start distributed task scheduler")
			return
		}

		// Post-bootstrap orphan-reindex audit. Must run AFTER
		// Scheduler.Start so the lookup observes the steady-state
		// view of RAFT-known tasks.
		//
		// Use serverShutdownCtx, not the outer MakeAppState ctx: the
		// outer ctx is canceled by configureAPI's defer once HTTP
		// server init returns, which is before this goroutine runs.
		// That cancellation propagates into Store.PauseCompaction and
		// surfaces as a misleading "context canceled" error.
		auditCtx := serverShutdownCtx
		type taskKey struct {
			id      string
			version uint64
		}
		// buildKnownTask returns an error on ListDistributedTasks
		// failure. Callers MUST propagate the error rather than
		// substitute a soft default — prior versions returned a
		// "treat every tracker as known" closure, which silently
		// misclassified orphans during a DTM partition. Explicit
		// error makes the failure path operator-observable.
		buildKnownTask := func() (db.KnownReindexTaskLookup, error) {
			tasksByNamespace, err := appState.ClusterService.ListDistributedTasks(auditCtx)
			if err != nil {
				return nil, fmt.Errorf("ListDistributedTasks: %w", err)
			}
			live := make(map[taskKey]bool, len(tasksByNamespace[db.ReindexNamespace]))
			for _, task := range tasksByNamespace[db.ReindexNamespace] {
				live[taskKey{task.ID, task.Version}] = db.IsLiveReindexTaskStatus(task.Status)
			}
			return func(taskID string, taskVersion uint64) bool {
				return live[taskKey{taskID, taskVersion}]
			}, nil
		}
		// Wait until ListDistributedTasks succeeds at least once before
		// running the startup audit. Without this, a transient DTM-list
		// failure during the bootstrap window means buildKnownTask
		// returns nil and the audit skips → orphan tracker dirs left
		// behind by a backup-restore are never classified. Exponential
		// backoff capped at 5s; bound the total wait so an offline
		// cluster doesn't block startup indefinitely.
		auditReadyBackoff := 100 * time.Millisecond
		auditReadyDeadline := time.Now().Add(60 * time.Second)
		for {
			_, listErr := appState.ClusterService.ListDistributedTasks(auditCtx)
			if listErr == nil {
				break
			}
			if time.Now().After(auditReadyDeadline) {
				appState.Logger.WithField("action", "reindex_orphan_audit").
					Errorf("reindex orphan audit: DTM list unavailable after 60s; skipping startup audit. Orphans from a prior restore (if any) will be picked up by the next process restart: %v", listErr)
				break
			}
			appState.Logger.WithField("action", "reindex_orphan_audit").
				Debugf("reindex orphan audit: DTM list not yet ready; retrying in %s: %v", auditReadyBackoff, listErr)
			select {
			case <-time.After(auditReadyBackoff):
			case <-auditCtx.Done():
				return
			}
			auditReadyBackoff = min(auditReadyBackoff*2, 5*time.Second)
		}
		startupLookup, startupBuildErr := buildKnownTask()
		if startupBuildErr != nil {
			appState.Logger.WithField("action", "startup").
				Errorf("reindex orphan audit: builder failed; skipping startup audit. The next process restart will retry: %v", startupBuildErr)
		} else if _, err := repo.AuditOrphanReindexTrackers(auditCtx, startupLookup, appState.Logger); err != nil {
			appState.Logger.WithField("action", "startup").
				Warnf("reindex orphan audit did not run cleanly; restored clusters may retain orphan sidecar buckets: %v", err)
		}

		// Install the audit deps so the post-restore-class-dir hook
		// (wired into RestoreClassDir above) can run the audit.
		repo.SetReindexAuditDeps(buildKnownTask, appState.Logger)

		// Install the backup-gate activity lookup so refuseIfReindexInFlight
		// consults DTM rather than per-shard filesystem markers. Built per
		// backup precheck so the snapshot is fresh; on list failure we
		// fall back to refusing every backup until DTM is reachable, to
		// avoid races against in-flight reindexes that the local node
		// cannot see.
		type shardKey struct {
			collection string
			shardName  string
		}
		buildShardReindexActivity := func() db.ShardReindexActivityLookup {
			tasksByNamespace, err := appState.ClusterService.ListDistributedTasks(auditCtx)
			if err != nil {
				appState.Logger.WithField("action", "backup_reindex_gate").
					Warnf("backup-reindex gate: cannot list DTM tasks; refusing all backups until DTM is reachable: %v", err)
				return func(string, string) bool { return true }
			}
			live := make(map[shardKey]bool)
			for _, task := range tasksByNamespace[db.ReindexNamespace] {
				if !db.IsLiveReindexTaskStatus(task.Status) {
					continue
				}
				var payload db.ReindexTaskPayload
				if err := json.Unmarshal(task.Payload, &payload); err != nil {
					appState.Logger.WithField("action", "backup_reindex_gate").
						WithField("task_id", task.ID).
						Warnf("backup-reindex gate: cannot decode task payload; skipping task: %v", err)
					continue
				}
				for _, shardName := range payload.UnitToShard {
					live[shardKey{payload.Collection, shardName}] = true
				}
			}
			return func(collection, shardName string) bool {
				return live[shardKey{collection, shardName}]
			}
		}
		repo.SetShardReindexActivityLookup(buildShardReindexActivity)
		// S1: the DTM-activity lookup flips a shard "free" the moment a
		// task lands in a terminal status; autoCleanupAfterTerminal then
		// tears the sidecar __reindex / __ingest dirs over the next
		// tens of seconds. The cleanup-in-progress lookup keeps the gate
		// closed for that window so a backup landing in the gap doesn't
		// snapshot half-removed sidecars.
		repo.SetReindexCleanupInProgressLookup(appState.ReindexProvider.CleanupInProgressLookupBuilder())
	}, appState.Logger)

	return appState
}

func configureBitmapBufPool(appState *state.State) (pool roaringset.BitmapBufPool, close func()) {
	return roaringset.NewBitmapBufPoolDefault(appState.Logger, appState.Metrics,
		appState.ServerConfig.Config.QueryBitmapBufsMaxBufSize,
		appState.ServerConfig.Config.QueryBitmapBufsMaxMemory)
}

// initReindexAndDistributedTasks builds the reindex provider, registers it in
// the distributedtask providers map, constructs the scheduler, and wires the
// cross-FSM conflict + schema-mutation detectors on the cluster service.
// Mutates appState (ReindexProvider, DistributedTaskScheduler) and the
// providers map. The scheduler is NOT started — caller gates Start() on RAFT
// readiness.
func initReindexAndDistributedTasks(
	appState *state.State,
	repo *db.DB,
	providers map[string]distributedtask.Provider,
	recoveredReindexes []db.RecoveredReindex,
	metricsRegisterer prometheus.Registerer,
	serverShutdownCtx context.Context,
) {
	reindexProvider := db.NewReindexProvider(
		repo, appState.SchemaManager, appState.Logger,
		appState.Cluster.LocalName(),
		appState.ServerConfig.Config.DistributedTasks.ReindexConcurrency.Get,
		serverShutdownCtx,
	)
	// Seed re-uses the SAME task instances ShardReindexerV3 registered, so
	// OnGroupCompleted's swap phase doesn't take the rehydrate path and try
	// to load already-loaded ingest buckets.
	db.SeedReindexProviderFromRecovery(reindexProvider, recoveredReindexes)
	providers[db.ReindexNamespace] = reindexProvider
	appState.ReindexProvider = reindexProvider

	appState.DistributedTaskScheduler = distributedtask.NewScheduler(distributedtask.SchedulerParams{
		CompletionRecorder: appState.ClusterService.Raft,
		TaskLister:         appState.ClusterService.Raft,
		TaskCleaner:        appState.ClusterService.Raft,
		TaskFinalizer:      appState.ClusterService.Raft,
		// AckRecorder gates MarkDistributedTaskFinalized on per-node acks
		// after OnGroupCompleted, so a failed ack flips the task to FAILED
		// before the cluster-wide schema flip can run.
		AckRecorder:       appState.ClusterService.Raft,
		Providers:         providers,
		Logger:            appState.Logger,
		MetricsRegisterer: metricsRegisterer,
		LocalNode:         appState.Cluster.LocalName(),
		TickInterval:      appState.ServerConfig.Config.DistributedTasks.SchedulerTickInterval,
		CompletedTaskTTL:  appState.ServerConfig.Config.DistributedTasks.CompletedTaskTTL,
	})
	// Reactive notifier: without this, barriers stagger by up to the tick
	// interval across nodes. See [distributedtask.SchedulerNotifier].
	appState.ClusterService.SetDistributedTaskSchedulerNotifier(appState.DistributedTaskScheduler)

	// FSM-deterministic conflict reject (closes the multi-node parallel-submit
	// race that per-node submit locks cannot cover; see
	// weaviate/0-weaviate-issues#54 / weaviate/weaviate#10675).
	conflictDetectors := map[string]distributedtask.ConflictDetector{}
	for ns, p := range providers {
		if cd, ok := p.(distributedtask.ConflictDetector); ok {
			conflictDetectors[ns] = cd
		}
	}
	appState.ClusterService.SetDistributedTaskConflictDetectors(conflictDetectors)

	// Symmetric guard in the other direction: protect in-flight tasks from
	// out-of-band schema mutations.
	schemaMutationDetectors := map[string]distributedtask.SchemaMutationDetector{}
	for ns, p := range providers {
		if smd, ok := p.(distributedtask.SchemaMutationDetector); ok {
			schemaMutationDetectors[ns] = smd
		}
	}
	appState.ClusterService.SetDistributedTaskSchemaMutationDetectors(schemaMutationDetectors)
}

func configureReindexer(recovered []db.RecoveredReindex, logger logrus.FieldLogger) db.ShardReindexerV3 {
	// All reindex operations are now triggered via the REST API
	// (DTM-based). The V3 startup reindexer is no longer used for
	// kicking off new reindexes — but we still need it for restart
	// recovery: in-flight runtime reindex tasks discovered on disk
	// register here so that OnAfterLsmInit fires during shard load and
	// re-installs the double-write callbacks before any post-restart
	// write reaches the shard.
	if len(recovered) == 0 {
		return db.NewShardReindexerV3Noop()
	}
	logger.WithField("count", len(recovered)).
		Info("reindex recovery: registering in-flight tasks discovered on disk")
	return db.NewShardReindexerV3FromRecovered(recovered, logger)
}

// enforceNamespaceStartupInvariants decides whether the current cluster state
// is compatible with the NAMESPACES_ENABLED flag. It is a pure function so the
// decision logic can be unit-tested without fighting os.Exit; the caller wires
// a non-nil return to logrus.Fatal.
//
// A class name is considered namespace-qualified iff it contains
// entschema.NamespaceSeparator (":"), which is forbidden in plain class names
// by ClassNameRegexCore and locked by TestValidateClassName_RejectsNamespaceSeparator.
func enforceNamespaceStartupInvariants(enabled bool, lsmSkipWriteClassNameEnabled bool, maxReplicationFactor int, classNames []string, nsCount int) error {
	var nonNamespacedCount, namespacedCount int
	var nonNamespacedExample, namespacedExample string
	for _, name := range classNames {
		if strings.Contains(name, entschema.NamespaceSeparator) {
			if namespacedCount == 0 {
				namespacedExample = name
			}
			namespacedCount++
		} else {
			if nonNamespacedCount == 0 {
				nonNamespacedExample = name
			}
			nonNamespacedCount++
		}
	}

	switch {
	case enabled && !lsmSkipWriteClassNameEnabled:
		return fmt.Errorf("internal invariant violated: NAMESPACES_ENABLED=true but LSMSkipWriteClassNameEnabled=false; env-var wiring in usecases/config/environment.go is broken")
	case enabled && maxReplicationFactor != 1:
		// Each namespace's shards are pinned to a single home_node; RF>1
		// would either leave replicas off-namespace or contradict the pin.
		return fmt.Errorf("NAMESPACES_ENABLED=true requires REPLICATION_MAXIMUM_FACTOR=1; got %d", maxReplicationFactor)
	case enabled && nonNamespacedCount > 0:
		return fmt.Errorf("NAMESPACES_ENABLED=true but cluster has %d non-namespaced collection(s) (e.g. %q); namespaces can only be enabled on newly bootstrapped clusters or on clusters whose collections are all already namespace-qualified", nonNamespacedCount, nonNamespacedExample)
	case !enabled && nsCount > 0:
		return fmt.Errorf("NAMESPACES_ENABLED=false but cluster has %d existing namespace entities; this is not supported", nsCount)
	case !enabled && namespacedCount > 0:
		// Guards against disabling namespaces on a namespaced cluster.
		return fmt.Errorf("NAMESPACES_ENABLED=false but cluster has %d namespace-qualified collection(s) (e.g. %q); refusing to start with inconsistent state", namespacedCount, namespacedExample)
	}
	return nil
}

type metaStoreReady struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
}

func newMetaStoreReady() *metaStoreReady {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &metaStoreReady{ctx: ctx, cancel: cancel}
}

func (msr *metaStoreReady) success() {
	msr.cancel(nil)
}

func (msr *metaStoreReady) failure(err error) {
	msr.cancel(err)
}

func (msr *metaStoreReady) waitForMetaStore() error {
	<-msr.ctx.Done()
	if err := context.Cause(msr.ctx); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func parseNode2Port(appState *state.State) (m map[string]int, err error) {
	m = make(map[string]int, len(appState.ServerConfig.Config.Raft.Join))
	for _, raftNamePort := range appState.ServerConfig.Config.Raft.Join {
		np := strings.Split(raftNamePort, ":")
		if np[0] == appState.Cluster.LocalName() {
			m[np[0]] = appState.ServerConfig.Config.Raft.Port
			continue
		}
		if m[np[0]], err = strconv.Atoi(np[1]); err != nil {
			return m, fmt.Errorf("expect integer as raft port: got %s:: %w", raftNamePort, err)
		}
	}

	return m, nil
}

// parseVotersNames parses names of all voters.
// If we reach this point, we assume that the configuration is valid
func parseVotersNames(cfg config.Raft) (m map[string]struct{}) {
	m = make(map[string]struct{}, cfg.BootstrapExpect)
	for _, raftNamePort := range cfg.Join[:cfg.BootstrapExpect] {
		m[strings.Split(raftNamePort, ":")[0]] = struct{}{}
	}
	return m
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	defer cancel()

	serverShutdownCtx, serverShutdownCancel := context.WithCancelCause(context.Background())
	appState := MakeAppState(ctx, serverShutdownCtx, connectorOptionGroup)

	appState.Logger.WithFields(logrus.Fields{
		"server_version": config.ServerVersion,
		"version":        build.Version,
	}).Infof("configured versions")

	api.ServeError = openapierrors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()
	// REST-only asyncEnabled shim — see adapters/handlers/rest/restcompat.
	api.JSONProducer = restcompat.NewJSONProducer()

	api.OidcAuth = composer.New(
		appState.ServerConfig.Config.Authentication,
		appState.ServerConfig.Config.Namespaces.Enabled, appState.Logger,
		appState.APIKey, appState.OIDC)

	api.Logger = func(msg string, args ...interface{}) {
		appState.Logger.WithFields(logrus.Fields{"action": "restapi_management", "version": build.Version}).Infof(msg, args...)
	}

	classifier := classification.New(appState.SchemaManager, appState.ClassificationRepo, appState.DB, // the DB is the vectorrepo
		appState.Authorizer,
		appState.Logger, appState.Modules)

	setupAuthnHandlers(api,
		appState.ClusterService.Raft,
		appState.ServerConfig.Config.Authorization.Rbac,
		appState.Logger)
	authz.SetupHandlers(api,
		appState.ClusterService.Raft,
		appState.SchemaManager,
		appState.ServerConfig.Config.Authentication.APIKey,
		appState.ServerConfig.Config.Authentication.OIDC,
		appState.ServerConfig.Config.Authorization.Rbac,
		appState.ServerConfig.Config.Namespaces.Enabled,
		appState.Metrics,
		appState.Authorizer,
		appState.Logger)

	replicationHandlers.SetupHandlers(appState.ServerConfig.Config.ReplicaMovementEnabled, api, appState.ClusterService.Raft, appState.Metrics, appState.Authorizer, appState.Logger)

	remoteDbUsers := clients.NewRemoteUser(appState.ClusterHttpClient, appState.Cluster)
	db_users.SetupHandlers(api, appState.ClusterService.Raft, appState.Authorizer, appState.ServerConfig.Config.Authentication, appState.ServerConfig.Config.Authorization, remoteDbUsers, appState.SchemaManager, appState.ServerConfig.Config.Namespaces.Enabled, appState.NamespacesController, appState.Logger)
	rest_namespaces.SetupHandlers(appState.ServerConfig.Config.Namespaces.Enabled, api, appState.ClusterService.Raft, appState.Authorizer, appState.Logger)

	setupSchemaHandlers(api, appState.SchemaManager, appState.Metrics, appState.Logger, appState.ClusterService.Raft, appState.ReindexSubmitLocks, appState.ServerConfig.Config.Namespaces.Enabled)
	setupIndexesHandlers(api, appState)
	setupTokenizeHandlers(api, appState.SchemaManager, appState.ServerConfig.Config.Namespaces.Enabled, appState.Logger)
	setupAliasesHandlers(api, appState.SchemaManager, appState.Metrics, appState.Logger)
	objectsManager := objects.NewManager(appState.SchemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.DB, appState.Modules,
		objects.NewMetrics(appState.Metrics), appState.MemWatch, appState.AutoSchemaManager)
	setupObjectHandlers(api, objectsManager, appState.ServerConfig.Config, appState.Logger,
		appState.Modules, appState.Metrics)
	setupObjectBatchHandlers(api, appState.BatchManager, appState.Metrics, appState.Logger)
	setupGraphQLHandlers(api, appState, appState.SchemaManager, appState.ServerConfig.Config.DisableGraphQL,
		appState.ServerConfig.Config.Namespaces.Enabled, appState.Metrics, appState.Logger)
	setupMiscHandlers(api, appState.ServerConfig, appState.Modules,
		appState.Metrics, appState.Logger)
	setupClassificationHandlers(api, classifier, appState.ServerConfig.Config.Namespaces.Enabled, appState.Metrics, appState.Logger)
	backupScheduler := startBackupScheduler(appState)
	setupBackupHandlers(api, backupScheduler, appState.ServerConfig.Config.Authorization.Rbac, appState.Metrics, appState.Logger)
	exportScheduler := startExportScheduler(appState)
	setupExportHandlers(api, exportScheduler, appState.Metrics, appState.Logger)
	setupNodesHandlers(api, appState.SchemaManager, appState.DB, appState)
	setupDistributedTasksHandlers(api, appState.Authorizer, appState.ClusterService.Raft)

	telemeter := telemetry.New(
		appState.DB,
		appState.SchemaManager,
		appState.Logger,
		getTelemetryURL(appState),
		appState.ServerConfig.Config.TelemetryPushInterval,
		telemetryEnabled(appState),
	)

	var grpcInstrument []grpc.ServerOption
	if appState.ServerConfig.Config.Monitoring.Enabled {
		grpcInstrument = monitoring.InstrumentGrpc(appState.GRPCServerMetrics)
	}

	grpcServer, batchDrain := createGrpcServer(appState, telemeter.GetClientTracker(), telemeter.GetIntegrationTracker(), grpcInstrument...)

	setupMiddlewares := makeSetupMiddlewares(appState)
	setupGlobalMiddleware := makeSetupGlobalMiddleware(appState, api.Context(), telemeter)
	if telemetryEnabled(appState) {
		enterrors.GoWrapper(func() {
			if err := telemeter.Start(context.Background()); err != nil {
				appState.Logger.
					WithField("action", "startup").
					Errorf("telemetry failed to start: %s", err.Error())
			}
		}, appState.Logger)
		setupTelemetryDebugHandlers(telemeter)
	}
	if entconfig.Enabled(os.Getenv("ENABLE_CLEANUP_UNFINISHED_BACKUPS")) {
		enterrors.GoWrapper(
			func() {
				// cleanup unfinished backups on startup
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				backupScheduler.CleanupUnfinishedBackups(ctx)
			}, appState.Logger)
	}

	api.PreServerShutdown = func() {
		// Reject new export requests and signal in-flight exports to stop
		// early, while the server can still serve other requests. The actual
		// wait for export drain happens in ServerShutdown.
		exportScheduler.StartShutdown()
		appState.ExportParticipant.StartShutdown()
		batchDrain()
	}

	api.ServerShutdown = func() {
		// leave memberlist first to announce node graceful departure
		if err := appState.Cluster.Leave(); err != nil {
			appState.Logger.WithError(err).Error("leave node from cluster")
		}

		// PreServerShutdown already called StartShutdown so exports are signaled to stop, wait here until completion.
		exportDone := make(chan struct{})
		enterrors.GoWrapper(func() {
			defer close(exportDone)
			exportCtx, exportCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer exportCancel()
			if err := appState.ExportParticipant.Shutdown(exportCtx); err != nil {
				appState.Logger.
					WithError(err).
					WithField("action", "shutdown export participant").
					Errorf("failed to gracefully shutdown")
			}
		}, appState.Logger)

		// drain any ongoing operations
		time.Sleep(appState.ServerConfig.Config.Raft.DrainSleep.Get())

		if telemetryEnabled(appState) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// must be shutdown before the db, to ensure the
			// termination payload contains the correct
			// object count
			if err := telemeter.Stop(ctx); err != nil {
				appState.Logger.WithField("action", "stop_telemetry").
					Errorf("failed to stop telemetry: %s", err.Error())
			}
		}

		// Shutdown OTEL tracing
		if err := opentelemetry.Shutdown(ctx); err != nil {
			appState.Logger.WithField("action", "stop_opentelemetry").
				Errorf("failed to stop opentelemetry: %s", err.Error())
		}

		serverShutdownCancel(fmt.Errorf("server shutdown"))

		if appState.DistributedTaskScheduler != nil {
			appState.DistributedTaskScheduler.Close()
		}

		// close grpc client connections
		appState.ReplGRPCConnManager.Close()
		appState.GRPCConnManager.Close()

		// gracefully stop gRPC server
		grpcServer.GracefulStop()

		if appState.ServerConfig.Config.Sentry.Enabled {
			sentry.Flush(2 * time.Second)
		}

		// Ensure export cleanup finished before closing the infrastructure
		// it depends on (internal server, cluster service, modules).
		<-exportDone

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		if err := appState.InternalServer.Close(ctx); err != nil {
			appState.Logger.
				WithError(err).
				WithField("action", "shutdown internal server").
				Errorf("failed to gracefully shutdown")
		}

		if err := appState.ClusterService.Close(ctx); err != nil {
			appState.Logger.
				WithError(err).
				WithField("action", "shutdown cluster service").
				Errorf("failed to gracefully shutdown")
		}

		if err := appState.APIKey.Dynamic.Close(); err != nil {
			appState.Logger.
				WithError(err).
				WithField("action", "shutdown db users").
				Errorf("failed to gracefully shutdown")
		}

		if err := appState.Modules.Close(); err != nil {
			appState.Logger.
				WithError(err).
				WithField("action", "shutdown modules").
				Errorf("failed to gracefully shutdown")
		}
	}

	startGrpcServer(grpcServer, appState)
	setupMCPHandlers(api, appState, objectsManager)
	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

func startBackupScheduler(appState *state.State) *backup.Scheduler {
	backupScheduler := backup.NewScheduler(
		appState.Authorizer,
		clients.NewClusterBackups(appState.ClusterHttpClient),
		appState.DB, appState.Modules,
		membership{appState.Cluster, appState.ClusterService},
		appState.SchemaManager,
		appState.Logger)
	return backupScheduler
}

func startExportScheduler(appState *state.State) *exportusecase.Scheduler {
	return exportusecase.NewScheduler(
		appState.Authorizer,
		appState.ServerConfig.Config.Authorization.Rbac,
		appState.ServerConfig.Config.Export,
		appState.DB,
		appState.Modules,
		appState.Logger,
		clients.NewClusterExports(appState.ClusterHttpClient),
		appState.Cluster,
		appState.Cluster.LocalName(),
		appState.ExportParticipant,
		appState.ExportMetrics,
	)
}

// TODO: Split up and don't write into global variables. Instead return an appState
func startupRoutine(ctx, serverShutdownCtx context.Context, options *swag.CommandLineOptionsGroup) *state.State {
	appState := &state.State{}

	logger := logger()
	appState.Logger = logger

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created startup context, nothing done so far")

	ldInteg, err := configRuntime.ConfigureLDIntegration()
	if err != nil {
		logger.WithField("action", "startup").Infof("Feature flag LD integration disabled: %s", err)
	}
	appState.LDIntegration = ldInteg
	// Load the config using the flags
	serverConfig := &config.WeaviateConfig{}
	appState.ServerConfig = serverConfig
	err = serverConfig.LoadConfig(options, logger)
	if err != nil {
		logger.WithField("action", "startup").WithError(err).Error("could not load config")
		logger.Exit(1)
	}
	// Initialize runtime config and load overridden config
	runtimeConfigManager := initRuntimeOverrides(appState)
	dataPath := serverConfig.Config.Persistence.DataPath
	if err := os.MkdirAll(dataPath, 0o777); err != nil {
		logger.WithField("action", "startup").
			WithField("path", dataPath).Error("cannot create data directory")
		logger.Exit(1)
	}

	monitoring.InitConfig(serverConfig.Config.Monitoring)

	if serverConfig.Config.DisableGraphQL {
		logger.WithFields(logrus.Fields{
			"action":          "startup",
			"disable_graphql": true,
		}).Warnf("GraphQL API disabled, relying only on gRPC API for querying. " +
			"This is considered experimental and will likely experience breaking changes " +
			"before reaching general availability")
	}

	logger.WithFields(logrus.Fields{
		"action":                    "startup",
		"default_vectorizer_module": serverConfig.Config.DefaultVectorizerModule,
	}).Infof("the default vectorizer modules is set to %q, as a result all new "+
		"schema classes without an explicit vectorizer setting, will use this "+
		"vectorizer", serverConfig.Config.DefaultVectorizerModule)

	logger.WithFields(logrus.Fields{
		"action":              "startup",
		"auto_schema_enabled": serverConfig.Config.AutoSchema.Enabled,
	}).Infof("auto schema enabled setting is set to \"%v\"", serverConfig.Config.AutoSchema.Enabled)

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("config loaded")

	// configureOIDC captures appState.NamespacesController as the
	// classifier's nsExister, so the controller must be initialised
	// before this call.
	appState.NamespacesController = usecasesNamespaces.NewController(logger)
	appState.OIDC = configureOIDC(appState)
	appState.APIKey = configureAPIKey(appState)
	appState.APIKeyRemote = apikey.NewRemoteApiKey(appState.APIKey)
	appState.AnonymousAccess = configureAnonymousAccess(appState)
	if err = configureAuthorizer(appState); err != nil {
		logger.WithField("action", "startup").WithField("error", err).Error("cannot configure authorizer")
		logger.Exit(1)
	}
	appState.Crons = configureCrons(appState, serverShutdownCtx)

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("configured OIDC and anonymous access client")

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("initialized schema")

	var nonStorageNodes map[string]struct{}
	if cfg := serverConfig.Config.Raft; cfg.MetadataOnlyVoters {
		nonStorageNodes = parseVotersNames(cfg)
	}

	serverConfig.Config.Cluster.RaftBootstrapExpect = serverConfig.Config.Raft.BootstrapExpect
	clusterState, err := cluster.Init(serverConfig.Config.Cluster, serverConfig.Config.Raft.TimeoutsMultiplier.Get(), dataPath, nonStorageNodes, logger)
	if err != nil {
		logger.WithField("action", "startup").WithError(err).
			Error("could not init cluster state")
		logger.Exit(1)
	}

	appState.Cluster = clusterState
	appState.Logger.
		WithField("action", "startup").
		Debug("startup routine complete")

	// Register enabled modules
	if err := registerModules(appState); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("modules didn't load")
	}
	// while we accept an overall longer startup, e.g. due to a recovery, we
	// still want to limit the module startup context, as that's mostly service
	// discovery / dependency checking
	moduleCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	if err := initModules(moduleCtx, appState); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("modules didn't initialize")
	}
	// now that modules are loaded we can run the remaining config validation
	// which is module dependent
	if err := appState.ServerConfig.Config.ValidateModules(appState.Modules); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("invalid config")
	}

	// Initialize runtime config hooks and start runtime config background process
	postInitRuntimeOverrides(appState, serverShutdownCtx, runtimeConfigManager)

	return appState
}

// logger does not parse the regular config object, as logging needs to be
// configured before the configuration is even loaded/parsed. We are thus
// "manually" reading the desired env vars and set reasonable defaults if they
// are not set.
//
// Defaults to log level info and json format
func logger() *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(NewWeaviateTextFormatter())

	if os.Getenv("LOG_FORMAT") != "text" {
		logger.SetFormatter(NewWeaviateJSONFormatter())
	}
	logLevelStr := os.Getenv("LOG_LEVEL")
	level, err := logLevelFromString(logLevelStr)
	if errors.Is(err, errlogLevelNotRecognized) {
		logger.WithField("log_level_env", logLevelStr).Warn("log level not recognized, defaulting to info")
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)
	return logger
}

// everything hard-coded right now, to be made dynamic (from go plugins later)
func registerModules(appState *state.State) error {
	appState.Logger.
		WithField("action", "startup").
		Debug("start registering modules")

	appState.Modules = modules.NewProvider(appState.Logger, appState.ServerConfig.Config)

	// Default modules
	defaultVectorizers := []string{
		modtext2vecaws.Name,
		modmulti2veccohere.Name,
		modcohere.Name,
		moddatabricks.Name,
		modtext2vecgoogle.Name,
		modmulti2vecgoogle.Name,
		modhuggingface.Name,
		modjinaai.Name,
		modmulti2vecjinaai.Name,
		modmistral.Name,
		modtext2vecoctoai.Name,
		modopenai.Name,
		modgenerativedeepseek.Name,
		moddigitalocean.Name,
		modmorph.Name,
		modvoyageai.Name,
		modmulti2vecvoyageai.Name,
		modweaviateembed.Name,
		modtext2multivecjinaai.Name,
		modnvidia.Name,
		modmulti2vecnvidia.Name,
		modmulti2multivecjinaai.Name,
		modmulti2multivecweaviate.Name,
		modmulti2vecaws.Name,
	}
	defaultGenerative := []string{
		modgenerativeanthropic.Name,
		modgenerativeanyscale.Name,
		modgenerativeaws.Name,
		modgenerativecohere.Name,
		modgenerativecontextualai.Name,
		modgenerativedatabricks.Name,
		modgenerativefriendliai.Name,
		modgenerativegoogle.Name,
		modgenerativemistral.Name,
		modgenerativenvidia.Name,
		modgenerativeoctoai.Name,
		modgenerativeopenai.Name,
		modgenerativexai.Name,
	}
	defaultOthers := []string{
		modrerankercohere.Name,
		modrerankercontextualai.Name,
		modrerankervoyageai.Name,
		modrerankerjinaai.Name,
		modrerankernvidia.Name,
	}

	defaultModules := append(defaultVectorizers, defaultGenerative...)
	defaultModules = append(defaultModules, defaultOthers...)

	var modules []string

	if len(appState.ServerConfig.Config.EnableModules) > 0 {
		modules = strings.Split(appState.ServerConfig.Config.EnableModules, ",")
	}

	if appState.ServerConfig.Config.EnableApiBasedModules {
		// Concatenate modules with default modules
		modules = append(modules, defaultModules...)
	}

	enabledModules := map[string]bool{}
	for _, module := range modules {
		enabledModules[strings.TrimSpace(module)] = true
	}

	if _, ok := enabledModules[modt2vbigram.Name]; ok {
		appState.Modules.Register(modt2vbigram.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modt2vbigram.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modcontextionary.Name]; ok {
		appState.Modules.Register(modcontextionary.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modcontextionary.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modt2vmodel2vec.Name]; ok {
		appState.Modules.Register(modt2vmodel2vec.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modt2vmodel2vec.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modtransformers.Name]; ok {
		appState.Modules.Register(modtransformers.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtransformers.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgpt4all.Name]; ok {
		appState.Modules.Register(modgpt4all.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgpt4all.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankervoyageai.Name]; ok {
		appState.Modules.Register(modrerankervoyageai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankervoyageai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankertransformers.Name]; ok {
		appState.Modules.Register(modrerankertransformers.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankertransformers.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankercohere.Name]; ok {
		appState.Modules.Register(modrerankercohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankercohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankercontextualai.Name]; ok {
		appState.Modules.Register(modrerankercontextualai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankercontextualai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankerdummy.Name]; ok {
		appState.Modules.Register(modrerankerdummy.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankerdummy.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankerjinaai.Name]; ok {
		appState.Modules.Register(modrerankerjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankerjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankernvidia.Name]; ok {
		appState.Modules.Register(modrerankernvidia.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankernvidia.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modqna.Name]; ok {
		appState.Modules.Register(modqna.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modqna.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modsum.Name]; ok {
		appState.Modules.Register(modsum.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modsum.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modimage.Name]; ok {
		appState.Modules.Register(modimage.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modimage.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modner.Name]; ok {
		appState.Modules.Register(modner.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modner.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modspellcheck.Name]; ok {
		appState.Modules.Register(modspellcheck.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modspellcheck.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modclip.Name]; ok {
		appState.Modules.Register(modclip.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modclip.Name).
			Debug("enabled module")
	}

	_, enabledMulti2VecGoogle := enabledModules[modmulti2vecgoogle.Name]
	_, enabledMulti2VecPaLM := enabledModules[modmulti2vecgoogle.LegacyName]
	if enabledMulti2VecGoogle || enabledMulti2VecPaLM {
		appState.Modules.Register(modmulti2vecgoogle.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecgoogle.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2veccohere.Name]; ok {
		appState.Modules.Register(modmulti2veccohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2veccohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2vecjinaai.Name]; ok {
		appState.Modules.Register(modmulti2vecjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2multivecjinaai.Name]; ok {
		appState.Modules.Register(modmulti2multivecjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2multivecjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2multivecweaviate.Name]; ok {
		appState.Modules.Register(modmulti2multivecweaviate.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2multivecweaviate.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2vecnvidia.Name]; ok {
		appState.Modules.Register(modmulti2vecnvidia.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecnvidia.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modnvidia.Name]; ok {
		appState.Modules.Register(modnvidia.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modnvidia.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2vecvoyageai.Name]; ok {
		appState.Modules.Register(modmulti2vecvoyageai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecvoyageai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modopenai.Name]; ok {
		appState.Modules.Register(modopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modopenai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[moddigitalocean.Name]; ok {
		appState.Modules.Register(moddigitalocean.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", moddigitalocean.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmorph.Name]; ok {
		appState.Modules.Register(modmorph.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmorph.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[moddatabricks.Name]; ok {
		appState.Modules.Register(moddatabricks.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", moddatabricks.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modqnaopenai.Name]; ok {
		appState.Modules.Register(modqnaopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modqnaopenai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativecohere.Name]; ok {
		appState.Modules.Register(modgenerativecohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativecohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativecontextualai.Name]; ok {
		appState.Modules.Register(modgenerativecontextualai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativecontextualai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativefriendliai.Name]; ok {
		appState.Modules.Register(modgenerativefriendliai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativefriendliai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativenvidia.Name]; ok {
		appState.Modules.Register(modgenerativenvidia.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativenvidia.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativemistral.Name]; ok {
		appState.Modules.Register(modgenerativemistral.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativemistral.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeopenai.Name]; ok {
		appState.Modules.Register(modgenerativeopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeopenai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativedeepseek.Name]; ok {
		appState.Modules.Register(modgenerativedeepseek.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativedeepseek.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativexai.Name]; ok {
		appState.Modules.Register(modgenerativexai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativexai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativedatabricks.Name]; ok {
		appState.Modules.Register(modgenerativedatabricks.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativedatabricks.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeollama.Name]; ok {
		appState.Modules.Register(modgenerativeollama.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeollama.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativedummy.Name]; ok {
		appState.Modules.Register(modgenerativedummy.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativedummy.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeaws.Name]; ok {
		appState.Modules.Register(modgenerativeaws.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeaws.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modhuggingface.Name]; ok {
		appState.Modules.Register(modhuggingface.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modhuggingface.Name).
			Debug("enabled module")
	}

	_, enabledGenerativeGoogle := enabledModules[modgenerativegoogle.Name]
	_, enabledGenerativePaLM := enabledModules[modgenerativegoogle.LegacyName]
	if enabledGenerativeGoogle || enabledGenerativePaLM {
		appState.Modules.Register(modgenerativegoogle.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativegoogle.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeanyscale.Name]; ok {
		appState.Modules.Register(modgenerativeanyscale.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeanyscale.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeanthropic.Name]; ok {
		appState.Modules.Register(modgenerativeanthropic.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeanthropic.Name).
			Debug("enabled module")
	}

	_, enabledText2vecGoogle := enabledModules[modtext2vecgoogle.Name]
	_, enabledText2vecPaLM := enabledModules[modtext2vecgoogle.LegacyName]
	if enabledText2vecGoogle || enabledText2vecPaLM {
		appState.Modules.Register(modtext2vecgoogle.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecgoogle.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modtext2vecaws.Name]; ok {
		appState.Modules.Register(modtext2vecaws.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecaws.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2vecaws.Name]; ok {
		appState.Modules.Register(modmulti2vecaws.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecaws.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstgfs.Name]; ok {
		appState.Modules.Register(modstgfs.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstgfs.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstgs3.Name]; ok {
		appState.Modules.Register(modstgs3.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstgs3.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modsloads3.Name]; ok {
		appState.Modules.Register(modsloads3.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modsloads3.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstggcs.Name]; ok {
		appState.Modules.Register(modstggcs.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstggcs.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstgazure.Name]; ok {
		appState.Modules.Register(modstgazure.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstgazure.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modcentroid.Name]; ok {
		appState.Modules.Register(modcentroid.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modcentroid.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modcohere.Name]; ok {
		appState.Modules.Register(modcohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modcohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modvoyageai.Name]; ok {
		appState.Modules.Register(modvoyageai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modvoyageai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmistral.Name]; ok {
		appState.Modules.Register(modmistral.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmistral.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modbind.Name]; ok {
		appState.Modules.Register(modbind.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modbind.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modjinaai.Name]; ok {
		appState.Modules.Register(modjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modollama.Name]; ok {
		appState.Modules.Register(modollama.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modollama.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modweaviateembed.Name]; ok {
		appState.Modules.Register(modweaviateembed.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modweaviateembed.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeoctoai.Name]; ok {
		appState.Modules.Register(modgenerativeoctoai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeoctoai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modtext2vecoctoai.Name]; ok {
		appState.Modules.Register(modtext2vecoctoai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecoctoai.Name).
			Debug("enabled module")
	}

	_, enabledText2MultivecJinaAI := enabledModules[modtext2multivecjinaai.Name]
	_, enabledText2ColBERTJinaAI := enabledModules[modtext2multivecjinaai.LegacyName]
	if enabledText2MultivecJinaAI || enabledText2ColBERTJinaAI {
		appState.Modules.Register(modtext2multivecjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2multivecjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modusagegcs.Name]; ok {
		appState.Modules.Register(modusagegcs.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modusagegcs.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modusages3.Name]; ok {
		appState.Modules.Register(modusages3.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modusages3.Name).
			Debug("enabled module")
	}

	appState.Logger.
		WithField("action", "startup").
		Debug("completed registering modules")

	return nil
}

func postInitModules(appState *state.State) {
	// Initialize usage service after all components are ready
	if appState.Modules.UsageEnabled() {
		appState.Logger.WithField("action", "startup").Debug("initializing usage service")

		// Initialize usage service for GCS
		if usageGCSModule := appState.Modules.GetByName(modusagegcs.Name); usageGCSModule != nil {
			if usageModuleWithService, ok := usageGCSModule.(modulecapabilities.ModuleWithUsageService); ok {
				usageService := usage.NewService(appState.SchemaManager, appState.DB, appState.Modules, usageModuleWithService.Logger())
				usageModuleWithService.SetUsageService(usageService)
			}
		}
		// Initialize usage service for S3
		if usageS3Module := appState.Modules.GetByName(modusages3.Name); usageS3Module != nil {
			if usageModuleWithService, ok := usageS3Module.(modulecapabilities.ModuleWithUsageService); ok {
				usageService := usage.NewService(appState.SchemaManager, appState.DB, appState.Modules, usageModuleWithService.Logger())
				usageModuleWithService.SetUsageService(usageService)
			}
		}
	}
}

func initModules(ctx context.Context, appState *state.State) error {
	storageProvider, err := modulestorage.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		return errors.Wrap(err, "init storage provider")
	}

	// TODO: gh-1481 don't pass entire appState in, but only what's needed. Probably only
	// config?
	moduleParams := moduletools.NewInitParams(storageProvider, appState,
		&appState.ServerConfig.Config, appState.Logger, prometheus.DefaultRegisterer)

	appState.Logger.
		WithField("action", "startup").
		Debug("start initializing modules")
	if err := appState.Modules.Init(ctx, moduleParams, appState.Logger); err != nil {
		return errors.Wrap(err, "init modules")
	}

	appState.Logger.
		WithField("action", "startup").
		Debug("finished initializing modules")

	return nil
}

type clientWithAuth struct {
	r         http.RoundTripper
	basicAuth cluster.BasicAuth
}

func (c clientWithAuth) RoundTrip(r *http.Request) (*http.Response, error) {
	r.SetBasicAuth(c.basicAuth.Username, c.basicAuth.Password)
	return c.r.RoundTrip(r)
}

func reasonableHttpClient(authConfig cluster.AuthConfig, minimumInternalTimeout time.Duration) *http.Client {
	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   minimumInternalTimeout,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   100,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// Wrap with OpenTelemetry tracing (only has an effect if tracing is enabled)
	transport := monitoring.NewTracingTransport(t)

	if authConfig.BasicAuth.Enabled() {
		return &http.Client{Transport: clientWithAuth{r: transport, basicAuth: authConfig.BasicAuth}}
	}
	return &http.Client{Transport: transport}
}

func setupGoProfiling(appState *state.State) {
	config := appState.ServerConfig.Config
	logger := appState.Logger
	port := config.Profiling.Port
	if port == 0 {
		port = 6060
	}
	// GO_PROFILING_DISABLE is the only switch that prevents binding;
	// DEBUG_ENDPOINTS_ENABLED is enforced per-request by the gate below.
	if config.Profiling.Disabled {
		logger.Infof("debug HTTP listener (port %d) disabled by GO_PROFILING_DISABLE; unset to enable", port)
		return
	}

	functionsToIgnoreInProfiling := []string{
		"raft",
		"http2",
		"memberlist",
		"selectgo", // various tickers
		"cluster",
		"rest",
		"signal_recv",
		"backgroundRead",
		"SetupGoProfiling",
		"serve",
		"Serve",
		"batchWorker",
	}
	http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler(functionsToIgnoreInProfiling...))

	enabled := config.Profiling.DebugEndpointsEnabled
	gateOpen := enabled != nil && enabled.Get()
	if gateOpen {
		logger.Infof("debug HTTP listener bound on :%d (DebugEndpointsEnabled=true; requests are served)", port)
	} else {
		logger.Infof("debug HTTP listener bound on :%d (DebugEndpointsEnabled=false; requests return 404 until enabled)", port)
	}
	debugHandler := makeDebugEndpointsGate(enabled)(http.DefaultServeMux)
	enterrors.GoWrapper(func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), debugHandler); err != nil {
			logger.WithField("action", "debug_listener").Error(err)
		}
	}, logger)
}

// setupRuntimeProfiling sets the Go block/mutex profile rates. Independent of
// the debug HTTP listener and DEBUG_ENDPOINTS_ENABLED (profiles can still be
// collected via runtime/pprof dumps), but GO_PROFILING_DISABLE switches them
// off too.
func setupRuntimeProfiling(appState *state.State) {
	config := appState.ServerConfig.Config
	if config.Profiling.Disabled {
		return
	}
	if config.Profiling.BlockProfileRate > 0 {
		goruntime.SetBlockProfileRate(config.Profiling.BlockProfileRate)
	}
	if config.Profiling.MutexProfileFraction > 0 {
		goruntime.SetMutexProfileFraction(config.Profiling.MutexProfileFraction)
	}
}

func ParseVersionFromSwaggerSpec() string {
	spec := struct {
		Info struct {
			Version string `json:"version"`
		} `json:"info"`
	}{}

	err := json.Unmarshal(SwaggerJSON, &spec)
	if err != nil {
		panic(err)
	}

	return spec.Info.Version
}

func limitResources(appState *state.State) {
	if os.Getenv("LIMIT_RESOURCES") == "true" {
		appState.Logger.Info("Limiting resources:  memory: 80%, cores: all but one")
		if os.Getenv("GOMAXPROCS") == "" {
			// Fetch the number of cores from the cgroups cpuset
			// and parse it into an int
			cores, err := getCores()
			if err == nil {
				appState.Logger.WithField("cores", cores).
					Warn("GOMAXPROCS not set, and unable to read from cgroups, setting to number of cores")
				goruntime.GOMAXPROCS(cores)
			} else {
				cores = goruntime.NumCPU() - 1
				if cores > 0 {
					appState.Logger.WithField("cores", cores).
						Warnf("Unable to read from cgroups: %v, setting to max cores to: %v", err, cores)
					goruntime.GOMAXPROCS(cores)
				}
			}
		}

		limit, err := memlimit.SetGoMemLimit(0.8)
		if err != nil {
			appState.Logger.WithError(err).Warnf("Unable to set memory limit from cgroups: %v", err)
			// Set memory limit to 90% of the available memory
			limit := int64(float64(memory.TotalMemory()) * 0.8)
			debug.SetMemoryLimit(limit)
			appState.Logger.WithField("limit", limit).Info("Set memory limit based on available memory")
		} else {
			appState.Logger.WithField("limit", limit).Info("Set memory limit")
		}
	} else {
		appState.Logger.Info("No resource limits set, weaviate will use all available memory and CPU. " +
			"To limit resources, set LIMIT_RESOURCES=true")
	}
}

func telemetryEnabled(state *state.State) bool {
	return !state.ServerConfig.Config.DisableTelemetry
}

// getTelemetryURL returns the telemetry consumer URL from config.
// If a custom URL is set, it's base64-encoded to match the expected format.
// Returns empty string if no custom URL is set (telemetry.New will use default).
func getTelemetryURL(state *state.State) string {
	url := state.ServerConfig.Config.TelemetryURL
	if url == "" {
		return ""
	}
	// The telemetry package expects base64-encoded URLs
	return base64.StdEncoding.EncodeToString([]byte(url))
}

type membership struct {
	*cluster.State
	raft *rCluster.Service
}

func (m membership) LeaderID() string {
	_, id := m.raft.LeaderWithID()
	return id
}

// initRuntimeOverrides assumes, Configs from envs are loaded before
// initializing runtime overrides.
func initRuntimeOverrides(appState *state.State) *configRuntime.ConfigManager[config.WeaviateRuntimeConfig] {
	// Enable runtime config manager
	if appState.ServerConfig.Config.RuntimeOverrides.Enabled {
		// Runtimeconfig manager takes of keeping the `registered` config values upto date
		registered := &config.WeaviateRuntimeConfig{}
		registered.MaximumAllowedCollectionsCount = appState.ServerConfig.Config.SchemaHandlerConfig.MaximumAllowedCollectionsCount
		registered.MaximumAllowedObjectsCount = appState.ServerConfig.Config.UsageLimits.MaxObjectsCount
		registered.MaximumAllowedTenantsPerCollection = appState.ServerConfig.Config.UsageLimits.MaxTenantsPerCollection
		registered.MaximumAllowedShardsPerCollection = appState.ServerConfig.Config.UsageLimits.MaxShardsPerCollection
		registered.UsageLimitsErrorMessage = appState.ServerConfig.Config.UsageLimits.ErrorMessage
		registered.AsyncReplicationDisabled = appState.ServerConfig.Config.Replication.AsyncReplicationDisabled
		registered.AsyncReplicationSchedulerWorkers = appState.ServerConfig.Config.Replication.AsyncReplicationSchedulerWorkers
		registered.AsyncReplicationHashtreeInitConcurrency = appState.ServerConfig.Config.Replication.AsyncReplicationHashtreeInitConcurrency
		registered.AsyncReplicationHashtreeHeight = appState.ServerConfig.Config.Replication.AsyncReplicationHashtreeHeight
		registered.AsyncReplicationFrequency = appState.ServerConfig.Config.Replication.AsyncReplicationFrequency
		registered.AsyncReplicationFrequencyWhilePropagating = appState.ServerConfig.Config.Replication.AsyncReplicationFrequencyWhilePropagating
		registered.AsyncReplicationLoggingFrequency = appState.ServerConfig.Config.Replication.AsyncReplicationLoggingFrequency
		registered.AsyncReplicationDiffBatchSize = appState.ServerConfig.Config.Replication.AsyncReplicationDiffBatchSize
		registered.AsyncReplicationDiffPerNodeTimeout = appState.ServerConfig.Config.Replication.AsyncReplicationDiffPerNodeTimeout
		registered.AsyncReplicationPrePropagationTimeout = appState.ServerConfig.Config.Replication.AsyncReplicationPrePropagationTimeout
		registered.AsyncReplicationPropagationTimeout = appState.ServerConfig.Config.Replication.AsyncReplicationPropagationTimeout
		registered.AsyncReplicationPropagationLimit = appState.ServerConfig.Config.Replication.AsyncReplicationPropagationLimit
		registered.AsyncReplicationPropagationConcurrency = appState.ServerConfig.Config.Replication.AsyncReplicationPropagationConcurrency
		registered.AsyncReplicationPropagationBatchSize = appState.ServerConfig.Config.Replication.AsyncReplicationPropagationBatchSize
		registered.AsyncReplicationPropagationDelay = appState.ServerConfig.Config.Replication.AsyncReplicationPropagationDelay
		registered.ReplicationGRPCEnabled = appState.ServerConfig.Config.Replication.ReplicationGRPCEnabled
		registered.AutoschemaEnabled = appState.ServerConfig.Config.AutoSchema.Enabled
		registered.TenantActivityReadLogLevel = appState.ServerConfig.Config.TenantActivityReadLogLevel
		registered.TenantActivityWriteLogLevel = appState.ServerConfig.Config.TenantActivityWriteLogLevel
		registered.RevectorizeCheckDisabled = appState.ServerConfig.Config.RevectorizeCheckDisabled
		registered.QuerySlowLogEnabled = appState.ServerConfig.Config.QuerySlowLogEnabled
		registered.QuerySlowLogThreshold = appState.ServerConfig.Config.QuerySlowLogThreshold
		registered.InvertedSorterDisabled = appState.ServerConfig.Config.InvertedSorterDisabled
		registered.LazyPropertyLengthsEnabled = appState.ServerConfig.Config.LazyPropertyLengthsEnabled
		registered.DefaultQuantization = appState.ServerConfig.Config.DefaultQuantization
		registered.DefaultVectorIndexType = appState.ServerConfig.Config.DefaultVectorIndexType
		registered.DefaultShardingCount = appState.ServerConfig.Config.DefaultShardingCount
		registered.AllowedVectorIndexTypes = appState.ServerConfig.Config.Restrictions.AllowedVectorIndexTypes
		registered.AllowedCompressionTypes = appState.ServerConfig.Config.Restrictions.AllowedCompressionTypes
		registered.RestrictionsErrorMessage = appState.ServerConfig.Config.Restrictions.ErrorMessage
		registered.RaftDrainSleep = appState.ServerConfig.Config.Raft.DrainSleep
		registered.RaftTimoutsMultiplier = appState.ServerConfig.Config.Raft.TimeoutsMultiplier
		registered.OperationalMode = appState.ServerConfig.Config.OperationalMode
		registered.NamespaceCleanupInterval = appState.ServerConfig.Config.Namespaces.CleanupInterval
		registered.ObjectsTTLDeleteSchedule = appState.ServerConfig.Config.ObjectsTTLDeleteSchedule
		registered.ObjectsTTLBatchSize = appState.ServerConfig.Config.ObjectsTTLBatchSize
		registered.ObjectsTTLPauseEveryNoBatches = appState.ServerConfig.Config.ObjectsTTLPauseEveryNoBatches
		registered.ObjectsTTLPauseDuration = appState.ServerConfig.Config.ObjectsTTLPauseDuration
		registered.ObjectsTTLConcurrencyFactor = appState.ServerConfig.Config.ObjectsTTLConcurrencyFactor
		registered.ExportEnabled = appState.ServerConfig.Config.Export.Enabled
		registered.ExportDefaultBucket = appState.ServerConfig.Config.Export.DefaultBucket
		registered.ExportDefaultPath = appState.ServerConfig.Config.Export.DefaultPath
		registered.ExportParallelism = appState.ServerConfig.Config.ExportParallelism
		registered.MCPEnabled = appState.ServerConfig.Config.MCP.Enabled
		registered.MCPWriteAccessEnabled = appState.ServerConfig.Config.MCP.WriteAccessEnabled
		registered.DebugEndpointsEnabled = appState.ServerConfig.Config.Profiling.DebugEndpointsEnabled

		if appState.ServerConfig.Config.Authentication.OIDC.Enabled {
			registered.OIDCIssuer = appState.ServerConfig.Config.Authentication.OIDC.Issuer
			registered.OIDCClientID = appState.ServerConfig.Config.Authentication.OIDC.ClientID
			registered.OIDCSkipClientIDCheck = appState.ServerConfig.Config.Authentication.OIDC.SkipClientIDCheck
			registered.OIDCUsernameClaim = appState.ServerConfig.Config.Authentication.OIDC.UsernameClaim
			registered.OIDCGroupsClaim = appState.ServerConfig.Config.Authentication.OIDC.GroupsClaim
			registered.OIDCScopes = appState.ServerConfig.Config.Authentication.OIDC.Scopes
			registered.OIDCCertificate = appState.ServerConfig.Config.Authentication.OIDC.Certificate
			registered.OIDCJWKSUrl = appState.ServerConfig.Config.Authentication.OIDC.JWKSUrl
			registered.OIDCSkipTLSVerify = appState.ServerConfig.Config.Authentication.OIDC.SkipTLSVerify
		}

		cm, err := configRuntime.NewConfigManager(
			appState.ServerConfig.Config.RuntimeOverrides.Path,
			config.NewRuntimeConfigParser(appState.Logger),
			config.UpdateRuntimeConfig,
			registered,
			appState.ServerConfig.Config.RuntimeOverrides.LoadInterval,
			appState.Logger,
			prometheus.DefaultRegisterer)
		if err != nil {
			appState.Logger.WithField("action", "runtime_overrides_parse").Errorf("could not create runtime config manager: %v", err)
		}
		return cm
	}
	return nil
}

// postInitRuntimeOverrides registers hooks and starts runtime config background process
func postInitRuntimeOverrides(appState *state.State, serverShutdownCtx context.Context, cm *configRuntime.ConfigManager[config.WeaviateRuntimeConfig]) {
	if appState.ServerConfig.Config.RuntimeOverrides.Enabled && cm != nil {
		// register any additional runtime configs
		if appState.Modules.UsageEnabled() {
			cm.RegisterAdditional(func(registered *config.WeaviateRuntimeConfig) {
				// gcs config
				registered.UsageGCSBucket = appState.ServerConfig.Config.Usage.GCSBucket
				registered.UsageGCSPrefix = appState.ServerConfig.Config.Usage.GCSPrefix
				// s3 config
				registered.UsageS3Bucket = appState.ServerConfig.Config.Usage.S3Bucket
				registered.UsageS3Prefix = appState.ServerConfig.Config.Usage.S3Prefix
				// common config
				registered.UsageScrapeInterval = appState.ServerConfig.Config.Usage.ScrapeInterval
				registered.UsageShardJitterInterval = appState.ServerConfig.Config.Usage.ShardJitterInterval
				registered.UsagePolicyVersion = appState.ServerConfig.Config.Usage.PolicyVersion
				registered.UsageVerifyPermissions = appState.ServerConfig.Config.Usage.VerifyPermissions
			})
		}
		// register hooks
		hooks := make(map[string]func() error)
		if appState.ServerConfig.Config.Authentication.OIDC.Enabled {
			hooks["OIDC"] = appState.OIDC.Init
		}
		// Reconcile loaded shards when the async-replication kill-switch is
		// toggled at runtime. Run in the background: ReconcileAsyncReplication
		// does per-shard hashtree disk I/O, which must not block the runtime-
		// config reload loop. serverShutdownCtx makes it cancellable on shutdown;
		// errors are logged here and surfaced via the reconcileFailures metric.
		hooks["AsyncReplicationDisabled"] = func() error {
			restcompat.SetAsyncReplicationGloballyDisabled(appState.ServerConfig.Config.Replication.AsyncReplicationDisabled.Get())
			enterrors.GoWrapper(func() {
				if err := appState.DB.ReconcileAsyncReplication(serverShutdownCtx); err != nil {
					appState.Logger.WithField("action", "reconcile_async_replication").Error(err)
				}
			}, appState.Logger)
			return nil
		}
		maps.Copy(hooks, appState.Crons.RuntimeConfigHooks())

		// Re-run cross-field restriction validation on runtime YAML pushes
		// (per-value runs at SetValue time). Keys are exact struct-field
		// names — matchUpdatedFields uses HasPrefix, so "Default" would
		// also match DefaultShardingCount and friends.
		restrictionHook := func() error {
			return appState.ServerConfig.Config.ValidateRestrictionsRuntime(appState.Logger)
		}
		hooks["AllowedVectorIndexTypes"] = restrictionHook
		hooks["AllowedCompressionTypes"] = restrictionHook
		hooks["DefaultVectorIndexType"] = restrictionHook
		hooks["DefaultQuantization"] = restrictionHook

		appState.Logger.Log(logrus.InfoLevel, "registering runtime overrides hooks")
		cm.RegisterHooks(hooks)
		// reload current overrides file to take into account additional settings
		if err := cm.ReloadConfig(); err != nil {
			appState.Logger.WithField("action", "startup").Errorf("could not reload config: %v", err)
		}
		// start runtime config background check
		enterrors.GoWrapper(func() {
			// NOTE: Not using parent `ctx` because that is getting cancelled in the caller even during startup.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if err := cm.Run(ctx); err != nil {
				appState.Logger.WithField("action", "runtime config manager startup").Fatalf("runtime config manager stopped: %v", err)
			}
		}, appState.Logger)
	}
}
