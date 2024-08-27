package main

import (
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	clusterweaviate "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/exp/query"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/auth/authentication/oidc"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/scaler"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/traverser"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	schemarepo "github.com/weaviate/weaviate/adapters/repos/schema"
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
		grpcQuerier, err := NewQuerier(log)
		if err != nil {
			log.Fatalf("failed to create querier: %w", err)
		}
		listener, err := net.Listen("tcp", opts.GRPCListenAddr)
		if err != nil {
			log.Fatalf("failed to bind grpc server port: %w", err)
		}

		grpcServer := grpc.NewServer()
		reflection.Register(grpcServer)
		protocol.RegisterWeaviateServer(grpcServer, grpcQuerier)

		log.WithField("port", opts.GRPCListenAddr).Info("starting querier grpc")
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("failed to start grpc server: %w", err)
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

func newSchemaManager(
	cfg *config.Config,
	authorizer authorization.Authorizer,
	modulesProvider *modules.Provider,
	clusterState *cluster.State,
	repo *db.DB,
	log logrus.FieldLogger) *schema.Manager {

	migrator := db.NewMigrator(repo, log) // still need db.DB
	scalar := scaler.New(clusterState, nil, nil, log, "/tmp")
	schemaRepo := schemarepo.NewStore(config.DefaultPersistenceDataPath, log)

	nodeName := clusterState.LocalName()
	addr, _ := clusterState.NodeHostname(nodeName)
	addrs := strings.Split(addr, ":")

	clusterConfig := clusterweaviate.Config{
		WorkDir:                filepath.Join(config.DefaultPersistenceDataPath, config.DefaultRaftDir),
		NodeID:                 clusterState.LocalName(),
		Host:                   addrs[0],
		RaftPort:               cfg.Raft.Port,
		RPCPort:                cfg.Raft.InternalRPCPort,
		RaftRPCMessageMaxSize:  cfg.Raft.RPCMessageMaxSize,
		BootstrapTimeout:       cfg.Raft.BootstrapTimeout,
		BootstrapExpect:        cfg.Raft.BootstrapExpect,
		HeartbeatTimeout:       cfg.Raft.HeartbeatTimeout,
		ElectionTimeout:        cfg.Raft.ElectionTimeout,
		SnapshotInterval:       cfg.Raft.SnapshotInterval,
		SnapshotThreshold:      cfg.Raft.SnapshotThreshold,
		ConsistencyWaitTimeout: cfg.Raft.ConsistencyWaitTimeout,
		MetadataOnlyVoters:     cfg.Raft.MetadataOnlyVoters,
		ForceOneNodeRecovery:   cfg.Raft.ForceOneNodeRecovery,
		DB:                     nil,
		Parser:                 schema.NewParser(clusterState, vectorindex.ParseAndValidateConfig, migrator),
		NodeNameToPortMap:      nil, // NOTE: Come back later
		NodeToAddressResolver:  clusterState,
		Logger:                 logrus.New(), // not reusing upstream logger
		IsLocalHost:            cfg.Cluster.Localhost,
	}

	clusterSvc := clusterweaviate.New(clusterState, clusterConfig)

	schemaManager, err := schema.NewManager(
		migrator,
		clusterSvc.Raft,
		clusterSvc.SchemaReader(),
		schemaRepo,
		log,
		authorizer,
		*cfg,
		vectorindex.ParseAndValidateConfig,
		modulesProvider,
		inverted.ValidateConfig,
		modulesProvider,
		clusterState,
		scalar,
		nil, // offload-s3 modulesProvider
	)
	if err != nil {
		panic(err)
	}
	return schemaManager
}

func newRepo(weaviateCfg *config.Config, clusterState *cluster.State, log logrus.FieldLogger) *db.DB {

	clusterHttpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 120 * time.Second,
			}).DialContext,
			MaxIdleConnsPerHost:   100,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	memWatch := memwatch.NewMonitor(memwatch.LiveHeapReader, debug.SetMemoryLimit, 0.97)
	remoteIndexClient := clients.NewRemoteIndex(clusterHttpClient)
	remoteNodesClient := clients.NewRemoteNode(clusterHttpClient)
	replicationClient := clients.NewReplicationClient(clusterHttpClient)

	repo, err := db.New(
		log,
		db.Config{
			ServerVersion:             config.ServerVersion,
			GitHash:                   config.GitHash,
			MemtablesFlushDirtyAfter:  weaviateCfg.Persistence.MemtablesFlushDirtyAfter,
			MemtablesInitialSizeMB:    10,
			MemtablesMaxSizeMB:        weaviateCfg.Persistence.MemtablesMaxSizeMB,
			MemtablesMinActiveSeconds: weaviateCfg.Persistence.MemtablesMinActiveDurationSeconds,
			MemtablesMaxActiveSeconds: weaviateCfg.Persistence.MemtablesMaxActiveDurationSeconds,
			MaxSegmentSize:            weaviateCfg.Persistence.LSMMaxSegmentSize,
			HNSWMaxLogSize:            weaviateCfg.Persistence.HNSWMaxLogSize,
			HNSWWaitForCachePrefill:   weaviateCfg.HNSWStartupWaitForVectorCache,
			RootPath:                  weaviateCfg.Persistence.DataPath,
			QueryLimit:                weaviateCfg.QueryDefaults.Limit,
			QueryMaximumResults:       weaviateCfg.QueryMaximumResults,
			QueryNestedRefLimit:       weaviateCfg.QueryNestedCrossReferenceLimit,
			MaxImportGoroutinesFactor: weaviateCfg.MaxImportGoroutinesFactor,
			TrackVectorDimensions:     weaviateCfg.TrackVectorDimensions,
			ResourceUsage:             weaviateCfg.ResourceUsage,
			AvoidMMap:                 weaviateCfg.AvoidMmap,
			DisableLazyLoadShards:     weaviateCfg.DisableLazyLoadShards,
			ForceFullReplicasSearch:   weaviateCfg.ForceFullReplicasSearch,
			// Pass dummy replication config with minimum factor 1. Otherwise the
			// setting is not backward-compatible. The user may have created a class
			// with factor=1 before the change was introduced. Now their setup would no
			// longer start up if the required minimum is now higher than 1. We want
			// the required minimum to only apply to newly created classes - not block
			// loading existing ones.
			Replication: replication.GlobalConfig{MinimumFactor: 1},
		},
		remoteIndexClient,
		clusterState,
		remoteNodesClient,
		replicationClient,
		nil, // metrics
		memWatch,
	)
	if err != nil {
		log.WithError(err).Fatal("failed to init local DB")
	}
	return repo
}

// NOTE: **Warning** All the dependencies we are creating it for querier is duplicated.
// Currently these are all already created when creating WeaviateServer via `rest.MakeAppState`. But there is no way we could re-use those now specifically for querier.
// We have to do bunch of refactoring before integrating this to original `cmd/weaviate-server` binary.
// TODO(kavi): Integrate with `cmd/weaviate-server` binary
func NewQuerier(log logrus.FieldLogger) (*query.GRPC, error) {
	log = log.WithField("target", "querier")

	cfg := config.Config{
		MaxImportGoroutinesFactor: 1,
		Authentication:            config.Authentication{OIDC: config.OIDC{Enabled: false}, APIKey: config.APIKey{Enabled: false}},
		QueryDefaults:             config.QueryDefaults{Limit: 25},
		QueryMaximumResults:       1000,
		Persistence:               config.Persistence{DataPath: "/serverless/data"},
	}

	var (
		schemaManager *schema.Manager
		repo          *db.DB
	)

	authorizer := authorization.New(cfg)
	modulesProvider := modules.NewProvider(log)
	clusterState, err := cluster.Init(cluster.Config{
		Localhost: true,
	}, "/tmp/", nil, log)
	if err != nil {
		log.WithError(err).Fatal(err)
	}

	repo = newRepo(&cfg, clusterState, log)
	schemaManager = newSchemaManager(&cfg, authorizer, modulesProvider, clusterState, repo, log)

	explorer := traverser.NewExplorer(
		repo,
		log,
		modulesProvider,
		nil, // metrics
		cfg,
	)

	traverser := traverser.NewTraverser(
		&config.WeaviateConfig{Config: cfg},
		&DummyLock{},
		log,
		authorizer,
		repo,
		explorer,
		schemaManager,
		modulesProvider,
		nil, // metrics
		0,   // maxGetRequests
	)

	batchManager := objects.NewBatchManager(
		repo, // db.DB
		modulesProvider,
		&DummyLock{},
		schemaManager,
		&config.WeaviateConfig{Config: cfg},
		log,
		authorizer,
		nil, // metrics

	)
	weaviateOIDC, err := oidc.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create weaviate OIDC", err)
	}

	weaviateApiKey, err := apikey.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create api key: %w", err)
	}

	// core query API
	api := query.NewAPI(
		traverser,
		composer.New(cfg.Authentication, weaviateOIDC, weaviateApiKey),
		authAnonymousEnabled,
		schemaManager,
		batchManager,
		&cfg,
		log,
	)

	// gRPC transport on top of query API
	grpc := query.NewGRPC(api, log)

	return grpc, nil
}

type DummyLock struct{}

func (d *DummyLock) LockConnector() (func() error, error) {
	return func() error { return nil }, nil
}

func (d *DummyLock) LockSchema() (func() error, error) {
	return func() error { return nil }, nil
}
