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

package state

import (
	"context"
	"net/http"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/cron"

	"github.com/weaviate/weaviate/adapters/handlers/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/rest/tenantactivity"
	"github.com/weaviate/weaviate/adapters/handlers/rest/types"
	"github.com/weaviate/weaviate/adapters/repos/classifications"
	"github.com/weaviate/weaviate/adapters/repos/db"
	rCluster "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/fsm"
	"github.com/weaviate/weaviate/cluster/shard"
	grpcconn "github.com/weaviate/weaviate/grpc/conn"
	"github.com/weaviate/weaviate/usecases/auth/authentication/anonymous"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/oidc"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	exportUsecase "github.com/weaviate/weaviate/usecases/export"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/traverser"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// State is the only source of application-wide state
// NOTE: This is not true yet, see gh-723
// TODO: remove dependencies to anything that's not an ent or uc
type State struct {
	OIDC             *oidc.Client
	AnonymousAccess  *anonymous.Client
	APIKey           *apikey.ApiKey
	APIKeyRemote     *apikey.RemoteApiKey
	Authorizer       authorization.Authorizer
	AuthzController  authorization.Controller
	AuthzSnapshotter fsm.Snapshotter
	RBAC             *rbac.Manager
	Crons            *cron.Crons

	ServerConfig        *config.WeaviateConfig
	LDIntegration       *configRuntime.LDIntegration
	Logger              *logrus.Logger
	gqlMutex            sync.Mutex
	GraphQL             graphql.GraphQL
	Modules             *modules.Provider
	SchemaManager       *schema.Manager
	Cluster             *cluster.State
	RemoteIndexIncoming *sharding.RemoteIndexIncoming
	RemoteNodeIncoming  *sharding.RemoteNodeIncoming
	Traverser           *traverser.Traverser

	ClassificationRepo *classifications.DistributedRepo
	Metrics            *monitoring.PrometheusMetrics
	HTTPServerMetrics  *monitoring.HTTPServerMetrics
	GRPCServerMetrics  *monitoring.GRPCServerMetrics
	BackupManager      *backup.Handler
	ExportParticipant  *exportUsecase.Participant
	ExportMetrics      *exportUsecase.ExportMetrics
	DB                 *db.DB
	BatchManager       *objects.BatchManager
	AutoSchemaManager  *objects.AutoSchemaManager
	ClusterHttpClient  *http.Client
	ReindexCtxCancel   context.CancelCauseFunc
	MemWatch           *memwatch.Monitor

	ClusterService       *rCluster.Service
	TenantActivity       *tenantactivity.Handler
	InternalServer       types.ClusterServer
	NamespacesController *usecasesNamespaces.Controller

	ObjectTTLCoordinator *objectttl.Coordinator
	ObjectTTLLocalStatus *objectttl.LocalStatus

	DistributedTaskScheduler *distributedtask.Scheduler
	Migrator                 *db.Migrator

	// ReindexProvider is the local handle for the runtime-reindex
	// distributed-task provider. Exposed here so the REST cancel handler
	// can wait for a cancelled task's local goroutine to drain before
	// triggering the on-disk state cleanup — see
	// [db.ReindexProvider.WaitForLocalTaskDrain].
	ReindexProvider *db.ReindexProvider

	// ReindexSubmitLocks serializes mutating REST operations on the same
	// (collection, property) tuple across BOTH the reindex-submit
	// handler (PUT /v1/schema/{class}/indexes/{prop}) and the
	// destructive property-index handler (DELETE
	// /v1/schema/{class}/properties/{prop}/index/{indexName}).
	//
	// Motivating failure mode (pinned by
	// test/acceptance/reindex_concurrent's
	// change_tokenization_both__delete_searchable_parallel matrix
	// sub-test): two parallel REST requests on the same property race
	// at the RAFT serializer. If DELETE searchable's UPDATE_PROPERTY
	// command commits BEFORE change-tokenization's DISTRIBUTED_TASK_ADD,
	// the apply-time MutationGuard cannot reject DELETE because no
	// task is in-flight yet; the bucket is dropped; the change-tok
	// task is then admitted, runs against a missing canonical bucket,
	// and FAILS — leaving a torn filterable bucket on the shard.
	//
	// The shared lock closes the race at the REST layer: change-tok
	// holds the lock across the AddDistributedTask RAFT commit, so a
	// concurrent DELETE on the same property waits, and then T1 sees
	// the task in-flight and rejects it deterministically. Conversely,
	// if DELETE wins the lock, change-tok's downstream validation
	// (e.g., validateTokenizationChange) sees IndexSearchable=false
	// and rejects with 400.
	//
	// Multi-node caveat: this lock is local. Two simultaneous submits
	// from two different REST nodes against the same property are
	// still possible. The RAFT apply-time MutationGuard remains the
	// authoritative defense; this lock just collapses the local
	// single-node race window that any realistic UI/CLI flow can hit.
	ReindexSubmitLocks *ReindexSubmitLocks

	// UsageLimits gates the object-count cap only. Collections/tenants/
	// shards caps are read directly at the schema-handler use sites.
	UsageLimits *usagelimits.Manager

	// GRPCConnManager is a general connection manager for any/all gRPC connections used by the application. It implements retry logic and connection pooling.
	GRPCConnManager *grpcconn.ConnManager
	ShardRegistry   *shard.Registry
	// ReplGRPCConnManager is a separate connection manager that implements retry logic to each RPC call on top of connection pooling, specifically for replication traffic.
	ReplGRPCConnManager *grpcconn.ConnManager
}

// GetGraphQL is the safe way to retrieve GraphQL from the state as it can be
// replaced at runtime. Instead of passing appState.GraphQL to your adapters,
// pass appState itself which you can abstract with a local interface such as:
//
// type gqlProvider interface { GetGraphQL graphql.GraphQL }
func (s *State) GetGraphQL() graphql.GraphQL {
	s.gqlMutex.Lock()
	gql := s.GraphQL
	s.gqlMutex.Unlock()
	return gql
}

func (s *State) SetGraphQL(gql graphql.GraphQL) {
	s.gqlMutex.Lock()
	s.GraphQL = gql
	s.gqlMutex.Unlock()
}
