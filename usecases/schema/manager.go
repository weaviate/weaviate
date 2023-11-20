//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/scaler"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

// Manager Manages schema changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	migrator     Migrator
	repo         SchemaStore
	logger       logrus.FieldLogger
	Authorizer   authorizer
	config       config.Config
	cluster      *cluster.TxManager
	clusterState clusterState

	sync.RWMutex
	// The handler is responsible for well-defined tasks and should be decoupled from the manager.
	// This enables API requests to be directed straight to the handler without the need to pass through the manager.
	// For more context, refer to the handler's definition.
	Handler

	metaReader
}

type VectorConfigParser func(in interface{}) (schemaConfig.VectorIndexConfig, error)

type InvertedConfigValidator func(in *models.InvertedIndexConfig) error

type SchemaGetter interface {
	GetSchemaSkipAuth() schema.Schema
	Nodes() []string
	NodeName() string
	ClusterHealthScore() int
	ResolveParentNodes(string, string) (map[string]string, error)

	CopyShardingState(class string) *sharding.State
	ShardOwner(class, shard string) (string, error)
	TenantShard(class, tenant string) (string, string)
	ShardFromUUID(class string, uuid []byte) string
	ShardReplicas(class, shard string) ([]string, error)
}

type VectorizerValidator interface {
	ValidateVectorizer(moduleName string) error
}

type ModuleConfig interface {
	SetClassDefaults(class *models.Class)
	SetSinglePropertyDefaults(class *models.Class, prop *models.Property)
	ValidateClass(ctx context.Context, class *models.Class) error
}

// State is a cached copy of the schema that can also be saved into a remote
// storage, as specified by Repo
type State struct {
	ObjectSchema  *models.Schema `json:"object"`
	ShardingState map[string]*sharding.State
}

// NewState returns a new state with room for nClasses classes
func NewState(nClasses int) State {
	return State{
		ObjectSchema: &models.Schema{
			Classes: make([]*models.Class, 0, nClasses),
		},
		ShardingState: make(map[string]*sharding.State, nClasses),
	}
}

// SchemaStore is responsible for persisting the schema
// by providing support for both partial and complete schema updates
type SchemaStore interface {
	// Save saves the complete schema to the persistent storage
	Save(ctx context.Context, schema State) error

	// Load loads the complete schema from the persistent storage
	Load(context.Context) (State, error)

	// NewClass creates a new class if it doesn't exists, otherwise return an error
	NewClass(context.Context, ClassPayload) error

	// UpdateClass if it exists, otherwise return an error
	UpdateClass(context.Context, ClassPayload) error

	// DeleteClass deletes class
	DeleteClass(ctx context.Context, class string) error

	// NewShards creates new shards of an existing class
	NewShards(ctx context.Context, class string, shards []KeyValuePair) error

	// UpdateShards updates (replaces) shards of on existing class
	// Error is returned if class or shard does not exist
	UpdateShards(ctx context.Context, class string, shards []KeyValuePair) error

	// DeleteShards deletes shards from a class
	// If the class or a shard does not exist then nothing is done and a nil error is returned
	DeleteShards(ctx context.Context, class string, shards []string) error
}

// KeyValuePair is used to serialize shards updates
type KeyValuePair struct {
	Key   string
	Value []byte
}

// ClassPayload is used to serialize class updates
type ClassPayload struct {
	Name          string
	Metadata      []byte
	ShardingState []byte
	Shards        []KeyValuePair
	ReplaceShards bool
	Error         error
}

type clusterState interface {
	// Hostnames initializes a broadcast
	Hostnames() []string

	// AllNames initializes shard distribution across nodes
	AllNames() []string
	Candidates() []string
	LocalName() string
	NodeCount() int
	NodeHostname(nodeName string) (string, bool)

	// ClusterHealthScore gets the whole cluster health, the lower number the better
	ClusterHealthScore() int

	SchemaSyncIgnored() bool
	SkipSchemaRepair() bool
}

type scaleOut interface {
	SetSchemaManager(sm scaler.SchemaManager)
	Scale(ctx context.Context, className string,
		updated shardingConfig.Config, prevReplFactor, newReplFactor int64) (*sharding.State, error)
}

// NewManager creates a new manager
func NewManager(migrator Migrator,
	store metaWriter, metaReader metaReader,
	repo SchemaStore,
	logger logrus.FieldLogger, authorizer authorizer, config config.Config,
	configParser VectorConfigParser, vectorizerValidator VectorizerValidator,
	invertedConfigValidator InvertedConfigValidator,
	moduleConfig ModuleConfig, clusterState clusterState,
	txClient cluster.Client, txPersistence cluster.Persistence,
	scaleoutManager scaleOut,
) (*Manager, error) {
	handler, err := NewHandler(
		store, metaReader, migrator,
		logger, authorizer,
		config, hnswConfigParser, vectorizerValidator, invertedConfigValidator,
		moduleConfig, clusterState, scaleoutManager)
	if err != nil {
		return nil, fmt.Errorf("cannot init handler: %w", err)
	}
	m := &Manager{
		config:       config,
		migrator:     migrator,
		repo:         repo,
		logger:       logger,
		clusterState: clusterState,
		Handler:      handler,
		metaReader:   metaReader,
	}

	if err := m.loadOrInitializeSchema(context.Background()); err != nil {
		return nil, fmt.Errorf("could not load or initialize schema: %v", err)
	}

	return m, nil
}

func (m *Manager) Shutdown(ctx context.Context) error {
	// TODO-RAFT START
	// Fix shutdown logic
	//

	// allCommitsDone := make(chan struct{})
	// go func() {
	// 	m.cluster.Shutdown()
	// 	allCommitsDone <- struct{}{}
	// }()

	// select {
	// case <-ctx.Done():
	// 	return fmt.Errorf("waiting for transactions to commit: %w", ctx.Err())
	// case <-allCommitsDone:
	// 	return nil
	// }
	return nil
}

func (m *Manager) TxManager() *cluster.TxManager {
	return m.cluster
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

func (m *Manager) loadOrInitializeSchema(ctx context.Context) error {
	return nil
	// localSchema, err := m.repo.Load(ctx)
	// if err != nil {
	// 	return fmt.Errorf("could not load schema:  %v", err)
	// }
	// if err := m.parseConfigs(ctx, &localSchema); err != nil {
	// 	return errors.Wrap(err, "load schema")
	// }

	// if err := m.migrateSchemaIfNecessary(ctx, &localSchema); err != nil {
	// 	return fmt.Errorf("migrate schema: %w", err)
	// }

	// // There was a bug that allowed adding the same prop multiple times. This
	// // leads to a race at startup. If an instance is already affected by this,
	// // this step can remove the duplicate ones.
	// //
	// // See https://github.com/weaviate/weaviate/issues/2609
	// for _, c := range localSchema.ObjectSchema.Classes {
	// 	c.Properties = m.deduplicateProps(c.Properties, c.Class)
	// }

	// // set internal state since it is used by startupClusterSync
	// m.schemaCache.setState(localSchema)

	// // make sure that all migrations have completed before checking sync,
	// // otherwise two identical schemas might fail the check based on form rather
	// // than content

	// if err := m.startupClusterSync(ctx); err != nil {
	// 	return errors.Wrap(err, "sync schema with other nodes in the cluster")
	// }

	// // store in persistent storage
	// // TODO: investigate if save() is redundant because it is called in startupClusterSync()
	// err = m.RLockGuard(func() error { return m.repo.Save(ctx, m.schemaCache.State) })
	// if err != nil {
	// 	return fmt.Errorf("store to persistent storage: %v", err)
	// }

	// return nil
}

// StartServing indicates that the schema manager is ready to accept incoming
// connections in cluster mode, i.e. it will accept opening transactions.
//
// Some transactions are exempt, such as ReadSchema which is required for nodes
// to start up.
//
// This method should be called when all backends, primarily the DB, are ready
// to serve.
func (m *Manager) StartServing(ctx context.Context) error {
	// only start accepting incoming connections when dangling txs have been
	// resumed, otherwise there is potential for conflict
	// m.cluster.StartAcceptIncoming()

	return nil
}

// func (m *Manager) migrateSchemaIfNecessary(ctx context.Context, localSchema *State) error {
// 	// introduced when Weaviate started supporting multi-shards per class in v1.8
// 	if err := m.checkSingleShardMigration(ctx, localSchema); err != nil {
// 		return errors.Wrap(err, "migrating sharding state from previous version")
// 	}

// 	// introduced when Weaviate started supporting replication in v1.17
// 	if err := m.checkShardingStateForReplication(ctx, localSchema); err != nil {
// 		return errors.Wrap(err, "migrating sharding state from previous version (before replication)")
// 	}

// 	// if other migrations become necessary in the future, you can add them here.
// 	return nil
// }

// func (m *Manager) checkSingleShardMigration(ctx context.Context, localSchema *State) error {
// 	for _, c := range localSchema.ObjectSchema.Classes {
// 		if _, ok := localSchema.ShardingState[c.Class]; ok { // there is sharding state for this class. Nothing to do
// 			continue
// 		}

// 		m.logger.WithField("className", c.Class).WithField("action", "initialize_schema").
// 			Warningf("No sharding state found for class %q, initializing new state. "+
// 				"This is expected behavior if the schema was created with an older Weaviate "+
// 				"version, prior to supporting multi-shard indices.", c.Class)

// 		// there is no sharding state for this class, let's create the correct
// 		// config. This class must have been created prior to the sharding feature,
// 		// so we now that the shardCount==1 - we do not care about any of the other
// 		// parameters and simply use the defaults for those
// 		c.ShardingConfig = map[string]interface{}{
// 			"desiredCount": 1,
// 		}
// 		if err := m.praser.parseShardingConfig(c); err != nil {
// 			return err
// 		}

// 		if err := replica.ValidateConfig(c, m.config.Replication); err != nil {
// 			return fmt.Errorf("validate replication config: %w", err)
// 		}
// 		shardState, err := sharding.InitState(c.Class,
// 			c.ShardingConfig.(sharding.Config),
// 			m.clusterState, c.ReplicationConfig.Factor,
// 			schema.MultiTenancyEnabled(c))
// 		if err != nil {
// 			return errors.Wrap(err, "init sharding state")
// 		}

// 		if localSchema.ShardingState == nil {
// 			localSchema.ShardingState = map[string]*sharding.State{}
// 		}
// 		localSchema.ShardingState[c.Class] = shardState

// 	}

// 	return nil
// }

func (m *Manager) checkShardingStateForReplication(ctx context.Context, localSchema *State) error {
	for _, classState := range localSchema.ShardingState {
		classState.MigrateFromOldFormat()
	}
	return nil
}

func newSchema() *State {
	return &State{
		ObjectSchema: &models.Schema{
			Classes: []*models.Class{},
		},
		ShardingState: map[string]*sharding.State{},
	}
}

func (m *Manager) parseConfigs(ctx context.Context, schema *State) error {
	// for _, class := range schema.ObjectSchema.Classes {
	// 	for _, prop := range class.Properties {
	// 		setPropertyDefaults(prop)
	// 		migratePropertySettings(prop)
	// 	}

	// 	if err := m.parseVectorIndexConfig(class); err != nil {
	// 		return errors.Wrapf(err, "class %s: vector index config", class.Class)
	// 	}

	// 	if err := m.parseShardingConfig(class); err != nil {
	// 		return errors.Wrapf(err, "class %s: sharding config", class.Class)
	// 	}

	// 	// Pass dummy replication config with minimum factor 1. Otherwise the
	// 	// setting is not backward-compatible. The user may have created a class
	// 	// with factor=1 before the change was introduced. Now their setup would no
	// 	// longer start up if the required minimum is now higher than 1. We want
	// 	// the required minimum to only apply to newly created classes - not block
	// 	// loading existing ones.
	// 	if err := replica.ValidateConfig(class, replication.GlobalConfig{MinimumFactor: 1}); err != nil {
	// 		return fmt.Errorf("replication config: %w", err)
	// 	}
	// }
	// m.schemaCache.LockGuard(func() {
	// 	for _, shardState := range schema.ShardingState {
	// 		shardState.SetLocalName(m.clusterState.LocalName())
	// 	}
	// })

	return nil
}

func (m *Manager) ClusterHealthScore() int {
	return m.clusterState.ClusterHealthScore()
}

// ResolveParentNodes gets all replicas for a specific class shard and resolves their names
//
// it returns map[node_name] node_address where node_address = "" if can't resolve node_name
func (m *Manager) ResolveParentNodes(class, shardName string) (map[string]string, error) {
	nodes, err := m.ShardReplicas(class, shardName)
	if err != nil {
		return nil, fmt.Errorf("get replicas from schema: %w", err)
	}

	if len(nodes) == 0 {
		return nil, nil
	}

	name2Addr := make(map[string]string, len(nodes))
	for _, node := range nodes {
		if node != "" {
			host, _ := m.clusterState.NodeHostname(node)
			name2Addr[node] = host
		}
	}
	return name2Addr, nil
}
