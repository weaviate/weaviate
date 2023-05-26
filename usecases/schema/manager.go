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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/scaler"
	"github.com/weaviate/weaviate/usecases/schema/migrate"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Manager Manages schema changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	migrator                migrate.Migrator
	repo                    Repo
	state                   State
	callbacks               []func(updatedSchema schema.Schema)
	logger                  logrus.FieldLogger
	Authorizer              authorizer
	config                  config.Config
	vectorizerValidator     VectorizerValidator
	moduleConfig            ModuleConfig
	cluster                 *cluster.TxManager
	clusterState            clusterState
	hnswConfigParser        VectorConfigParser
	invertedConfigValidator InvertedConfigValidator
	scaleOut                scaleOut
	RestoreStatus           sync.Map
	RestoreError            sync.Map
	sync.RWMutex
	shardingStateLock sync.RWMutex
}

type VectorConfigParser func(in interface{}) (schema.VectorIndexConfig, error)

type InvertedConfigValidator func(in *models.InvertedIndexConfig) error

type SchemaGetter interface {
	GetSchemaSkipAuth() schema.Schema
	ShardingState(class string) *sharding.State
	Nodes() []string
	NodeName() string
	ClusterHealthScore() int
	ResolveParentNodes(string, string) (map[string]string, error)
}

type VectorizerValidator interface {
	ValidateVectorizer(moduleName string) error
}

type ModuleConfig interface {
	SetClassDefaults(class *models.Class)
	SetSinglePropertyDefaults(class *models.Class, prop *models.Property)
	ValidateClass(ctx context.Context, class *models.Class) error
}

// Repo describes the requirements the schema manager has to a database to load
// and persist the schema state
type Repo interface {
	SaveSchema(ctx context.Context, schema State) error

	// should return nil (and no error) to indicate that no remote schema had
	// been stored before
	LoadSchema(ctx context.Context) (*State, error)
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
}

type scaleOut interface {
	SetSchemaManager(sm scaler.SchemaManager)
	Scale(ctx context.Context, className string,
		updated sharding.Config, prevReplFactor, newReplFactor int64) (*sharding.State, error)
}

// NewManager creates a new manager
func NewManager(migrator migrate.Migrator, repo Repo,
	logger logrus.FieldLogger, authorizer authorizer, config config.Config,
	hnswConfigParser VectorConfigParser, vectorizerValidator VectorizerValidator,
	invertedConfigValidator InvertedConfigValidator,
	moduleConfig ModuleConfig, clusterState clusterState,
	txClient cluster.Client, scaleoutManager scaleOut,
) (*Manager, error) {
	txBroadcaster := cluster.NewTxBroadcaster(clusterState, txClient)
	m := &Manager{
		config:                  config,
		migrator:                migrator,
		repo:                    repo,
		state:                   State{},
		logger:                  logger,
		Authorizer:              authorizer,
		hnswConfigParser:        hnswConfigParser,
		vectorizerValidator:     vectorizerValidator,
		invertedConfigValidator: invertedConfigValidator,
		moduleConfig:            moduleConfig,
		cluster:                 cluster.NewTxManager(txBroadcaster, logger),
		clusterState:            clusterState,
		scaleOut:                scaleoutManager,
	}

	m.scaleOut.SetSchemaManager(m)

	m.cluster.SetCommitFn(m.handleCommit)
	m.cluster.SetResponseFn(m.handleTxResponse)
	txBroadcaster.SetConsensusFunction(newReadConsensus(m.parseConfigs, m.logger))

	err := m.loadOrInitializeSchema(context.Background())
	if err != nil {
		return nil, fmt.Errorf("could not load or initialize schema: %v", err)
	}

	return m, nil
}

func (m *Manager) TxManager() *cluster.TxManager {
	return m.cluster
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

// State is a cached copy of the schema that can also be saved into a remote
// storage, as specified by Repo
type State struct {
	ObjectSchema  *models.Schema `json:"object"`
	ShardingState map[string]*sharding.State
}

func (m *Manager) saveSchema(ctx context.Context, doAfter func(err error)) error {
	m.logger.
		WithField("action", "schema_update").
		Debug("saving updated schema to configuration store")

	err := m.repo.SaveSchema(ctx, m.state)
	if doAfter != nil {
		doAfter(err)
	}
	if err != nil {
		return err
	}
	m.triggerSchemaUpdateCallbacks()
	return nil
}

// RegisterSchemaUpdateCallback allows other usecases to register a primitive
// type update callback. The callbacks will be called any time we persist a
// schema upadate
func (m *Manager) RegisterSchemaUpdateCallback(callback func(updatedSchema schema.Schema)) {
	m.callbacks = append(m.callbacks, callback)
}

func (m *Manager) triggerSchemaUpdateCallbacks() {
	schema := m.getSchema()

	for _, cb := range m.callbacks {
		cb(schema)
	}
}

func (m *Manager) loadOrInitializeSchema(ctx context.Context) error {
	schema, err := m.repo.LoadSchema(ctx)
	if err != nil {
		return fmt.Errorf("could not load schema:  %v", err)
	}

	if schema == nil {
		schema = newSchema()
	}

	if err := m.parseConfigs(ctx, schema); err != nil {
		return errors.Wrap(err, "load schema")
	}

	// store in local cache
	m.state = *schema

	if err := m.migrateSchemaIfNecessary(ctx); err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}

	// There was a bug that allowed adding the same prop multiple times. This
	// leads to a race at startup. If an instance is already affected by this,
	// this step can remove the duplicate ones.
	//
	// See https://github.com/weaviate/weaviate/issues/2609
	m.removeDuplicatePropsIfPresent()

	// make sure that all migrations have completed before checking sync,
	// otherwise two identical schemas might fail the check based on form rather
	// than content

	if err := m.startupClusterSync(ctx, schema); err != nil {
		return errors.Wrap(err, "sync schema with other nodes in the cluster")
	}

	// store in persistent storage
	if err := m.repo.SaveSchema(ctx, m.state); err != nil {
		return fmt.Errorf("initialized a new schema, but couldn't update remote: %v", err)
	}

	return nil
}

func (m *Manager) migrateSchemaIfNecessary(ctx context.Context) error {
	// introduced when Weaviate started supporting multi-shards per class in v1.8
	if err := m.checkSingleShardMigration(ctx); err != nil {
		return errors.Wrap(err, "migrating sharding state from previous version")
	}

	// introduced when Weaviate started supporting replication in v1.17
	if err := m.checkShardingStateForReplication(ctx); err != nil {
		return errors.Wrap(err, "migrating sharding state from previous version (before replication)")
	}

	// if other migrations become necessary in the future, you can add them here.
	return nil
}

func (m *Manager) checkSingleShardMigration(ctx context.Context) error {
	for _, c := range m.state.ObjectSchema.Classes {
		m.shardingStateLock.RLock()
		_, ok := m.state.ShardingState[c.Class]
		m.shardingStateLock.RUnlock()
		if ok { // there is sharding state for this class. Nothing to do
			continue
		}

		m.logger.WithField("className", c.Class).WithField("action", "initialize_schema").
			Warningf("No sharding state found for class %q, initializing new state. "+
				"This is expected behavior if the schema was created with an older Weaviate "+
				"version, prior to supporting multi-shard indices.", c.Class)

		// there is no sharding state for this class, let's create the correct
		// config. This class must have been created prior to the sharding feature,
		// so we now that the shardCount==1 - we do not care about any of the other
		// parameters and simply use the defaults for those
		c.ShardingConfig = map[string]interface{}{
			"desiredCount": 1,
		}
		if err := m.parseShardingConfig(ctx, c); err != nil {
			return err
		}

		if err := replica.ValidateConfig(c); err != nil {
			return fmt.Errorf("validate replication config: %w", err)
		}
		shardState, err := sharding.InitState(c.Class,
			c.ShardingConfig.(sharding.Config),
			m.clusterState, c.ReplicationConfig.Factor,
			isMultiTenancyEnabled(c.MultiTenancyConfig))
		if err != nil {
			return errors.Wrap(err, "init sharding state")
		}

		m.shardingStateLock.Lock()
		if m.state.ShardingState == nil {
			m.state.ShardingState = map[string]*sharding.State{}
		}
		m.state.ShardingState[c.Class] = shardState
		m.shardingStateLock.Unlock()

	}

	return nil
}

func (m *Manager) checkShardingStateForReplication(ctx context.Context) error {
	m.shardingStateLock.Lock()
	defer m.shardingStateLock.Unlock()

	for _, classState := range m.state.ShardingState {
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
	for _, class := range schema.ObjectSchema.Classes {
		for _, prop := range class.Properties {
			m.setPropertyDefaults(prop)
			m.migratePropertySettings(prop)
		}

		if err := m.parseVectorIndexConfig(ctx, class); err != nil {
			return errors.Wrapf(err, "class %s: vector index config", class.Class)
		}

		if err := m.parseShardingConfig(ctx, class); err != nil {
			return errors.Wrapf(err, "class %s: sharding config", class.Class)
		}

		if err := replica.ValidateConfig(class); err != nil {
			return fmt.Errorf("replication config: %w", err)
		}
	}
	m.shardingStateLock.Lock()
	for _, shardState := range schema.ShardingState {
		shardState.SetLocalName(m.clusterState.LocalName())
	}
	m.shardingStateLock.Unlock()

	return nil
}
