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
	"io"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// fixedActivityProber answers the backup activity probe from a static map.
type fixedActivityProber map[string]backup.NodeActivity

func (p fixedActivityProber) NodeActivity(_ context.Context, nodeName string) (backup.NodeActivity, error) {
	return p[nodeName], nil
}

// fixedMembership is the node list the gate fans out over.
type fixedMembership []string

func (m fixedMembership) AllNames() []string { return m }

// TestUpdateIndexRefusesWhileBackupRuns drives the submission handler end to
// end against a node holding a backup slot. Without the gate the handler
// reaches the task submission and answers 202, so removing the call fails here
// and not only in the acceptance suite.
func TestUpdateIndexRefusesWhileBackupRuns(t *testing.T) {
	const (
		collection = "Movies"
		property   = "title"
		node       = "node1"
	)

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	indexFilterable := true
	class := &models.Class{
		Class: collection,
		Properties: []*models.Property{{
			Name:            property,
			DataType:        []string{"text"},
			IndexFilterable: &indexFilterable,
		}},
	}
	shardState := &sharding.State{
		IndexID:  collection,
		Physical: map[string]sharding.Physical{"shard1": {Name: "shard1", BelongsToNodes: []string{node}}},
	}

	reader := schemaUC.NewMockSchemaReader(t)
	reader.On("ReadOnlyClass", collection).Return(class).Maybe()
	reader.On("Read", collection, true, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(*models.Class, *sharding.State) error)
		require.NoError(t, fn(class, shardState))
	}).Maybe()

	theDB := &db.DB{}
	theDB.SetSchemaReader(reader)

	h := &indexesHandlers{
		appState: &state.State{
			Authorizer:         &authorization.DummyAuthorizer{},
			ReindexSubmitLocks: state.NewReindexSubmitLocks(),
			Logger:             logger,
			ServerConfig:       &config.WeaviateConfig{Config: config.Config{}},
			SchemaManager:      &schemaUC.Manager{SchemaReader: reader},
			DB:                 theDB,
			// ClusterService left nil: the conflict and cap checks it feeds are
			// skipped, so the gate is the next thing the handler reaches.
		},
		cluster: fixedMembership{node},
		backupActivity: fixedActivityProber{node: backup.NodeActivity{
			Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1",
		}},
	}

	responder := h.updateIndex(schema.SchemaObjectsIndexesUpdateParams{
		HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
		ClassName:    collection,
		PropertyName: property,
		Body: &models.IndexUpdateRequest{
			Filterable: &models.IndexUpdateFilterable{Rebuild: true},
		},
	}, &models.Principal{Username: "u1"})

	conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
	require.Truef(t, ok, "a running backup must be refused with 409, got %T", responder)
	require.Equal(t,
		"reindex blocked: a backup is running in the cluster; retry after it finishes",
		errorMessage(t, conflict.Payload))
}

// The submission handler reaches the cluster service unguarded, unlike the
// cancel handler. Answer 503 rather than crashing the request.
func TestUpdateIndexWithoutClusterServiceIsUnavailable(t *testing.T) {
	const (
		collection = "Movies"
		property   = "title"
		node       = "node1"
	)

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	indexFilterable := true
	class := &models.Class{
		Class: collection,
		Properties: []*models.Property{{
			Name:            property,
			DataType:        []string{"text"},
			IndexFilterable: &indexFilterable,
		}},
	}
	shardState := &sharding.State{
		IndexID:  collection,
		Physical: map[string]sharding.Physical{"shard1": {Name: "shard1", BelongsToNodes: []string{node}}},
	}

	reader := schemaUC.NewMockSchemaReader(t)
	reader.On("ReadOnlyClass", collection).Return(class).Maybe()
	reader.On("Read", collection, true, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(*models.Class, *sharding.State) error)
		require.NoError(t, fn(class, shardState))
	}).Maybe()

	theDB := &db.DB{}
	theDB.SetSchemaReader(reader)

	h := &indexesHandlers{
		appState: &state.State{
			Authorizer:         &authorization.DummyAuthorizer{},
			ReindexSubmitLocks: state.NewReindexSubmitLocks(),
			Logger:             logger,
			ServerConfig:       &config.WeaviateConfig{Config: config.Config{}},
			SchemaManager:      &schemaUC.Manager{SchemaReader: reader},
			DB:                 theDB,
		},
		// No cluster and no prober, so the backup gate allows the submission
		// through and the missing cluster service is what answers.
	}

	responder := h.updateIndex(schema.SchemaObjectsIndexesUpdateParams{
		HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
		ClassName:    collection,
		PropertyName: property,
		Body: &models.IndexUpdateRequest{
			Filterable: &models.IndexUpdateFilterable{Rebuild: true},
		},
	}, &models.Principal{Username: "u1"})

	unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
	require.Truef(t, ok, "expected 503, got %T", responder)
	require.Equal(t, "cluster service unavailable; cannot submit reindex task",
		errorMessage(t, unavailable.Payload))
}
