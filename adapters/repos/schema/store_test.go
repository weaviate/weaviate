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

package schema

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	ucs "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestSaveAndLoadSchema(t *testing.T) {
	var (
		ctx       = context.Background()
		logger, _ = test.NewNullLogger()
		dirName   = t.TempDir()
	)

	schema := ucs.NewState(2)
	addClass(&schema, "C1", 0, 1, 0)
	addClass(&schema, "C2", 0, 3, 3)

	// Save the schema
	repo, _ := newRepo(dirName, 0, logger)
	defer repo.Close()

	cs := map[string]types.ClassState{}
	for _, s := range schema.ObjectSchema.Classes {
		cs[s.Class] = types.ClassState{
			Class:  *s,
			Shards: *schema.ShardingState[s.Class],
		}
	}

	if err := repo.SaveLegacySchema(cs); err != nil {
		t.Fatalf("save all schema: %v", err)
	}

	// Load the schema
	loadedSchema, err := repo.Load(ctx)
	if err != nil {
		t.Fatalf("load schema: %v", err)
	}

	// Assert that the loaded schema is the same as the saved schema

	if !reflect.DeepEqual(schema.ObjectSchema, loadedSchema.ObjectSchema) {
		t.Errorf("loaded schema does not match saved schema")
	}
	if !reflect.DeepEqual(schema.ShardingState, loadedSchema.ShardingState) {
		t.Errorf("loaded sharding state does not match saved sharding state")
	}
}

func TestRepositoryMigrate(t *testing.T) {
	var (
		ctx                 = context.Background()
		logger, _           = test.NewNullLogger()
		dirName             = t.TempDir()
		canceledCtx, cancel = context.WithCancel(ctx)
	)
	cancel()
	schema := ucs.NewState(3)
	addClass(&schema, "C1", 0, 1, 0)
	addClass(&schema, "C2", 0, 3, 3)
	t.Run("SaveOldSchema", func(t *testing.T) {
		repo, _ := newRepo(dirName, 0, logger)
		defer repo.Close()
		if err := repo.saveSchemaV1(schema); err != nil {
			t.Fatalf("save all schema: %v", err)
		}
	})
	t.Run("LoadOldchema", func(t *testing.T) {
		repo, err := newRepo(dirName, -1, logger)
		if err != nil {
			t.Fatalf("create new repo %v", err)
		}
		defer repo.Close()

		_, err = repo.Load(canceledCtx)
		assert.ErrorIs(t, err, context.Canceled)

		state, err := repo.Load(ctx)
		assert.Nil(t, err)
		assert.Equal(t, schema, state)
	})
	t.Run("LoadSchema", func(t *testing.T) {
		repo, err := newRepo(dirName, -1, logger)
		if err != nil {
			t.Fatalf("create new repo %v", err)
		}
		defer repo.Close()

		state, err := repo.Load(ctx)
		assert.Nil(t, err)
		assert.Equal(t, schema, state)
	})

	t.Run("LoadSchemaWithHigherVersion", func(t *testing.T) {
		_, err := newRepo(dirName, 1, logger)
		assert.NotNil(t, err)
	})
}

func TestRepositorySaveLoad(t *testing.T) {
	var (
		ctx                 = context.Background()
		canceledCtx, cancel = context.WithCancel(ctx)
		logger, _           = test.NewNullLogger()
		dirName             = t.TempDir()
	)
	cancel()
	repo, err := newRepo(dirName, -1, logger)
	if err != nil {
		t.Fatalf("create new repo: %v", err)
	}
	// load empty schema
	res, err := repo.Load(ctx)
	if err != nil {
		t.Fatalf("loading schema from empty file: %v", err)
	}
	if len(res.ShardingState) != 0 || len(res.ObjectSchema.Classes) != 0 {
		t.Fatalf("expected empty schema got %v", res)
	}

	// save and load non empty schema
	schema := ucs.NewState(3)
	addClass(&schema, "C1", 0, 1, 0)
	addClass(&schema, "C2", 0, 3, 3)
	err = repo.Save(canceledCtx, schema)
	assert.ErrorIs(t, err, context.Canceled)

	if err = repo.Save(ctx, schema); err != nil {
		t.Fatalf("save schema: %v", err)
	}
	if err = repo.Save(ctx, schema); err != nil {
		t.Fatalf("save schema: %v", err)
	}

	res, err = repo.Load(context.Background())
	if err != nil {
		t.Fatalf("load schema: %v", err)
	}
	assert.Equal(t, schema, res)
}

func createClass(name string, start, nProps, nShards int) (models.Class, sharding.State) {
	cls := models.Class{Class: name}
	for i := start; i < start+nProps; i++ {
		prop := models.Property{
			Name:         fmt.Sprintf("property-%d", i),
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}
		cls.Properties = append(cls.Properties, &prop)
	}
	ss := sharding.State{IndexID: name}
	ss.Physical = createShards(start, nShards, models.TenantActivityStatusHOT)

	return cls, ss
}

func createShards(start, nShards int, activityStatus string) map[string]sharding.Physical {
	if nShards < 1 {
		return nil
	}

	shards := make(map[string]sharding.Physical, nShards)
	for i := start; i < start+nShards; i++ {
		name := fmt.Sprintf("shard-%d", i)
		node := fmt.Sprintf("node-%d", i)
		shards[name] = sharding.Physical{
			Name:           name,
			BelongsToNodes: []string{node},
			Status:         activityStatus,
		}
	}
	return shards
}

func addClass(schema *ucs.State, name string, start, nProps, nShards int) (*models.Class, *sharding.State) {
	cls, ss := createClass(name, start, nProps, nShards)
	if schema.ObjectSchema == nil {
		schema.ObjectSchema = &models.Schema{}
	}
	if schema.ShardingState == nil {
		schema.ShardingState = make(map[string]*sharding.State)
	}
	schema.ObjectSchema.Classes = append(schema.ObjectSchema.Classes, &cls)
	schema.ShardingState[name] = &ss
	return &cls, &ss
}

func newRepo(homeDir string, version int, logger logrus.FieldLogger) (*store, error) {
	r := NewStore(homeDir, logger)
	if version > -1 {
		r.version = version
	}
	return r, r.Open()
}
