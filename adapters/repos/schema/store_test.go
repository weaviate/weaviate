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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	ucs "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

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

	// delete class
	deleteClass(&schema, "C2")
	repo.DeleteClass(ctx, "C2") // second call to test impotency
	if err := repo.DeleteClass(ctx, "C2"); err != nil {
		t.Errorf("delete bucket: %v", err)
	}
	repo.asserEqualSchema(t, schema, "delete class")
}

func TestRepositoryUpdateClass(t *testing.T) {
	var (
		ctx       = context.Background()
		logger, _ = test.NewNullLogger()
		dirName   = t.TempDir()
	)
	repo, err := newRepo(dirName, -1, logger)
	if err != nil {
		t.Fatalf("create new repo: %v", err)
	}

	// save and load non empty schema
	schema := ucs.NewState(3)
	cls, ss := addClass(&schema, "C1", 0, 1, 0)
	payload, err := ucs.CreateClassPayload(cls, ss)
	assert.Nil(t, err)
	if err := repo.NewClass(ctx, payload); err != nil {
		t.Fatalf("create new class: %v", err)
	}
	if err := repo.NewClass(ctx, payload); err == nil {
		t.Fatal("create new class: must fail since class already exits")
	}
	repo.asserEqualSchema(t, schema, "create class")

	// update class
	deleteClass(&schema, "C1")
	cls, ss = addClass(&schema, "C1", 0, 2, 1)

	payload, err = ucs.CreateClassPayload(cls, ss)
	assert.Nil(t, err)
	payload.Name = "C3"
	if err := repo.UpdateClass(ctx, payload); err == nil {
		t.Fatal("updating class by adding shards to non existing class must fail")
	}
	payload.Name = "C1"
	if err := repo.UpdateClass(ctx, payload); err != nil {
		t.Errorf("update class: %v", err)
	}
	repo.asserEqualSchema(t, schema, "update class")

	// overwrite class
	deleteClass(&schema, "C1")
	cls, ss = addClass(&schema, "C1", 2, 2, 3)
	payload, err = ucs.CreateClassPayload(cls, ss)
	assert.Nil(t, err)
	payload.ReplaceShards = true
	if err := repo.UpdateClass(ctx, payload); err != nil {
		t.Errorf("update class: %v", err)
	}
	repo.asserEqualSchema(t, schema, "overwrite class")

	// delete class
	deleteClass(&schema, "C1")
	repo.DeleteClass(ctx, "C1") // second call to test impotency
	if err := repo.DeleteClass(ctx, "C1"); err != nil {
		t.Errorf("delete bucket: %v", err)
	}
	repo.asserEqualSchema(t, schema, "delete class")
}

func TestRepositoryUpdateShards(t *testing.T) {
	var (
		ctx       = context.Background()
		logger, _ = test.NewNullLogger()
		dirName   = t.TempDir()
	)
	repo, err := newRepo(dirName, -1, logger)
	if err != nil {
		t.Fatalf("create new repo: %v", err)
	}

	schema := ucs.NewState(2)
	cls, ss := addClass(&schema, "C1", 0, 2, 1)
	payload, err := ucs.CreateClassPayload(cls, ss)
	assert.Nil(t, err)
	if err := repo.NewClass(ctx, payload); err != nil {
		t.Errorf("update class: %v", err)
	}
	repo.asserEqualSchema(t, schema, "update class")

	// add two shards
	deleteClass(&schema, "C1")
	_, ss = addClass(&schema, "C1", 0, 2, 5)
	shards := serializeShards(ss.Physical)
	if err := repo.NewShards(ctx, "C1", shards); err != nil {
		t.Fatalf("add new shards: %v", err)
	}
	if err := repo.NewShards(ctx, "C3", shards); err == nil {
		t.Fatal("add new shards to a non existing class must fail")
	}
	repo.asserEqualSchema(t, schema, "adding new shards")

	t.Run("fails updating non existent shards", func(t *testing.T) {
		nonExistentShards := createShards(4, 2, models.TenantActivityStatusCOLD)
		nonExistentShardPairs := serializeShards(nonExistentShards)
		err := repo.UpdateShards(ctx, "C1", nonExistentShardPairs)
		require.NotNil(t, err)
		assert.ErrorContains(t, err, "shard not found")
	})

	existentShards := createShards(3, 2, models.TenantActivityStatusCOLD)
	existentShardPairs := serializeShards(existentShards)

	t.Run("fails updating shards of non existent class", func(t *testing.T) {
		err := repo.UpdateShards(ctx, "ClassNonExistent", existentShardPairs)
		require.NotNil(t, err)
		assert.ErrorContains(t, err, "class not found")
	})
	t.Run("succeeds updating shards", func(t *testing.T) {
		err := repo.UpdateShards(ctx, "C1", existentShardPairs)
		require.Nil(t, err)

		replaceShards(ss, existentShards)
		repo.asserEqualSchema(t, schema, "update shards")
	})

	xset := removeShards(ss, []int{0, 3, 4})
	if err := repo.DeleteShards(ctx, "C1", xset); err != nil {
		t.Fatalf("delete shards: %v", err)
	}
	repo.asserEqualSchema(t, schema, "remove shards")

	if err := repo.DeleteShards(ctx, "C3", xset); err != nil {
		t.Fatalf("delete shards from unknown class: %v", err)
	}
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

func replaceShards(ss *sharding.State, shards map[string]sharding.Physical) {
	for name, shard := range shards {
		ss.Physical[name] = shard
	}
}

func removeShards(ss *sharding.State, shards []int) []string {
	res := make([]string, len(shards))
	for i, j := range shards {
		name := fmt.Sprintf("shard-%d", j)
		delete(ss.Physical, name)
		res[i] = name
	}
	return res
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

func deleteClass(schema *ucs.State, name string) {
	idx := -1
	for i, cls := range schema.ObjectSchema.Classes {
		if cls.Class == name {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}
	schema.ObjectSchema.Classes = append(schema.ObjectSchema.Classes[:idx], schema.ObjectSchema.Classes[idx+1:]...)
	delete(schema.ShardingState, name)
}

func (r *store) asserEqualSchema(t *testing.T, expected ucs.State, msg string) {
	t.Helper()
	actual, err := r.Load(context.Background())
	if err != nil {
		t.Fatalf("load schema: %s: %v", msg, err)
	}
	assert.Equal(t, expected, actual)
}

func serializeShards(shards map[string]sharding.Physical) []ucs.KeyValuePair {
	xs := make([]ucs.KeyValuePair, 0, len(shards))
	for k, v := range shards {
		val, _ := json.Marshal(&v)
		xs = append(xs, ucs.KeyValuePair{Key: k, Value: val})
	}
	return xs
}

func newRepo(homeDir string, version int, logger logrus.FieldLogger) (*store, error) {
	r := NewStore(homeDir, logger)
	if version > -1 {
		r.version = version
	}
	return r, r.Open()
}
