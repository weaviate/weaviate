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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/scaler"
	"github.com/weaviate/weaviate/usecases/schema/migrate"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TODO: These tests don't match the overall testing style in Weaviate.
// Refactor!
type NilMigrator struct{}

func (n *NilMigrator) AddClass(ctx context.Context, class *models.Class,
	shardingState *sharding.State,
) error {
	return nil
}

func (n *NilMigrator) DropClass(ctx context.Context, className string) error {
	return nil
}

func (n *NilMigrator) UpdateClass(ctx context.Context, className string, newClassName *string) error {
	return nil
}

func (n *NilMigrator) GetShardsQueueSize(ctx context.Context, className, tenant string) (map[string]int64, error) {
	return nil, nil
}

func (n *NilMigrator) GetShardsStatus(ctx context.Context, className, tenant string) (map[string]string, error) {
	return nil, nil
}

func (n *NilMigrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error {
	return nil
}

func (n *NilMigrator) AddProperty(ctx context.Context, className string, prop *models.Property) error {
	return nil
}

func (n *NilMigrator) NewTenants(ctx context.Context, class *models.Class, creates []*migrate.CreateTenantPayload) (commit func(success bool), err error) {
	return func(bool) {}, nil
}

func (n *NilMigrator) UpdateTenants(ctx context.Context, class *models.Class, updates []*migrate.UpdateTenantPayload) (commit func(success bool), err error) {
	return func(bool) {}, nil
}

func (n *NilMigrator) DeleteTenants(ctx context.Context, class *models.Class, tenants []string) (commit func(success bool), err error) {
	return func(bool) {}, nil
}

func (n *NilMigrator) UpdateProperty(ctx context.Context, className string, propName string, newName *string) error {
	return nil
}

func (n *NilMigrator) UpdatePropertyAddDataType(ctx context.Context, className string, propName string, newDataType string) error {
	return nil
}

func (n *NilMigrator) DropProperty(ctx context.Context, className string, propName string) error {
	return nil
}

func (n *NilMigrator) ValidateVectorIndexConfigUpdate(ctx context.Context, old, updated schema.VectorIndexConfig) error {
	return nil
}

func (n *NilMigrator) UpdateVectorIndexConfig(ctx context.Context, className string, updated schema.VectorIndexConfig) error {
	return nil
}

func (n *NilMigrator) ValidateInvertedIndexConfigUpdate(ctx context.Context, old, updated *models.InvertedIndexConfig) error {
	return nil
}

func (n *NilMigrator) UpdateInvertedIndexConfig(ctx context.Context, className string, updated *models.InvertedIndexConfig) error {
	return nil
}

func (n *NilMigrator) RecalculateVectorDimensions(ctx context.Context) error {
	return nil
}

func (n *NilMigrator) InvertedReindex(ctx context.Context, taskNames ...string) error {
	return nil
}

func (n *NilMigrator) AdjustFilterablePropSettings(ctx context.Context) error {
	return nil
}

func (n *NilMigrator) RecountProperties(ctx context.Context) error {
	return nil
}

var schemaTests = []struct {
	name string
	fn   func(*testing.T, *Manager)
}{
	{name: "AddObjectClass", fn: testAddObjectClass},
	{name: "AddObjectClassWithExplicitVectorizer", fn: testAddObjectClassExplicitVectorizer},
	{name: "AddObjectClassWithImplicitVectorizer", fn: testAddObjectClassImplicitVectorizer},
	{name: "AddObjectClassWithWrongVectorizer", fn: testAddObjectClassWrongVectorizer},
	{name: "AddObjectClassWithWrongIndexType", fn: testAddObjectClassWrongIndexType},
	{name: "RemoveObjectClass", fn: testRemoveObjectClass},
	{name: "CantAddSameClassTwice", fn: testCantAddSameClassTwice},
	{name: "CantAddSameClassTwiceDifferentKind", fn: testCantAddSameClassTwiceDifferentKinds},
	{name: "AddPropertyDuringCreation", fn: testAddPropertyDuringCreation},
	{name: "AddInvalidPropertyDuringCreation", fn: testAddInvalidPropertyDuringCreation},
	{name: "AddInvalidPropertyWithEmptyDataTypeDuringCreation", fn: testAddInvalidPropertyWithEmptyDataTypeDuringCreation},
	{name: "DropProperty", fn: testDropProperty},
}

func testAddObjectClass(t *testing.T, lsm *Manager) {
	t.Parallel()

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class: "Car",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
		VectorIndexConfig: map[string]interface{}{
			"dummy": "this should be parsed",
		},
	})

	assert.Nil(t, err)

	objectClassesNames = testGetClassNames(lsm)
	assert.Contains(t, objectClassesNames, "Car")

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Equal(t, config.VectorizerModuleNone, objectClasses[0].Vectorizer)
	assert.Equal(t, fakeVectorConfig{
		raw: map[string]interface{}{
			"distance": "cosine",
			"dummy":    "this should be parsed",
		},
	}, objectClasses[0].VectorIndexConfig)
	assert.Equal(t, int64(60), objectClasses[0].InvertedIndexConfig.CleanupIntervalSeconds,
		"the default was set")
}

func testAddObjectClassExplicitVectorizer(t *testing.T, lsm *Manager) {
	t.Parallel()

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Vectorizer:      config.VectorizerModuleText2VecContextionary,
		VectorIndexType: "hnsw",
		Class:           "Car",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
	})

	assert.Nil(t, err)

	objectClassesNames = testGetClassNames(lsm)
	assert.Contains(t, objectClassesNames, "Car")

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Equal(t, config.VectorizerModuleText2VecContextionary, objectClasses[0].Vectorizer)
	assert.Equal(t, "hnsw", objectClasses[0].VectorIndexType)
}

func testAddObjectClassImplicitVectorizer(t *testing.T, lsm *Manager) {
	t.Parallel()
	lsm.config.DefaultVectorizerModule = config.VectorizerModuleText2VecContextionary

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class: "Car",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
	})

	assert.Nil(t, err)

	objectClassesNames = testGetClassNames(lsm)
	assert.Contains(t, objectClassesNames, "Car")

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Equal(t, config.VectorizerModuleText2VecContextionary, objectClasses[0].Vectorizer)
	assert.Equal(t, "hnsw", objectClasses[0].VectorIndexType)
}

func testAddObjectClassWrongVectorizer(t *testing.T, lsm *Manager) {
	t.Parallel()

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "vectorizer-5000000",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
	})

	require.NotNil(t, err)
	assert.Equal(t, "vectorizer: invalid vectorizer \"vectorizer-5000000\"",
		err.Error())
}

func testAddObjectClassWrongIndexType(t *testing.T, lsm *Manager) {
	t.Parallel()

	objectClassesNames := testGetClassNames(lsm)
	assert.NotContains(t, objectClassesNames, "Car")

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class:           "Car",
		VectorIndexType: "vector-index-2-million",
		Properties: []*models.Property{{
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			Name:         "dummy",
		}},
	})

	require.NotNil(t, err)
	assert.Equal(t, "unrecognized or unsupported vectorIndexType "+
		"\"vector-index-2-million\"", err.Error())
}

func testRemoveObjectClass(t *testing.T, lsm *Manager) {
	t.Parallel()

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	})

	assert.Nil(t, err)

	objectClasses := testGetClassNames(lsm)
	assert.Contains(t, objectClasses, "Car")

	// Now delete the class
	err = lsm.DeleteClass(context.Background(), nil, "Car")
	assert.Nil(t, err)

	objectClasses = testGetClassNames(lsm)
	assert.NotContains(t, objectClasses, "Car")
}

func testCantAddSameClassTwice(t *testing.T, lsm *Manager) {
	t.Parallel()

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	})

	assert.Nil(t, err)

	// Add it again
	err = lsm.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	})

	assert.NotNil(t, err)
}

func testCantAddSameClassTwiceDifferentKinds(t *testing.T, lsm *Manager) {
	t.Parallel()

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	})

	assert.Nil(t, err)

	// Add it again, but with a different kind.
	err = lsm.AddClass(context.Background(), nil, &models.Class{
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Class:      "Car",
		Vectorizer: "text2vec-contextionary",
	})

	assert.NotNil(t, err)
}

// TODO: parts of this test contain text2vec-contextionary logic, but parts are
// also general logic
func testAddPropertyDuringCreation(t *testing.T, lsm *Manager) {
	t.Parallel()

	vFalse := false
	vTrue := true

	var properties []*models.Property = []*models.Property{
		{
			Name:         "color",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizePropertyName": true,
				},
			},
		},
		{
			Name:            "colorRaw1",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vFalse,
			IndexSearchable: &vFalse,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"skip": true,
				},
			},
		},
		{
			Name:            "colorRaw2",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"skip": true,
				},
			},
		},
		{
			Name:            "colorRaw3",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vFalse,
			IndexSearchable: &vTrue,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"skip": true,
				},
			},
		},
		{
			Name:            "colorRaw4",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vTrue,
			IndexSearchable: &vTrue,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"skip": true,
				},
			},
		},
		{
			Name:         "content",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizePropertyName": false,
				},
			},
		},
		{
			Name:         "allDefault",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
	}

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	require.Len(t, objectClasses[0].Properties, 7)
	assert.Equal(t, objectClasses[0].Properties[0].Name, "color")
	assert.Equal(t, objectClasses[0].Properties[0].DataType, schema.DataTypeText.PropString())

	assert.True(t, lsm.IndexedInverted("Car", "color"), "color should be indexed")
	assert.False(t, lsm.IndexedInverted("Car", "colorRaw1"), "colorRaw1 should not be indexed")
	assert.True(t, lsm.IndexedInverted("Car", "colorRaw2"), "colorRaw2 should be indexed")
	assert.True(t, lsm.IndexedInverted("Car", "colorRaw3"), "colorRaw3 should be indexed")
	assert.True(t, lsm.IndexedInverted("Car", "colorRaw4"), "colorRaw4 should be indexed")
	assert.True(t, lsm.IndexedInverted("Car", "allDefault"), "allDefault should be indexed")
}

func testAddInvalidPropertyDuringCreation(t *testing.T, lsm *Manager) {
	t.Parallel()

	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: []string{"blurp"}},
	}

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)
}

func testAddInvalidPropertyWithEmptyDataTypeDuringCreation(t *testing.T, lsm *Manager) {
	t.Parallel()

	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: []string{""}},
	}

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.NotNil(t, err)
}

func testDropProperty(t *testing.T, lsm *Manager) {
	// TODO: https://github.com/weaviate/weaviate/issues/973
	// Remove skip

	t.Skip()

	t.Parallel()

	var properties []*models.Property = []*models.Property{
		{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
	}

	err := lsm.AddClass(context.Background(), nil, &models.Class{
		Class:      "Car",
		Properties: properties,
	})
	assert.Nil(t, err)

	objectClasses := testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Len(t, objectClasses[0].Properties, 1)

	// Now drop the property
	lsm.DeleteClassProperty(context.Background(), nil, "Car", "color")

	objectClasses = testGetClasses(lsm)
	require.Len(t, objectClasses, 1)
	assert.Len(t, objectClasses[0].Properties, 0)
}

// This grant parent test setups up the temporary directory needed for the tests.
func TestSchema(t *testing.T) {
	// We need this test here to make sure that we wait until all child tests
	// (that can be run in parallel) have finished, before cleaning up the temp directory.
	t.Run("group", func(t *testing.T) {
		for _, testCase := range schemaTests {
			// Create a test case, and inject the etcd schema manager in there
			// to reduce boilerplate in each separate test.
			t.Run(testCase.name, func(t *testing.T) {
				sm := newSchemaManager()
				sm.StartServing(context.Background()) // will also mark tx manager as ready
				testCase.fn(t, sm)
			})
		}
	})
}

// New Local Schema *Manager
func newSchemaManager() *Manager {
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{"text2vec-contextionary", "model1", "model2"},
	}
	dummyConfig := config.Config{
		DefaultVectorizerModule:     config.VectorizerModuleNone,
		DefaultVectorDistanceMetric: "cosine",
	}
	sm, err := NewManager(&NilMigrator{}, newFakeRepo(), logger, &fakeAuthorizer{},
		dummyConfig, dummyParseVectorConfig, // only option for now
		vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, &fakeClusterState{hosts: []string{"node1"}},
		&fakeTxClient{}, &fakeTxPersistence{}, &fakeScaleOutManager{},
	)
	if err != nil {
		panic(err.Error())
	}

	sm.StartServing(context.Background()) // will also mark tx manager as ready

	return sm
}

func testGetClasses(l *Manager) []*models.Class {
	var classes []*models.Class
	schema, _ := l.GetSchema(nil)

	classes = append(classes, schema.SemanticSchemaFor().Classes...)

	return classes
}

func testGetClassNames(l *Manager) []string {
	var names []string
	schema, _ := l.GetSchema(nil)

	// Extract all names
	for _, class := range schema.SemanticSchemaFor().Classes {
		names = append(names, class.Class)
	}

	return names
}

func Test_ParseVectorConfigOnDiskLoad(t *testing.T) {
	logger, _ := test.NewNullLogger()

	repo := newFakeRepo()
	repo.schema = State{
		ObjectSchema: &models.Schema{
			Classes: []*models.Class{{
				Class:             "Foo",
				VectorIndexConfig: "parse me, i should be in some sort of an object",
				VectorIndexType:   "hnsw", // will always be set when loading from disk
			}},
		},
	}
	sm, err := NewManager(&NilMigrator{}, repo, logger, &fakeAuthorizer{},
		config.Config{DefaultVectorizerModule: config.VectorizerModuleNone},
		dummyParseVectorConfig, // only option for now
		&fakeVectorizerValidator{}, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, &fakeClusterState{hosts: []string{"node1"}},
		&fakeTxClient{}, &fakeTxPersistence{}, &fakeScaleOutManager{},
	)
	require.Nil(t, err)

	classes := sm.GetSchemaSkipAuth().Objects.Classes
	assert.Equal(t, fakeVectorConfig{
		raw: "parse me, i should be in some sort of an object",
	}, classes[0].VectorIndexConfig)
}

func Test_ExtendSchemaWithExistingPropName(t *testing.T) {
	logger, _ := test.NewNullLogger()

	repo := newFakeRepo()
	repo.schema = State{
		ObjectSchema: &models.Schema{
			Classes: []*models.Class{{
				Class:             "Foo",
				VectorIndexConfig: "parse me, i should be in some sort of an object",
				VectorIndexType:   "hnsw", // will always be set when loading from disk
				Properties: []*models.Property{{
					Name:         "my_prop",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				}},
			}},
		},
	}
	sm, err := NewManager(&NilMigrator{}, repo, logger, &fakeAuthorizer{},
		config.Config{DefaultVectorizerModule: config.VectorizerModuleNone},
		dummyParseVectorConfig, // only option for now
		&fakeVectorizerValidator{}, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, &fakeClusterState{hosts: []string{"node1"}},
		&fakeTxClient{}, &fakeTxPersistence{}, &fakeScaleOutManager{},
	)
	require.Nil(t, err)

	// exactly identical name
	err = sm.AddClassProperty(context.Background(), nil, "Foo", &models.Property{
		Name:     "my_prop",
		DataType: []string{"int"},
	})

	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "conflict for property")

	// identical if case insensitive
	err = sm.AddClassProperty(context.Background(), nil, "Foo", &models.Property{
		Name:     "mY_pROp",
		DataType: []string{"int"},
	})

	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "conflict for property")
}

type fakeScaleOutManager struct{}

func (f *fakeScaleOutManager) Scale(ctx context.Context,
	className string, updated sharding.Config, _, _ int64,
) (*sharding.State, error) {
	return nil, nil
}

func (f *fakeScaleOutManager) SetSchemaManager(sm scaler.SchemaManager) {
}

// does nothing as these do not involve crashes
type fakeTxPersistence struct{}

func (f *fakeTxPersistence) StoreTx(ctx context.Context,
	tx *cluster.Transaction,
) error {
	return nil
}

func (f *fakeTxPersistence) DeleteTx(ctx context.Context,
	txID string,
) error {
	return nil
}

func (f *fakeTxPersistence) IterateAll(ctx context.Context,
	cb func(tx *cluster.Transaction),
) error {
	return nil
}

type fakeBroadcaster struct {
	openErr       error
	commitErr     error
	abortErr      error
	abortCalledId string
}

func (f *fakeBroadcaster) BroadcastTransaction(ctx context.Context,
	tx *cluster.Transaction,
) error {
	return f.openErr
}

func (f *fakeBroadcaster) BroadcastAbortTransaction(ctx context.Context,
	tx *cluster.Transaction,
) error {
	f.abortCalledId = tx.ID
	return f.abortErr
}

func (f *fakeBroadcaster) BroadcastCommitTransaction(ctx context.Context,
	tx *cluster.Transaction,
) error {
	return f.commitErr
}
