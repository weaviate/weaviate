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

package objects

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// fakeObjectCounter is a minimal stand-in for db.DB.LocalObjectCount.
// It returns a fixed count so tests can drive the limit gate
// deterministically without spinning up a real DB.
type fakeObjectCounter struct {
	count int64
}

func (f *fakeObjectCounter) LocalObjectCount(_ context.Context) (int64, error) {
	return f.count, nil
}

// usageLimitsManagerObjectCap builds a *usagelimits.Manager configured with
// just the object-count cap and a stub counter. Used by AddObject /
// AddObjects unit tests that exercise only the limit gate.
func usageLimitsManagerObjectCap(cap int, currentCount int64) *usagelimits.Manager {
	return usagelimits.NewManager(usagelimits.Config{
		MaxObjectsCount: runtime.NewDynamicValue(cap),
		ErrorMessage:    runtime.NewDynamicValue(""),
	}, &fakeObjectCounter{count: currentCount}, nil, nil)
}

// schemaWithFooClass mirrors the inline schema setup used elsewhere in
// the package (see add_test.go) so AddObject finds a known class.
func schemaWithFooClass() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "Foo",
					Vectorizer:        config.VectorizerModuleNone,
					VectorIndexConfig: hnsw.UserConfig{},
				},
			},
		},
	}
}

// TestAddObject_RejectsWhenObjectLimitExceeded confirms the single-object
// hook in usecases/objects/add.go fires *LimitExceededError once the
// per-instance object-count limit is hit. The REST/gRPC layer maps that
// to HTTP 429 / RESOURCE_EXHAUSTED elsewhere; here we only assert the
// usecase contract.
func TestAddObject_RejectsWhenObjectLimitExceeded(t *testing.T) {
	logger, _ := test.NewNullLogger()
	authorizer := mocks.NewMockAuthorizer()
	vectorRepo := &fakeVectorRepo{}
	modulesProvider := getFakeModulesProvider()
	schemaManager := &fakeSchemaManager{GetSchemaResponse: schemaWithFooClass()}
	cfg := &config.WeaviateConfig{}

	// Cap = 10, current = 10 → next AddObject must be rejected.
	manager := NewManager(schemaManager, cfg, logger, authorizer, vectorRepo,
		modulesProvider, &fakeMetrics{}, nil,
		NewAutoSchemaManager(schemaManager, vectorRepo, cfg, authorizer, logger, prometheus.NewPedanticRegistry()),
		usageLimitsManagerObjectCap(10, 10))

	_, err := manager.AddObject(context.Background(), nil,
		&models.Object{Class: "Foo"}, nil)
	require.Error(t, err, "expected limit-exceeded rejection at the object cap")

	le, ok := usagelimits.AsLimitExceeded(err)
	require.True(t, ok, "expected *LimitExceededError, got %T: %v", err, err)
	assert.Equal(t, usagelimits.LimitObjects, le.Limit)
	assert.Equal(t, int64(10), le.Value)
}

// TestBatchAddObjects_WholeBatchRejection confirms the batch hook in
// usecases/objects/batch_add.go rejects the whole batch when it would push
// the object count past the limit — no partial fill, per the RFC's
// whole-batch-rejection rule.
func TestBatchAddObjects_WholeBatchRejection(t *testing.T) {
	logger, _ := test.NewNullLogger()
	authorizer := mocks.NewMockAuthorizer()
	vectorRepo := &fakeVectorRepo{}
	modulesProvider := getFakeModulesProvider()
	schemaManager := &fakeSchemaManager{GetSchemaResponse: schemaWithFooClass()}
	cfg := &config.WeaviateConfig{}

	// Cap = 10, current = 8 → batch of 5 would overflow → reject all.
	manager := NewBatchManager(vectorRepo, modulesProvider, schemaManager, cfg,
		logger, authorizer, nil,
		NewAutoSchemaManager(schemaManager, vectorRepo, cfg, authorizer, logger, prometheus.NewPedanticRegistry()),
		usageLimitsManagerObjectCap(10, 8))

	batch := make([]*models.Object, 5)
	for i := range batch {
		batch[i] = &models.Object{Class: "Foo"}
	}
	_, err := manager.AddObjects(context.Background(), nil, batch, nil, nil)
	require.Error(t, err, "expected whole-batch rejection when batch would exceed cap")

	le, ok := usagelimits.AsLimitExceeded(err)
	require.True(t, ok, "expected *LimitExceededError, got %T: %v", err, err)
	assert.Equal(t, usagelimits.LimitObjects, le.Limit)
	assert.Equal(t, int64(10), le.Value)
}

// TestAddObject_NoLimitWhenManagerNil confirms that a nil usagelimits
// Manager is a no-op (used in tests / older deployments where the
// guardrails are not wired).
func TestAddObject_NoLimitWhenManagerNil(t *testing.T) {
	logger, _ := test.NewNullLogger()
	authorizer := mocks.NewMockAuthorizer()
	vectorRepo := &fakeVectorRepo{}
	vectorRepo.On("PutObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	modulesProvider := getFakeModulesProvider()
	modulesProvider.On("UpdateVector", mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	schemaManager := &fakeSchemaManager{GetSchemaResponse: schemaWithFooClass()}
	cfg := &config.WeaviateConfig{}

	manager := NewManager(schemaManager, cfg, logger, authorizer, vectorRepo,
		modulesProvider, &fakeMetrics{}, nil,
		NewAutoSchemaManager(schemaManager, vectorRepo, cfg, authorizer, logger, prometheus.NewPedanticRegistry()),
		nil)

	_, err := manager.AddObject(context.Background(), nil,
		&models.Object{Class: "Foo", Vector: []float32{0.1, 0.2, 0.3}}, nil)
	if err != nil {
		_, isLimitErr := usagelimits.AsLimitExceeded(err)
		assert.False(t, isLimitErr, "no limit should fire when Manager is nil; got %v", err)
	}
}
