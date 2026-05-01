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

package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestAddTenants(t *testing.T) {
	var (
		ctx        = context.Background()
		tenants    = []*models.Tenant{{Name: "USER1"}, {Name: "USER2"}}
		properties = []*models.Property{
			{
				Name:     "uuid",
				DataType: schema.DataTypeText.PropString(),
			},
		}
		repConfig = &models.ReplicationConfig{Factor: 1}
	)

	mtNilClass := &models.Class{
		Class:              "MTnil",
		MultiTenancyConfig: nil,
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}
	mtDisabledClass := &models.Class{
		Class:              "MTdisabled",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}
	mtEnabledClass := &models.Class{
		Class:              "MTenabled",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}
	type test struct {
		name      string
		class     string
		tenants   []*models.Tenant
		errMsgs   []string
		mockCalls func(fakeSchemaManager *fakeSchemaManager)
	}

	tests := []test{
		{
			name:    "MTIsNil",
			class:   mtNilClass.Class,
			tenants: tenants,
			errMsgs: nil,
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {
				// MT validation is done leader side now
				fakeSchemaManager.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:    "MTDisabled",
			class:   mtDisabledClass.Class,
			tenants: tenants,
			errMsgs: nil,
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {
				// MT validation is done leader side now
				fakeSchemaManager.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:    "UnknownClass",
			class:   "UnknownClass",
			tenants: tenants,
			errMsgs: nil,
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {
				fakeSchemaManager.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:  "EmptyTenantValue",
			class: mtEnabledClass.Class,
			tenants: []*models.Tenant{
				{Name: "Aaaa"},
				{Name: ""},
				{Name: "Bbbb"},
			},
			errMsgs:   []string{"tenant"},
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {},
		},
		{
			name:  "InvalidActivityStatus",
			class: mtEnabledClass.Class,
			tenants: []*models.Tenant{
				{Name: "Aaaa", ActivityStatus: "DOES_NOT_EXIST_1"},
				{Name: "Bbbb", ActivityStatus: "DOES_NOT_EXIST_2"},
				{Name: "Bbbb2", ActivityStatus: "WARM"},
			},
			errMsgs: []string{
				"invalid activity status",
				"DOES_NOT_EXIST_1",
				"DOES_NOT_EXIST_2",
			},
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {},
		},
		{
			name:  "Success",
			class: mtEnabledClass.Class,
			tenants: []*models.Tenant{
				{Name: "Aaaa"},
				{Name: "Bbbb", ActivityStatus: models.TenantActivityStatusHOT},
				{Name: "Cccc", ActivityStatus: models.TenantActivityStatusCOLD},
			},
			errMsgs: []string{},
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {
				fakeSchemaManager.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		// TODO test with replication factor >= 2
	}

	// AddTenants
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Isolate schema for each tests
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

			test.mockCalls(fakeSchemaManager)

			_, err := handler.AddTenants(ctx, nil, test.class, test.tenants)

			fakeSchemaManager.AssertExpectations(t)

			if len(test.errMsgs) == 0 {
				require.NoError(t, err)
			} else {
				for _, msg := range test.errMsgs {
					assert.ErrorContains(t, err, msg)
				}
			}
		})
	}
}

func TestUpdateTenants(t *testing.T) {
	var (
		ctx     = context.Background()
		tenants = []*models.Tenant{
			{Name: "USER1", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER2", ActivityStatus: models.TenantActivityStatusACTIVE},
		}
		properties = []*models.Property{
			{
				Name:     "uUID",
				DataType: schema.DataTypeText.PropString(),
			},
		}
		repConfig = &models.ReplicationConfig{Factor: 1}
	)

	mtNilClass := &models.Class{
		Class:              "MTnil",
		MultiTenancyConfig: nil,
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}
	mtDisabledClass := &models.Class{
		Class:              "MTdisabled",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}
	mtEnabledClass := &models.Class{
		Class:              "MTenabled",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}

	type test struct {
		name            string
		class           string
		updateTenants   []*models.Tenant
		errMsgs         []string
		expectedTenants []*models.Tenant
		mockCalls       func(fakeSchemaManager *fakeSchemaManager)
	}

	tests := []test{
		{
			name:            "MTIsNil",
			class:           mtNilClass.Class,
			updateTenants:   tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeSchemaManager) {
				fakeMetaHandler.On("UpdateTenants", mock.Anything, mock.Anything).Return(uint64(0), nil)
				fakeMetaHandler.On("TenantsShardsWithVersion", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					map[string]string{"USER1": models.TenantActivityStatusCOLD, "USER2": models.TenantActivityStatusCOLD}, nil)
			},
		},
		{
			name:            "MTDisabled",
			class:           mtDisabledClass.Class,
			updateTenants:   tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeSchemaManager) {
				fakeMetaHandler.On("UpdateTenants", mock.Anything, mock.Anything).Return(uint64(0), nil)
				fakeMetaHandler.On("TenantsShardsWithVersion", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					map[string]string{"USER1": models.TenantActivityStatusCOLD, "USER2": models.TenantActivityStatusCOLD}, nil)
			},
		},
		{
			name:            "UnknownClass",
			class:           "UnknownClass",
			updateTenants:   tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeSchemaManager) {
				fakeMetaHandler.On("UpdateTenants", mock.Anything, mock.Anything).Return(uint64(0), nil)
				fakeMetaHandler.On("TenantsShardsWithVersion", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					map[string]string{"USER1": models.TenantActivityStatusCOLD, "USER2": models.TenantActivityStatusCOLD}, nil)
			},
		},
		{
			name:  "EmptyTenantValue",
			class: mtEnabledClass.Class,
			updateTenants: []*models.Tenant{
				{Name: "", ActivityStatus: models.TenantActivityStatusCOLD},
			},
			errMsgs:         []string{"tenant"},
			expectedTenants: tenants,
			mockCalls:       func(fakeSchemaManager *fakeSchemaManager) {},
		},
		{
			name:  "InvalidActivityStatus",
			class: mtEnabledClass.Class,
			updateTenants: []*models.Tenant{
				{Name: tenants[0].Name, ActivityStatus: "DOES_NOT_EXIST_1"},
				{Name: tenants[1].Name, ActivityStatus: "WARM"},
			},
			errMsgs: []string{
				"invalid activity status",
				"DOES_NOT_EXIST_1",
				"WARM",
			},
			expectedTenants: tenants,
			mockCalls:       func(fakeSchemaManager *fakeSchemaManager) {},
		},
		{
			name:  "EmptyActivityStatus",
			class: mtEnabledClass.Class,
			updateTenants: []*models.Tenant{
				{Name: tenants[0].Name},
				{Name: tenants[1].Name, ActivityStatus: ""},
			},
			errMsgs:         []string{"invalid activity status"},
			expectedTenants: tenants,
			mockCalls:       func(fakeSchemaManager *fakeSchemaManager) {},
		},
		{
			name:  "Success",
			class: mtEnabledClass.Class,
			updateTenants: []*models.Tenant{
				{Name: tenants[0].Name, ActivityStatus: models.TenantActivityStatusCOLD},
				{Name: tenants[1].Name, ActivityStatus: models.TenantActivityStatusHOT},
			},
			errMsgs: []string{},
			expectedTenants: []*models.Tenant{
				{Name: tenants[0].Name, ActivityStatus: models.TenantActivityStatusCOLD},
				{Name: tenants[1].Name, ActivityStatus: models.TenantActivityStatusHOT},
			},
			mockCalls: func(fakeMetaHandler *fakeSchemaManager) {
				fakeMetaHandler.On("UpdateTenants", mock.Anything, mock.Anything).Return(uint64(0), nil)
				fakeMetaHandler.On("TenantsShardsWithVersion", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					map[string]string{"USER1": models.TenantActivityStatusCOLD, "USER2": models.TenantActivityStatusHOT}, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Isolate schema for each tests
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
			test.mockCalls(fakeSchemaManager)

			_, err := handler.UpdateTenants(ctx, nil, test.class, test.updateTenants)
			if len(test.errMsgs) == 0 {
				require.NoError(t, err)
			} else {
				for i := range test.errMsgs {
					assert.ErrorContains(t, err, test.errMsgs[i])
				}
			}

			fakeSchemaManager.AssertExpectations(t)
		})
	}
}

func TestDeleteTenants(t *testing.T) {
	var (
		ctx     = context.Background()
		tenants = []*models.Tenant{
			{Name: "USER1"},
			{Name: "USER2"},
			{Name: "USER3"},
			{Name: "USER4"},
		}
		properties = []*models.Property{
			{
				Name:     "uuid",
				DataType: schema.DataTypeText.PropString(),
			},
		}
		repConfig = &models.ReplicationConfig{Factor: 1}
	)

	mtNilClass := &models.Class{
		Class:              "MTnil",
		MultiTenancyConfig: nil,
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}
	mtDisabledClass := &models.Class{
		Class:              "MTdisabled",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}
	mtEnabledClass := &models.Class{
		Class:              "MTenabled",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}

	type test struct {
		name            string
		class           string
		tenants         []*models.Tenant
		errMsgs         []string
		expectedTenants []*models.Tenant
		mockCalls       func(fakeSchemaManager *fakeSchemaManager)
	}

	tests := []test{
		{
			name:            "MTIsNil",
			class:           mtNilClass.Class,
			tenants:         tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {
				fakeSchemaManager.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:            "MTDisabled",
			class:           mtDisabledClass.Class,
			tenants:         tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {
				fakeSchemaManager.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:            "UnknownClass",
			class:           "UnknownClass",
			tenants:         tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {
				fakeSchemaManager.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:  "EmptyTenantValue",
			class: mtEnabledClass.Class,
			tenants: []*models.Tenant{
				{Name: "Aaaa"},
				{Name: ""},
				{Name: "Bbbb"},
			},
			errMsgs:         []string{"empty tenant name at index 1"},
			expectedTenants: tenants,
			mockCalls:       func(fakeSchemaManager *fakeSchemaManager) {},
		},
		{
			name:            "Success",
			class:           mtEnabledClass.Class,
			tenants:         tenants[:2],
			errMsgs:         []string{},
			expectedTenants: tenants[2:],
			mockCalls: func(fakeSchemaManager *fakeSchemaManager) {
				fakeSchemaManager.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Isolate schema for each tests
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
			test.mockCalls(fakeSchemaManager)

			tenantNames := make([]string, len(test.tenants))
			for i := range test.tenants {
				tenantNames[i] = test.tenants[i].Name
			}

			err := handler.DeleteTenants(ctx, nil, test.class, tenantNames)
			if len(test.errMsgs) == 0 {
				require.NoError(t, err)
			} else {
				for i := range test.errMsgs {
					assert.ErrorContains(t, err, test.errMsgs[i])
				}
			}
		})
	}
}

func TestGetConsistentTenants_WithAlias(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("get tenants via alias - alias resolves to existing class", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

		className := "RealClass"
		aliasName := "TestAlias"
		expectedTenants := []*models.Tenant{
			{Name: "tenant1"},
			{Name: "tenant2"},
		}

		// Mock the tenant retrieval with the resolved class name
		fakeSchemaManager.On("QueryTenants", className, mock.Anything).Return(expectedTenants, uint64(0), nil)

		// Create a custom fakeSchemaManager with alias support
		fakeSchemaManagerWithAlias := &fakeSchemaManagerWithAlias{
			fakeSchemaManager: fakeSchemaManager,
			aliasMap:          map[string]string{aliasName: className},
		}
		handler.schemaReader = fakeSchemaManagerWithAlias

		tenants, err := handler.GetConsistentTenants(ctx, nil, aliasName, true, nil)
		require.NoError(t, err)
		assert.Equal(t, expectedTenants, tenants)
		fakeSchemaManager.AssertExpectations(t)
	})

	t.Run("get tenants via alias - alias resolves to empty (fallback to direct name)", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

		aliasName := "NonExistentAlias"
		expectedTenants := []*models.Tenant{
			{Name: "tenant1"},
		}

		// Mock the tenant retrieval with the alias name (will be called since alias doesn't resolve)
		fakeSchemaManager.On("QueryTenants", aliasName, mock.Anything).Return(expectedTenants, uint64(0), nil)

		// Create a custom fakeSchemaManager with empty alias resolution
		fakeSchemaManagerWithAlias := &fakeSchemaManagerWithAlias{
			fakeSchemaManager: fakeSchemaManager,
			aliasMap:          map[string]string{}, // empty map
		}
		handler.schemaReader = fakeSchemaManagerWithAlias

		tenants, err := handler.GetConsistentTenants(ctx, nil, aliasName, true, nil)
		require.NoError(t, err)
		assert.Equal(t, expectedTenants, tenants)
		fakeSchemaManager.AssertExpectations(t)
	})

	t.Run("get tenants via direct class name - no alias resolution needed", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

		className := "RealClass"
		expectedTenants := []*models.Tenant{
			{Name: "tenant1"},
			{Name: "tenant2"},
		}

		// Mock the direct tenant retrieval
		fakeSchemaManager.On("QueryTenants", className, mock.Anything).Return(expectedTenants, uint64(0), nil)

		// Create a custom fakeSchemaManager (alias resolution returns empty for direct class names)
		fakeSchemaManagerWithAlias := &fakeSchemaManagerWithAlias{
			fakeSchemaManager: fakeSchemaManager,
			aliasMap:          map[string]string{}, // empty map
		}
		handler.schemaReader = fakeSchemaManagerWithAlias

		tenants, err := handler.GetConsistentTenants(ctx, nil, className, true, nil)
		require.NoError(t, err)
		assert.Equal(t, expectedTenants, tenants)
		fakeSchemaManager.AssertExpectations(t)
	})
}

// autoActivateSM wraps fakeSchemaManager with a proper QueryTenantsShards
// that returns configurable tenant statuses, needed to trigger auto-activation.
type autoActivateSM struct {
	*fakeSchemaManager
	tenantStatuses map[string]string
}

func (a *autoActivateSM) QueryTenantsShards(class string, tenants ...string) (map[string]string, uint64, error) {
	result := make(map[string]string, len(tenants))
	for _, t := range tenants {
		if s, ok := a.tenantStatuses[t]; ok {
			result[t] = s
		} else {
			result[t] = models.TenantActivityStatusHOT
		}
	}
	return result, 0, nil
}

// TestAutoTenantActivation_TransitionalStateRejected verifies that when a request
// triggers auto-activation of a tenant that is currently mid-freeze or mid-unfreeze,
// the activation fails and the error propagates back to the original caller.
//
// This guards against the data-loss race where an HOT request (via auto-activation)
// would trigger UNFREEZE while the FREEZE goroutine is still running.
func TestAutoTenantActivation_TransitionalStateRejected(t *testing.T) {
	ctx := context.Background()
	const (
		className  = "TestClass"
		tenantName = "tenant1"
	)

	// Build a class with AutoTenantActivation enabled
	autoActClass := &models.Class{
		Class: className,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
		},
	}

	t.Run("activateTenantIfInactive propagates ErrTenantTransitionalState", func(t *testing.T) {
		// This is the core of the bug: auto-activation calls UpdateTenants(HOT) while
		// the tenant is FREEZING. The schema layer now returns ErrTenantTransitionalState
		// which must propagate back to the caller so the original request is rejected.
		sm := &fakeSchemaManager{}
		sm.On("UpdateTenants", className, mock.Anything).Return(
			uint64(0),
			fmt.Errorf("%w: mid-freeze", clusterSchema.ErrTenantTransitionalState),
		)

		m := &Manager{
			Handler: Handler{
				schemaManager: sm,
			},
		}

		// Tenant is in FREEZING (mid-freeze), so activateTenantIfInactive will call UpdateTenants
		status := map[string]string{tenantName: models.TenantActivityStatusFREEZING}
		_, _, err := m.activateTenantIfInactive(ctx, className, status)

		require.Error(t, err)
		require.ErrorIs(t, err, clusterSchema.ErrTenantTransitionalState)
		sm.AssertExpectations(t)
	})

	t.Run("TenantsShardsWithVersion auto-activation rejects transitional tenant", func(t *testing.T) {
		// Full end-to-end path: TenantsShardsWithVersion → AllowImplicitTenantActivation
		// → activateTenantIfInactive → UpdateTenants → ErrTenantTransitionalState.
		sm := &fakeSchemaManager{}

		// Read is called by AllowImplicitTenantActivation; invoke the callback with a
		// class that has AutoTenantActivation enabled.
		sm.On("Read", className, mock.Anything).
			Run(func(args mock.Arguments) {
				reader := args.Get(1).(func(*models.Class, *sharding.State) error)
				_ = reader(autoActClass, nil)
			}).
			Return(nil)

		// UpdateTenants is called by activateTenantIfInactive with the HOT request.
		sm.On("UpdateTenants", className, mock.Anything).Return(
			uint64(0),
			fmt.Errorf("%w: mid-freeze", clusterSchema.ErrTenantTransitionalState),
		)

		// Wrap with autoActivateSM so QueryTenantsShards returns FREEZING (mid-freeze),
		// which triggers auto-activation and matches the transitional-state scenario.
		asm := &autoActivateSM{
			fakeSchemaManager: sm,
			tenantStatuses:    map[string]string{tenantName: models.TenantActivityStatusFREEZING},
		}

		m := &Manager{
			Handler: Handler{
				schemaManager: asm,
				schemaReader:  sm,
			},
		}

		_, _, err := m.TenantsShardsWithVersion(ctx, className, tenantName)

		require.Error(t, err)
		require.ErrorIs(t, err, clusterSchema.ErrTenantTransitionalState)
		sm.AssertExpectations(t)
	})
}
