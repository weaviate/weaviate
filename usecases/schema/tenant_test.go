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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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
		mockCalls func(fakeMetaHandler *fakeMetaHandler)
	}

	tests := []test{
		{
			name:    "MTIsNil",
			class:   mtNilClass.Class,
			tenants: tenants,
			errMsgs: nil,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				// MT validation is done leader side now
				fakeMetaHandler.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:    "MTDisabled",
			class:   mtDisabledClass.Class,
			tenants: tenants,
			errMsgs: nil,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				// MT validation is done leader side now
				fakeMetaHandler.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:    "UnknownClass",
			class:   "UnknownClass",
			tenants: tenants,
			errMsgs: nil,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
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
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {},
		},
		{
			name:  "InvalidActivityStatus",
			class: mtEnabledClass.Class,
			tenants: []*models.Tenant{
				{Name: "Aaaa", ActivityStatus: "DOES_NOT_EXIST_1"},
				{Name: "Bbbb", ActivityStatus: "DOES_NOT_EXIST_2"},
				{Name: "Bbbb", ActivityStatus: "WARM"},
			},
			errMsgs: []string{
				"invalid activity status",
				"DOES_NOT_EXIST_1",
				"DOES_NOT_EXIST_2",
			},
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {},
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
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		// TODO test with replication factor >= 2
	}

	// AddTenants
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Isolate schema for each tests
			handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})

			test.mockCalls(fakeMetaHandler)

			_, err := handler.AddTenants(ctx, nil, test.class, test.tenants)

			fakeMetaHandler.AssertExpectations(t)

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
			{Name: "USER2", ActivityStatus: models.TenantActivityStatusHOT},
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
		mockCalls       func(fakeMetaHandler *fakeMetaHandler)
	}

	tests := []test{
		{
			name:            "MTIsNil",
			class:           mtNilClass.Class,
			updateTenants:   tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("UpdateTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:            "MTDisabled",
			class:           mtDisabledClass.Class,
			updateTenants:   tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("UpdateTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:            "UnknownClass",
			class:           "UnknownClass",
			updateTenants:   tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("UpdateTenants", mock.Anything, mock.Anything).Return(nil)
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
			mockCalls:       func(fakeMetaHandler *fakeMetaHandler) {},
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
			mockCalls:       func(fakeMetaHandler *fakeMetaHandler) {},
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
			mockCalls:       func(fakeMetaHandler *fakeMetaHandler) {},
		},
		{
			name:  "Success",
			class: mtEnabledClass.Class,
			updateTenants: []*models.Tenant{
				{Name: tenants[0].Name, ActivityStatus: models.TenantActivityStatusCOLD},
				{Name: tenants[1].Name, ActivityStatus: models.TenantActivityStatusHOT},
				{Name: tenants[0].Name, ActivityStatus: models.TenantActivityStatusFROZEN},
			},
			errMsgs: []string{},
			expectedTenants: []*models.Tenant{
				{Name: tenants[0].Name, ActivityStatus: models.TenantActivityStatusCOLD},
				{Name: tenants[1].Name, ActivityStatus: models.TenantActivityStatusHOT},
				{Name: tenants[0].Name, ActivityStatus: models.TenantActivityStatusFROZEN},
			},
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("UpdateTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Isolate schema for each tests
			handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
			test.mockCalls(fakeMetaHandler)

			err := handler.UpdateTenants(ctx, nil, test.class, test.updateTenants)
			if len(test.errMsgs) == 0 {
				require.NoError(t, err)
			} else {
				for i := range test.errMsgs {
					assert.ErrorContains(t, err, test.errMsgs[i])
				}
			}

			fakeMetaHandler.AssertExpectations(t)
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
		mockCalls       func(fakeMetaHandler *fakeMetaHandler)
	}

	tests := []test{
		{
			name:            "MTIsNil",
			class:           mtNilClass.Class,
			tenants:         tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:            "MTDisabled",
			class:           mtDisabledClass.Class,
			tenants:         tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			name:            "UnknownClass",
			class:           "UnknownClass",
			tenants:         tenants,
			errMsgs:         nil,
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
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
			mockCalls:       func(fakeMetaHandler *fakeMetaHandler) {},
		},
		{
			name:            "Success",
			class:           mtEnabledClass.Class,
			tenants:         tenants[:2],
			errMsgs:         []string{},
			expectedTenants: tenants[2:],
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Isolate schema for each tests
			handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
			test.mockCalls(fakeMetaHandler)

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

func TestGetConsistentTenants(t *testing.T) {
	var (
		ctx     = context.Background()
		tenants = []*models.Tenant{
			{Name: "USER1", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER2", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER3", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER4", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER5", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER6", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER7", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER8", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER9", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER10", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER11", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER12", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "USER13", ActivityStatus: models.TenantActivityStatusHOT},
		}
		after      = "USER5"
		limit      = int64(5)
		properties = []*models.Property{
			{
				Name:     "uuid",
				DataType: schema.DataTypeText.PropString(),
			},
		}
		repConfig   = &models.ReplicationConfig{Factor: 1}
		testTenants = []string{"USER1", "USER2", "USER3", "USER4", "USER5", "USER6", "USER7", "USER8", "USER9", "USER10", "USER11", "USER13", "USER13"}
	)

	mtEnabledClass := &models.Class{
		Class:              "MTenabled",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
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

	unknownClass := &models.Class{
		Class:              "UnknownClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}

	tests := []struct {
		name            string
		class           *models.Class
		consistency     bool
		tenants         []string
		afterTenant     *string
		limitTenant     *int64
		expectedTenants []*models.Tenant
		errMsgs         []string
		mockCalls       func(fakeMetaHandler *fakeMetaHandler)
	}{
		{
			name:            "Failure - MTdisabled",
			class:           mtDisabledClass,
			consistency:     true,
			tenants:         []string{"USER1", "USER2"},
			afterTenant:     nil,
			limitTenant:     nil,
			expectedTenants: nil,
			errMsgs:         []string{"not enabled"},
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("QueryTenants", mock.Anything, mock.Anything, mock.Anything).Return(nil, uint64(1), errors.New("multi-tenancy not enabled"))
			},
		},
		{
			name:            "Failure - UnknownClass",
			class:           unknownClass,
			consistency:     true,
			tenants:         []string{"USER1", "USER2"},
			afterTenant:     nil,
			limitTenant:     nil,
			expectedTenants: nil,
			errMsgs:         []string{ErrNotFound.Error()},
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("QueryTenants", mock.Anything, mock.Anything, mock.Anything).Return(nil, uint64(1), ErrNotFound)
			},
		},
		{
			name:            "Success - MTenabled",
			class:           mtEnabledClass,
			consistency:     true,
			tenants:         testTenants,
			afterTenant:     &after,
			limitTenant:     nil,
			expectedTenants: tenants[6:],
			errMsgs:         []string{},
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("QueryTenants", mock.Anything, mock.Anything, mock.Anything).Return(tenants[6:], uint64(1), nil)
			},
		},
		{
			name:            "Success - MTenabledWithAfterAndLimit",
			class:           mtEnabledClass,
			consistency:     true,
			tenants:         testTenants,
			afterTenant:     &after,
			limitTenant:     &limit,
			expectedTenants: tenants[6:],
			errMsgs:         []string{},
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("QueryTenants", mock.Anything, mock.Anything, mock.Anything).Return(tenants[6:], uint64(1), nil)
			},
		},
		{
			name:            "Success - TestNoAfterAndLimit",
			class:           mtEnabledClass,
			consistency:     true,
			tenants:         testTenants,
			afterTenant:     nil,
			limitTenant:     nil,
			expectedTenants: tenants,
			errMsgs:         []string{},
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("QueryTenants", mock.Anything, mock.Anything, mock.Anything).Return(tenants, uint64(len(tenants)), nil)
			},
		},
		{
			name:            "Success - TestAfterOnly",
			class:           mtEnabledClass,
			consistency:     true,
			tenants:         testTenants,
			afterTenant:     &after,
			limitTenant:     nil,
			expectedTenants: tenants[2:],
			errMsgs:         []string{},
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("QueryTenants", mock.Anything, mock.Anything, mock.Anything).Return(tenants[2:], uint64(1), nil)
			},
		},
		{
			name:            "Success - TestLimitOnly",
			class:           mtEnabledClass,
			consistency:     true,
			tenants:         testTenants,
			afterTenant:     nil,
			limitTenant:     &limit,
			expectedTenants: tenants[:5],
			errMsgs:         []string{},
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("QueryTenants", mock.Anything, mock.Anything, mock.Anything).Return(tenants[:5], uint64(1), nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
			test.mockCalls(fakeMetaHandler)

			tenants, err := handler.GetConsistentTenants(ctx, nil, test.class.Class, test.consistency, test.tenants, test.afterTenant, test.limitTenant)

			fakeMetaHandler.AssertExpectations(t)

			assert.Equal(t, test.expectedTenants, tenants)
			if len(test.errMsgs) == 0 {
				assert.NoError(t, err)
			} else {
				for _, errMsg := range test.errMsgs {
					assert.Contains(t, err.Error(), errMsg)
				}
			}
		})
	}
}
