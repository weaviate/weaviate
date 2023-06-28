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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestAddTenants(t *testing.T) {
	var (
		ctx        = context.Background()
		mt         = &models.MultiTenancyConfig{Enabled: true}
		tenants    = []*models.Tenant{{Name: "USER1"}, {Name: "USER2"}}
		cls        = "C1"
		properties = []*models.Property{
			{
				Name:     "uUID",
				DataType: schema.DataTypeText.PropString(),
			},
		}
		repConfig = &models.ReplicationConfig{Factor: 1}
	)

	type test struct {
		name    string
		Class   string
		tenants []*models.Tenant
		initial *models.Class
		errMsg  string
	}
	tests := []test{
		{
			name:    "UnknownClass",
			Class:   "UnknownClass",
			tenants: tenants,
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: mt,
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: ErrNotFound.Error(),
		},
		{
			name:    "MTIsNil",
			Class:   "C1",
			tenants: tenants,
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: nil,
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "not enabled",
		},
		{
			name:    "MTDisabled",
			Class:   "C1",
			tenants: tenants,
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "not enabled",
		},
		{
			name:    "EmptyTenantKeyValue",
			Class:   "C1",
			tenants: []*models.Tenant{{Name: "A"}, {Name: ""}, {Name: "B"}},
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "tenant",
		},
		{
			name:    "Success",
			Class:   "C1",
			tenants: []*models.Tenant{{Name: "A"}, {Name: "B"}},
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "",
		},
		// TODO test with replication factor >= 2
	}

	// AddTenants
	for _, test := range tests {
		sm := newSchemaManager()
		err := sm.AddClass(ctx, nil, test.initial)
		if err == nil {
			err = sm.AddTenants(ctx, nil, test.Class, test.tenants)
		}
		if test.errMsg == "" {
			assert.Nil(t, err)
			ss := sm.state.ShardingState[test.Class]
			assert.NotNil(t, ss, test.name)
			assert.Equal(t, len(ss.Physical), len(test.tenants), test.name)
		} else {
			assert.ErrorContains(t, err, test.errMsg, test.name)
		}

	}
}

func TestDeleteTenants(t *testing.T) {
	var (
		ctx     = context.Background()
		mt      = &models.MultiTenancyConfig{Enabled: true}
		tenants = []*models.Tenant{
			{Name: "USER1"},
			{Name: "USER2"},
			{Name: "USER3"},
			{Name: "USER4"},
		}
		cls        = "C1"
		properties = []*models.Property{
			{
				Name:     "uUID",
				DataType: schema.DataTypeText.PropString(),
			},
		}
		repConfig = &models.ReplicationConfig{Factor: 1}
	)

	type test struct {
		name       string
		Class      string
		tenants    []*models.Tenant
		initial    *models.Class
		errMsg     string
		addTenants bool
	}
	tests := []test{
		{
			name:    "UnknownClass",
			Class:   "UnknownClass",
			tenants: tenants,
			initial: &models.Class{
				Class: cls, MultiTenancyConfig: mt,
				Properties:        properties,
				ReplicationConfig: repConfig,
			},
			errMsg: ErrNotFound.Error(),
		},
		{
			name:    "MTIsNil",
			Class:   "C1",
			tenants: tenants,
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: nil,
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "not enabled",
		},
		{
			name:    "MTDisabled",
			Class:   "C1",
			tenants: tenants,
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "not enabled",
		},
		{
			name:    "EmptyTenantKeyValue",
			Class:   "C1",
			tenants: []*models.Tenant{{Name: "A"}, {Name: ""}, {Name: "B"}},
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "tenant",
		},
		{
			name:    "Success",
			Class:   "C1",
			tenants: tenants[:2],
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg:     "",
			addTenants: true,
		},
	}

	for _, test := range tests {
		sm := newSchemaManager()
		err := sm.AddClass(ctx, nil, test.initial)
		if err != nil {
			t.Fatalf("%s: add class: %v", test.name, err)
		}
		if test.addTenants {
			err = sm.AddTenants(ctx, nil, test.Class, tenants)
			if err != nil {
				t.Fatalf("%s: add tenants: %v", test.name, err)
			}
		}
		err = sm.DeleteTenants(ctx, nil, test.Class, test.tenants)
		if test.errMsg == "" {
			if err != nil {
				t.Fatalf("%s: remove tenants: %v", test.name, err)
			}
			ss := sm.state.ShardingState[test.Class]
			if ss == nil {
				t.Fatalf("%s: sharding state equal nil", test.name)
			}
			assert.Equal(t, len(test.tenants)+len(ss.Physical), len(tenants))
		} else {
			assert.ErrorContains(t, err, test.errMsg, test.name)
		}

	}
}
