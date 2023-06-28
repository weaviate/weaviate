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
		mt         = &models.MultiTenancyConfig{Enabled: true, TenantKey: "uUID"}
		tenants    = []*models.Tenant{{"USER1"}, {"USER2"}}
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
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false, TenantKey: "uUID"},
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "not enabled",
		},
		{
			name:    "EmptyTenantKeyValue",
			Class:   "C1",
			tenants: []*models.Tenant{{"A"}, {""}, {"B"}},
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true, TenantKey: "uUID"},
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "tenant",
		},
		{
			name:    "Success",
			Class:   "C1",
			tenants: []*models.Tenant{{"A"}, {"B"}},
			initial: &models.Class{
				Class:              cls,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true, TenantKey: "uUID"},
				Properties:         properties,
				ReplicationConfig:  repConfig,
			},
			errMsg: "",
		},
		// TODO test with replication factor >= 2
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sm := newSchemaManager()
			err := sm.AddClass(ctx, nil, test.initial)
			if err == nil {
				err = sm.AddTenants(ctx, nil, test.Class, test.tenants)
			}
			if test.errMsg == "" {
				assert.Nil(t, err)
			} else {
				assert.ErrorContains(t, err, test.errMsg)
			}
		})
	}
}
