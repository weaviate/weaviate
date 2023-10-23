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

// Package migrate provides a simple composer tool, which implements the
// Migrator interface and can take in any number of migrators which themselves
// have to implement the interface
package schema

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type CreateTenantPayload struct {
	Name   string
	Status string
}

type UpdateTenantPayload struct {
	Name   string
	Status string
}

// Migrator represents both the input and output interface of the Composer
type Migrator interface {
	AddClass(ctx context.Context, class *models.Class, shardingState *sharding.State) error
	DropClass(ctx context.Context, className string) error
	UpdateClass(ctx context.Context, className string,
		newClassName *string) error
	GetShardsQueueSize(ctx context.Context, className, tenant string) (map[string]int64, error)

	AddProperty(ctx context.Context, className string,
		prop *models.Property) error
	UpdateProperty(ctx context.Context, className string,
		propName string, newName *string) error

	NewTenants(ctx context.Context, class *models.Class, creates []*CreateTenantPayload) (commit func(success bool), err error)
	UpdateTenants(ctx context.Context, class *models.Class, updates []*UpdateTenantPayload) (commit func(success bool), err error)
	DeleteTenants(ctx context.Context, class string, tenants []string) (commit func(success bool), err error)

	GetShardsStatus(ctx context.Context, className, tenant string) (map[string]string, error)
	UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error

	// GetShardsStatus(ctx context.Context, className string) (map[string]string, error)
	// UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error

	ValidateVectorIndexConfigUpdate(ctx context.Context,
		old, updated schemaConfig.VectorIndexConfig) error
	UpdateVectorIndexConfig(ctx context.Context, className string,
		updated schemaConfig.VectorIndexConfig) error
	ValidateInvertedIndexConfigUpdate(ctx context.Context,
		old, updated *models.InvertedIndexConfig) error
	UpdateInvertedIndexConfig(ctx context.Context, className string,
		updated *models.InvertedIndexConfig) error
}
