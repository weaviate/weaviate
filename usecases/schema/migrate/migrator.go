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
package migrate

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Migrator represents both the input and output interface of the Composer
type Migrator interface {
	AddClass(ctx context.Context, class *models.Class, shardingState *sharding.State) error
	DropClass(ctx context.Context, className string) error
	UpdateClass(ctx context.Context, className string,
		newClassName *string) error
	GetShardsStatus(ctx context.Context, className string) (map[string]string, error)
	UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error
	AddProperty(ctx context.Context, className string,
		prop *models.Property) error
	UpdateProperty(ctx context.Context, className string,
		propName string, newName *string) error
	NewPartitions(ctx context.Context, class *models.Class, partitions []string) (commit func(success bool), err error)
	ValidateVectorIndexConfigUpdate(ctx context.Context,
		old, updated schema.VectorIndexConfig) error
	UpdateVectorIndexConfig(ctx context.Context, className string,
		updated schema.VectorIndexConfig) error
	ValidateInvertedIndexConfigUpdate(ctx context.Context,
		old, updated *models.InvertedIndexConfig) error
	UpdateInvertedIndexConfig(ctx context.Context, className string,
		updated *models.InvertedIndexConfig) error
	RecalculateVectorDimensions(ctx context.Context) error
	RecountProperties(ctx context.Context) error
	InvertedReindex(ctx context.Context, taskNames ...string) error
	AdjustFilterablePropSettings(ctx context.Context) error
}
