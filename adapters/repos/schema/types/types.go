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

package types

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// SchemaRepo is implemented by the schema store and is used to retrieve the schema from the disk
type SchemaRepo interface {
	Load(context.Context) (SchemaStateGetter, error)
}

// SchemaStateGetter is the interface that the schema state must satify in order to migrate from non-raft to raft based
// representation. We need to use an interface to avoid import cycle between schema usecase and cloud folder
type SchemaStateGetter interface {
	GetSchema() *models.Schema
	GetShardingState() map[string]*sharding.State
}
