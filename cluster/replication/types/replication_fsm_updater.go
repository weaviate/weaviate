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

	"github.com/weaviate/weaviate/cluster/proto/api"
)

type FSMUpdater interface {
	AddReplicaToShard(context.Context, string, string, string) (uint64, error)
	ReplicationUpdateReplicaOpStatus(id uint64, state api.ShardReplicationState) error
}
