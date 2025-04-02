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

import "github.com/weaviate/weaviate/cluster/proto/api"

type ReplicationFSMReader interface {
	FilterOneShardReplicasReadWrite(collection string, shard string, shardReplicasLocation []string) ([]string, []string)
}

type ReplicationStatusProvider interface {
	GetReplicationDetailsByReplicationId(id uint64) (api.ReplicationDetailsResponse, error)
}
