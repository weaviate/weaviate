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

import "context"

// ReplicaCopier see cluster/replication/copier.Copier
type ReplicaCopier interface {
	// CopyReplica see cluster/replication/copier.Copier.CopyReplica
	CopyReplica(ctx context.Context, sourceNode string, sourceCollection string, sourceShard string) error
}
