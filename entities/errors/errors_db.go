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

package errors

import "errors"

// ErrShardBusyStructuralOp marks a replica-transfer refusal the movement waits out instead of counting (see replication.IsReversibleRefusal).
var ErrShardBusyStructuralOp = errors.New("shard busy: structural vector op in progress")
