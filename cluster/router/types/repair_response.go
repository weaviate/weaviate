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

package types

import "github.com/google/uuid"

type RepairResponse struct {
	ID         string // object id
	Version    int64  // sender's current version of the object
	UpdateTime int64  // sender's current update time
	Err        string
	Deleted    bool
}

// RepairDigest is the byte-ID digest used end-to-end on the async-replication
// compare/propagate pipeline; the string-ID RepairResponse survives only at
// JSON and proto boundaries. Field set mirrors the binary wire records.
type RepairDigest struct {
	ID         uuid.UUID
	UpdateTime int64
	Deleted    bool
}
