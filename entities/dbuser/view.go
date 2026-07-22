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

package dbuser

import "time"

// View is an immutable snapshot of a DB user. It is both the value callers
// read and the RAFT user-query response shape, so the two cannot drift apart.
// Field names carry the wire contract.
type View struct {
	Id                 string
	Active             bool
	InternalIdentifier string
	ApiKeyFirstLetters string
	CreatedAt          time.Time
	LastUsedAt         time.Time
	ImportedWithKey    bool
	Namespace          string
}
