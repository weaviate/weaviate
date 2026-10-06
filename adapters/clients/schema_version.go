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

package clients

import (
	"net/url"
	"strconv"
	"sync/atomic"

	"github.com/weaviate/weaviate/usecases/replica"
)

// SchemaVersionProvider reports the local schema version for a class. Reads send it so the
// receiving node can tell its own schema lag from data it genuinely does not hold; see
// entities/errors.NotServedHere.
type SchemaVersionProvider func(class string) uint64

// schemaVersionSource is embedded by the cluster clients. It is set once at startup, after the
// schema reader exists, which is later than the clients are built; until then, and in tests that
// build a client directly, reads send version 0 and the receiver treats every miss as lag.
type schemaVersionSource struct {
	provider atomic.Pointer[SchemaVersionProvider]
}

// SetSchemaVersionProvider is safe to call once, during startup wiring.
func (s *schemaVersionSource) SetSchemaVersionProvider(p SchemaVersionProvider) {
	if p == nil {
		return
	}
	s.provider.Store(&p)
}

// schemaVersion reports the version to send for a class, or 0 when none is wired.
func (s *schemaVersionSource) schemaVersion(class string) uint64 {
	if p := s.provider.Load(); p != nil {
		return (*p)(class)
	}
	return 0
}

// schemaVersionQuery encodes the schema version a read was resolved against.
func (s *schemaVersionSource) schemaVersionQuery(class string) string {
	return url.Values{
		replica.SchemaVersionKey: []string{strconv.FormatUint(s.schemaVersion(class), 10)},
	}.Encode()
}
