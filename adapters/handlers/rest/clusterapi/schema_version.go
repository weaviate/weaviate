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

package clusterapi

import (
	"net/url"
	"strconv"

	"github.com/weaviate/weaviate/usecases/replica"
)

func extractSchemaVersionFromUrlQuery(values url.Values) uint64 {
	var schemaVersion uint64
	if v := values.Get(replica.SchemaVersionKey); v != "" {
		if vAsUint64, err := strconv.ParseUint(v, 10, 64); err != nil {
			schemaVersion = vAsUint64
		}
	}
	return schemaVersion
}
