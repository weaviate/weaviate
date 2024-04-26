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
	"fmt"
	"net/url"
	"strconv"

	"github.com/weaviate/weaviate/usecases/replica"
)

func extractSchemaVersionFromUrlQuery(values url.Values) (uint64, error) {
	if v := values.Get(replica.SchemaVersionKey); v != "" {
		schemaVersion, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %q is an invalid value for %s", err, v, replica.SchemaVersionKey)
		}

		return schemaVersion, nil
	}
	return 0, nil
}
