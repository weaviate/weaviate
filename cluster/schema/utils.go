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

package schema

import (
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func removeNilTenants(tenants []*cmd.Tenant) []*cmd.Tenant {
	n := 0
	for i := range tenants {
		if tenants[i] != nil && tenants[i].Name != "" {
			tenants[n] = tenants[i]
			n++
		}
	}
	return tenants[:n]
}
