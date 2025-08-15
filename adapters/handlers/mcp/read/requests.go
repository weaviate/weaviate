//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package read

type GetTenantsArgs struct {
	Collection string   `json:"collection" jsonschema_description:"Name of collection to get tenants from"`
	Tenants    []string `json:"tenants,omitempty" jsonschema_description:"Names of tenants to get"`
}
