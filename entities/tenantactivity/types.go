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

package tenantactivity

import "time"

type ByCollection map[string]ByTenant

type ByTenant map[string]time.Time

type UsageFilter int

const (
	UsageFilterAll UsageFilter = iota
	UsageFilterOnlyReads
	UsageFilterOnlyWrites
)
