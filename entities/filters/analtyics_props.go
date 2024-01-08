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

package filters

// AnalyticsProps will be extracted from the graphql args of analytics
// functions (such as Meta and Aggregate). They tell the connectors whether
// to use an external analytics engine if such an engine is configured.
type AnalyticsProps struct {
	UseAnalyticsEngine bool
	ForceRecalculate   bool
}
