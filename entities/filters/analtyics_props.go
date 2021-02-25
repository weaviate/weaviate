//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package filters

// AnalyticsProps will be extracted from the graphql args of analytics
// functions (such as Meta and Aggregate). They tell the connectors whether
// to use an external analytics engine if such an engine is configured.
type AnalyticsProps struct {
	UseAnaltyicsEngine bool
	ForceRecalculate   bool
}
