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

package types

type Router interface {
	GetReadWriteReplicasLocation(collection string, shard string) ([]string, []string, error)
	GetWriteReplicasLocation(collection string, shard string) ([]string, error)
	GetReadReplicasLocation(collection string, shard string) ([]string, error)
	BuildReadRoutingPlan(params RoutingPlanBuildOptions) (RoutingPlan, error)
	BuildWriteRoutingPlan(params RoutingPlanBuildOptions) (RoutingPlan, error)

	NodeHostname(nodeName string) (string, bool)
	AllHostnames() []string
}
