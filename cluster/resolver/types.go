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

package resolver

import "github.com/weaviate/weaviate/usecases/cluster"

type RaftConfig struct {
	NodeResolver      cluster.NodeResolver
	RaftPort          int
	IsLocalHost       bool
	NodeNameToPortMap map[string]int
	LocalName         string
	LocalAddress      string
}
