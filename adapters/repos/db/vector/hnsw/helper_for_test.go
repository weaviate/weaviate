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

package hnsw

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

func dumpIndex(index *hnsw, labels ...string) {
	if len(labels) > 0 {
		fmt.Printf("--------------------------------------------------\n")
		fmt.Printf("--  %s\n", strings.Join(labels, ", "))
	}
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("ID: %s\n", index.id)
	fmt.Printf("Entrypoint: %d\n", index.entryPointID)
	fmt.Printf("Max Level: %d\n", index.currentMaximumLayer)
	fmt.Printf("Tombstones %v\n", index.tombstones)
	fmt.Printf("\nNodes and Connections:\n")
	for i, node := range index.nodes {
		if node == nil {
			continue
		}

		fmt.Printf("  Node %d\n", i)
		iter := node.connections.Iterator()
		for iter.Next() {
			level, conns := iter.Current()
			fmt.Printf("    Level %d: Connections: %v\n", level, conns)
		}
	}

	fmt.Printf("--------------------------------------------------\n")
}

func getRandomSeed() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

// vertexAt builds a vertex at the given level, copying conns when not nil.
func vertexAt(level uint16, conns *packedconn.Connections) *vertex {
	v := &vertex{}
	if conns != nil {
		v.connections = *conns
	}
	v.connections.SetLevel(level)
	return v
}
