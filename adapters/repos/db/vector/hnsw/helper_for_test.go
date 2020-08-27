package hnsw

import "fmt"

func dumpIndex(index *hnsw) {
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("ID: %s\n", index.id)
	fmt.Printf("Entrypoint: %d\n", index.entryPointID)
	fmt.Printf("Max Level: %d\n", index.currentMaximumLayer)
	fmt.Printf("Tombstones %v\n", index.tombstones)
	fmt.Printf("\nNodes and Connections:\n")
	for _, node := range index.nodes {
		if node == nil {
			continue
		}

		fmt.Printf("  Node %d\n", node.id)
		for level, conns := range node.connections {
			fmt.Printf("    Level %d: Connections: %v\n", level, conns)
		}
	}

	fmt.Printf("--------------------------------------------------\n")
}
