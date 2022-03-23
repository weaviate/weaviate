//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Dump to stdout for debugging purposes
func (index *hnsw) Dump(labels ...string) {
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
	for _, node := range index.nodes {
		if node == nil {
			continue
		}

		fmt.Printf("  Node %d (level %d)\n", node.id, node.level)
		for level, conns := range node.connections {
			fmt.Printf("    Level %d: Connections: %v\n", level, conns)
		}
	}

	fmt.Printf("--------------------------------------------------\n")
}

// DumpJSON to stdout for debugging purposes
func (index *hnsw) DumpJSON(labels ...string) {
	dump := JSONDump{
		Labels:              labels,
		ID:                  index.id,
		Entrypoint:          index.entryPointID,
		CurrentMaximumLayer: index.currentMaximumLayer,
		Tombstones:          index.tombstones,
	}
	for _, node := range index.nodes {
		if node == nil {
			continue
		}

		dumpNode := JSONDumpNode{
			ID:          node.id,
			Level:       node.level,
			Connections: node.connections,
		}
		dump.Nodes = append(dump.Nodes, dumpNode)
	}

	out, err := json.Marshal(dump)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%s\n", string(out))
}

type JSONDump struct {
	Labels              []string            `json:"labels"`
	ID                  string              `json:"id"`
	Entrypoint          uint64              `json:"entrypoint"`
	CurrentMaximumLayer int                 `json:"currentMaximumLayer"`
	Tombstones          map[uint64]struct{} `json:"tombstones"`
	Nodes               []JSONDumpNode      `json:"nodes"`
}

type JSONDumpNode struct {
	ID          uint64           `json:"id"`
	Level       int              `json:"level"`
	Connections map[int][]uint64 `json:"connections"`
}

func NewFromJSONDump(dumpBytes []byte, vecForID VectorForID) (*hnsw, error) {
	var dump JSONDump
	err := json.Unmarshal(dumpBytes, &dump)
	if err != nil {
		return nil, err
	}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    dump.ID,
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineProvider(),
		VectorForIDThunk:      vecForID,
	}, UserConfig{
		MaxConnections: 30,
		EFConstruction: 128,
	})
	if err != nil {
		return nil, err
	}

	index.currentMaximumLayer = dump.CurrentMaximumLayer
	index.entryPointID = dump.Entrypoint
	index.tombstones = dump.Tombstones

	for _, n := range dump.Nodes {
		index.nodes[n.ID] = &vertex{
			id:          n.ID,
			level:       n.Level,
			connections: n.Connections,
		}
	}

	return index, nil
}

// was added as part of
// https://github.com/semi-technologies/weaviate/issues/1868 for debugging. It
// is not currently in use anywhere as it is somewhat costly, it would lock the
// entire graph and iterate over every node which would lead to disruptions in
// production. However, keeping this method around may be valuable for future
// investigations where the amount of links may be a problem.
func (h *hnsw) ValidateLinkIntegrity() {
	h.Lock()
	defer h.Unlock()

	for i, node := range h.nodes {
		if node == nil {
			continue
		}

		for level, conns := range node.connections {
			m := h.maximumConnections
			if level == 0 {
				m = h.maximumConnectionsLayerZero
			}

			if len(conns) > m {
				h.logger.Warnf("node %d at level %d has %d connections", i, level, len(conns))
			}

		}
	}

	h.logger.Infof("completed link integrity check")
}
