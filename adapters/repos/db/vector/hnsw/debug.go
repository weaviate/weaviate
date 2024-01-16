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

package hnsw

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Dump to stdout for debugging purposes
func (h *hnsw) Dump(labels ...string) {
	if len(labels) > 0 {
		fmt.Printf("--------------------------------------------------\n")
		fmt.Printf("--  %s\n", strings.Join(labels, ", "))
	}
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("ID: %s\n", h.id)
	fmt.Printf("Entrypoint: %d\n", h.entryPointID)
	fmt.Printf("Max Level: %d\n", h.currentMaximumLayer)
	fmt.Printf("Tombstones %v\n", h.tombstones)
	fmt.Printf("\nNodes and Connections:\n")
	for _, node := range h.nodes {
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
func (h *hnsw) DumpJSON(labels ...string) {
	dump := JSONDump{
		Labels:              labels,
		ID:                  h.id,
		Entrypoint:          h.entryPointID,
		CurrentMaximumLayer: h.currentMaximumLayer,
		Tombstones:          h.tombstones,
	}
	for _, node := range h.nodes {
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
	ID          uint64     `json:"id"`
	Level       int        `json:"level"`
	Connections [][]uint64 `json:"connections"`
}

type JSONDumpMap struct {
	Labels              []string            `json:"labels"`
	ID                  string              `json:"id"`
	Entrypoint          uint64              `json:"entrypoint"`
	CurrentMaximumLayer int                 `json:"currentMaximumLayer"`
	Tombstones          map[uint64]struct{} `json:"tombstones"`
	Nodes               []JSONDumpNodeMap   `json:"nodes"`
}

type JSONDumpNodeMap struct {
	ID          uint64           `json:"id"`
	Level       int              `json:"level"`
	Connections map[int][]uint64 `json:"connections"`
}

func NewFromJSONDump(dumpBytes []byte, vecForID common.VectorForID[float32]) (*hnsw, error) {
	var dump JSONDump
	err := json.Unmarshal(dumpBytes, &dump)
	if err != nil {
		return nil, err
	}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    dump.ID,
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 128,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), nil)
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

func NewFromJSONDumpMap(dumpBytes []byte, vecForID common.VectorForID[float32]) (*hnsw, error) {
	var dump JSONDumpMap
	err := json.Unmarshal(dumpBytes, &dump)
	if err != nil {
		return nil, err
	}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    dump.ID,
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 128,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), nil)
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
			connections: make([][]uint64, len(n.Connections)),
		}
		for level, conns := range n.Connections {
			index.nodes[n.ID].connections[level] = conns
		}
	}

	return index, nil
}

// was added as part of
// https://github.com/weaviate/weaviate/issues/1868 for debugging. It
// is not currently in use anywhere as it is somewhat costly, it would lock the
// entire graph and iterate over every node which would lead to disruptions in
// production. However, keeping this method around may be valuable for future
// investigations where the amount of links may be a problem.
func (h *hnsw) ValidateLinkIntegrity() {
	h.RLock()
	defer h.RUnlock()

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
