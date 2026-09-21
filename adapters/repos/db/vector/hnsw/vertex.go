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
	"sync"

	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

type vertex struct {
	// connections first so the GC scans only the pointer-bearing prefix; the
	// narrow fields at the tail keep the struct in the 64 B size class, one
	// allocation per node
	connections packedconn.Connections
	sync.Mutex
	level       uint16
	maintenance bool
}

func (v *vertex) markAsMaintenance() {
	v.Lock()
	v.maintenance = true
	v.Unlock()
}

func (v *vertex) unmarkAsMaintenance() {
	v.Lock()
	v.maintenance = false
	v.Unlock()
}

func (v *vertex) isUnderMaintenance() bool {
	v.Lock()
	m := v.maintenance
	v.Unlock()
	return m
}

func (v *vertex) connectionsAtLevelNoLock(level int) []uint64 {
	return v.connections.GetLayer(uint8(level))
}

func (v *vertex) upgradeToLevelNoLock(level int) {
	v.level = uint16(level)
	v.connections.GrowLayersTo(uint8(level))
}

func (v *vertex) setConnectionsAtLevel(level int, connections []uint64) {
	v.Lock()
	defer v.Unlock()
	v.connections.ReplaceLayer(uint8(level), connections)
}

func (v *vertex) appendConnectionAtLevelNoLock(level int, connection uint64, maxConns int) {
	v.connections.InsertAtLayer(connection, uint8(level))
}

func (v *vertex) appendConnectionsAtLevelNoLock(level int, connections []uint64, maxConns int) {
	v.connections.BulkInsertAtLayer(connections, uint8(level))
}

func (v *vertex) resetConnectionsAtLevelNoLock(level int) {
	v.connections.ReplaceLayer(uint8(level), []uint64{})
}

func (v *vertex) connectionsAtLowerLevelsNoLock(level int, visitedNodes map[nodeLevel]bool) []nodeLevel {
	var connections []nodeLevel
	for i := level; i >= 0; i-- {
		for _, nodeId := range v.connections.GetLayer(uint8(i)) {
			if !visitedNodes[nodeLevel{nodeId, i}] {
				connections = append(connections, nodeLevel{nodeId, i})
			}
		}
	}
	return connections
}
