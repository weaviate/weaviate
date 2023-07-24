//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
)

type vertex struct {
	id uint64
	sync.Mutex
	level int
	//connections       [][]uint64
	packedConnections *packedconn.Connections
	maintenance       bool
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

func (v *vertex) upgradePackedToLevelNoLock(level int) {
	v.level = level
	v.packedConnections.AddLayer()
}

func (v *vertex) setPackedConnectionsAtLevel(level int, connections []uint64) {
	v.Lock()
	defer v.Unlock()
	v.packedConnections.ReplaceLayer(uint8(level), connections)
}
