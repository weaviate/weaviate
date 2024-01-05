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
	"sync"
)

type vertex struct {
	id uint64
	sync.Mutex
	level       int
	connections [][]uint64
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
	return v.connections[level]
}

func (v *vertex) upgradeToLevelNoLock(level int) {
	newConnections := make([][]uint64, level+1)
	copy(newConnections, v.connections)
	v.level = level
	v.connections = newConnections
}

func (v *vertex) setConnectionsAtLevel(level int, connections []uint64) {
	v.Lock()
	defer v.Unlock()

	// before we simply copy the connections let's check how much smaller the new
	// list is. If it's considerably smaller, we might want to downsize the
	// current allocation
	oldCap := cap(v.connections[level])
	newLen := len(connections)
	ratio := float64(1) - float64(newLen)/float64(oldCap)
	if ratio > 0.33 || oldCap < newLen {
		// the replaced slice is over 33% smaller than the last one, this makes it
		// worth to replace it entirely. This has a small performance cost, it
		// means that if we need append to this node again, we need to re-allocate,
		// but we gain at least a 33% memory saving on this particular node right
		// away.
		v.connections[level] = connections
		return
	}

	v.connections[level] = v.connections[level][:newLen]
	copy(v.connections[level], connections)
}

func (v *vertex) appendConnectionAtLevelNoLock(level int, connection uint64, maxConns int) {
	if len(v.connections[level]) == cap(v.connections[level]) {
		// if the len is the capacity, this  means a new array needs to be
		// allocated to back this slice. The go runtime would do this
		// automatically, if we just use 'append', but it wouldn't do it very
		// efficiently. It would always double the existing capacity. Since we have
		// a hard limit (maxConns), we don't ever want to grow it beyond that. In
		// the worst case, the current len & cap could be maxConns-1, which would
		// mean we would double to 2*(maxConns-1) which would be way too large.
		//
		// Instead let's grow in 4 steps: 25%, 50%, 75% or full capacity
		ratio := float64(len(v.connections[level])) / float64(maxConns)

		target := 0
		switch {
		case ratio < 0.25:
			target = int(float64(0.25) * float64(maxConns))
		case ratio < 0.50:
			target = int(float64(0.50) * float64(maxConns))
		case ratio < 0.75:
			target = int(float64(0.75) * float64(maxConns))
		default:
			target = maxConns
		}

		// handle rounding errors on maxConns not cleanly divisible by 4
		if target < len(v.connections[level])+1 {
			target = len(v.connections[level]) + 1
		}

		newConns := make([]uint64, len(v.connections[level]), target)
		copy(newConns, v.connections[level])
		v.connections[level] = newConns
	}

	v.connections[level] = append(v.connections[level], connection)
}

func (v *vertex) resetConnectionsAtLevelNoLock(level int) {
	v.connections[level] = v.connections[level][:0]
}
