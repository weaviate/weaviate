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

package graph

import (
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

type Vertex struct {
	m           sync.Mutex
	id          uint64
	level       int
	connections [][]uint64
	maintenance bool
}

func NewVertex(id uint64, level int) *Vertex {
	return NewVertexWithConnections(id, level, make([][]uint64, level+1))
}

func NewVertexWithConnections(id uint64, level int, connections [][]uint64) *Vertex {
	return &Vertex{
		id:          id,
		level:       level,
		connections: connections,
	}
}

func (v *Vertex) MarkAsMaintenance() {
	v.m.Lock()
	v.maintenance = true
	v.m.Unlock()
}

func (v *Vertex) UnmarkAsMaintenance() {
	v.m.Lock()
	v.maintenance = false
	v.m.Unlock()
}

func (v *Vertex) IsUnderMaintenance() bool {
	v.m.Lock()
	m := v.maintenance
	v.m.Unlock()
	return m
}

func (v *Vertex) ID() uint64 {
	// we don't need to lock here, the id is immutable
	return v.id
}

func (v *Vertex) Level() int {
	v.m.Lock()
	l := v.level
	v.m.Unlock()
	return l
}

func (v *Vertex) ConnectionLen() int {
	v.m.Lock()
	l := len(v.connections)
	v.m.Unlock()
	return l
}

func (v *Vertex) LevelLen(level int) int {
	v.m.Lock()
	if level >= len(v.connections) {
		return 0
	}
	l := len(v.connections[level])
	v.m.Unlock()
	return l
}

func (v *Vertex) IterConnections(level int, fn func(uint64) bool) {
	v.m.Lock()
	defer v.m.Unlock()

	if level >= len(v.connections) {
		return
	}

	for _, conn := range v.connections[level] {
		if !fn(conn) {
			return
		}
	}
}

// CopyConnections returns a full copy of the connections.
// The returned slice is a copy of the internal slice, so it can be modified
// without affecting the internal state.
// Do not use this method for large graphs, use IterConnections instead,
// in combination with ConnectionLen.
func (v *Vertex) CopyConnections() [][]uint64 {
	v.m.Lock()
	defer v.m.Unlock()

	connections := make([][]uint64, len(v.connections))
	for i, level := range v.connections {
		connections[i] = make([]uint64, len(level))
		copy(connections[i], level)
	}

	return connections
}

// CopyLevel returns a copy of the connections at a specific level. The returned
// slice is a copy of the internal slice, so it can be modified without affecting
// the internal state.
// If the buffer is nil or too small, a new slice will be allocated.
// If the buffer is large enough, it will be used to store the connections.
func (v *Vertex) CopyLevel(buf []uint64, level int) []uint64 {
	v.m.Lock()
	if level >= len(v.connections) {
		v.m.Unlock()
		return nil
	}

	// check the capacity of the buffer
	if buf == nil || cap(buf) < len(v.connections[level]) {
		buf = make([]uint64, len(v.connections[level]))
	} else {
		buf = buf[:len(v.connections[level])]
	}

	copy(buf, v.connections[level])
	v.m.Unlock()
	return buf
}

func (v *Vertex) ConnectionsAtLowerLevels(level int, visitedNodes map[NodeLevel]bool) []NodeLevel {
	v.m.Lock()
	defer v.m.Unlock()

	var connections []NodeLevel
	for i := level; i >= 0; i-- {
		for _, nodeID := range v.connections[i] {
			if !visitedNodes[NodeLevel{nodeID, i}] {
				connections = append(connections, NodeLevel{nodeID, i})
			}
		}
	}

	return connections
}

func (v *Vertex) ConnectionsPointTo(needles helpers.AllowList) bool {
	v.m.Lock()
	defer v.m.Unlock()

	for _, atLevel := range v.connections {
		for _, pointer := range atLevel {
			if needles.Contains(pointer) {
				return true
			}
		}
	}

	return false
}

// Edit allows to modify the vertex in a safe way. The function passed to Edit
// will be called with a VertexEditor, which includes methods to modify the
// vertex.
func (v *Vertex) Edit(fn func(v *VertexEditor) error) error {
	v.m.Lock()
	defer v.m.Unlock()

	return fn(&VertexEditor{v: v})
}

type VertexEditor struct {
	v *Vertex
}

func (v *VertexEditor) ID() uint64 {
	return v.v.id
}

func (v *VertexEditor) Level() int {
	return v.v.level
}

func (v *VertexEditor) ConnectionLen() int {
	return len(v.v.connections)
}

func (v *VertexEditor) LevelLen(level int) int {
	if level >= len(v.v.connections) {
		return 0
	}
	return len(v.v.connections[level])
}

func (v *VertexEditor) SetLevel(level int) {
	v.v.level = level

	v.EnsureLevel(level)
}

func (v *VertexEditor) EnsureLevel(level int) {
	if level >= len(v.v.connections) {
		// we need to grow the connections slice
		newConns := make([][]uint64, level+1)
		copy(newConns, v.v.connections)
		v.v.connections = newConns
	}
}

func (v *VertexEditor) ConnectionsAtLevel(level int) []uint64 {
	return v.v.connections[level]
}

func (v *VertexEditor) SetConnectionsAtLevel(level int, connections []uint64) (owned bool) {
	v.EnsureLevel(level)

	// before we simply copy the connections let's check how much smaller the new
	// list is. If it's considerably smaller, we might want to downsize the
	// current allocation
	oldCap := cap(v.v.connections[level])
	newLen := len(connections)
	ratio := float64(1) - float64(newLen)/float64(oldCap)
	if ratio > 0.33 || oldCap < newLen {
		// the replaced slice is over 33% smaller than the last one, this makes it
		// worth to replace it entirely. This has a small performance cost, it
		// means that if we need append to this node again, we need to re-allocate,
		// but we gain at least a 33% memory saving on this particular node right
		// away.
		v.v.connections[level] = connections
		return true
	}

	v.v.connections[level] = v.v.connections[level][:newLen]
	copy(v.v.connections[level], connections)
	return false
}

func (v *VertexEditor) AppendConnectionAtLevel(level int, connection uint64, maxConns int) {
	if len(v.v.connections[level]) == cap(v.v.connections[level]) && maxConns > 0 {
		// if the len is the capacity, this  means a new array needs to be
		// allocated to back this slice. The go runtime would do this
		// automatically, if we just use 'append', but it wouldn't do it very
		// efficiently. It would always double the existing capacity. Since we have
		// a hard limit (maxConns), we don't ever want to grow it beyond that. In
		// the worst case, the current len & cap could be maxConns-1, which would
		// mean we would double to 2*(maxConns-1) which would be way too large.
		//
		// Instead let's grow in 4 steps: 25%, 50%, 75% or full capacity
		ratio := float64(len(v.v.connections[level])) / float64(maxConns)

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
		if target < len(v.v.connections[level])+1 {
			target = len(v.v.connections[level]) + 1
		}

		newConns := make([]uint64, len(v.v.connections[level]), target)
		copy(newConns, v.v.connections[level])
		v.v.connections[level] = newConns
	}

	v.v.connections[level] = append(v.v.connections[level], connection)
}

func (v *VertexEditor) ResetConnections() {
	v.v.connections = make([][]uint64, len(v.v.connections))
}

func (v *VertexEditor) ResetConnectionsWith(connections [][]uint64) {
	v.v.connections = connections
}

func (v *VertexEditor) ResetConnectionsAtLevel(level int) {
	v.v.connections[level] = v.v.connections[level][:0]
}

type NodeLevel struct {
	NodeID uint64
	Level  int
}
