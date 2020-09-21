//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
)

type deserializer struct{}

func newDeserializer() *deserializer {
	return &deserializer{}
}

type deserializationResult struct {
	nodes      []*vertex
	entrypoint uint32
	level      uint16
	tombstones map[int]struct{}
}

func (c *deserializer) Do(fd *os.File) (*deserializationResult, error) {
	out := &deserializationResult{
		nodes:      make([]*vertex, initialSize), // assume fixed length for now, make growable later
		tombstones: make(map[int]struct{}),
	}

	for {
		ct, err := c.readCommitType(fd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		switch ct {
		case addNode:
			err = c.readNode(fd, out)
		case setEntryPointMaxLevel:
			var entrypoint uint32
			var level uint16
			entrypoint, level, err = c.readEP(fd)
			out.entrypoint = entrypoint
			out.level = level
		case addLinkAtLevel:
			err = c.readLink(fd, out.nodes)
		case replaceLinksAtLevel:
			err = c.readLinks(fd, out.nodes)
		case addTombstone:
			err = c.readAddTombstone(fd, out.tombstones)
		case removeTombstone:
			err = c.readRemoveTombstone(fd, out.tombstones)
		case clearLinks:
			err = c.readClearLinks(fd, out.nodes)
		case deleteNode:
			err = c.readDeleteNode(fd, out.nodes)
		case resetIndex:
			out.entrypoint = 0
			out.level = 0
			out.nodes = make([]*vertex, initialSize)
		default:
			err = fmt.Errorf("unrecognized commit type %d", ct)
		}
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (c *deserializer) readNode(r io.Reader, res *deserializationResult) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return err
	}

	newNodes, err := growIndexToAccomodateNode(res.nodes, int(id))
	if err != nil {
		return err
	}

	res.nodes = newNodes

	if res.nodes[id] == nil {
		res.nodes[id] = &vertex{level: int(level), id: int(id), connections: make(map[int][]uint32)}
	} else {
		res.nodes[id].level = int(level)
	}
	return nil
}

func (c *deserializer) readEP(r io.Reader) (uint32, uint16, error) {
	id, err := c.readUint32(r)
	if err != nil {
		return 0, 0, err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return 0, 0, err
	}

	return id, level, nil
}

func (c *deserializer) readLink(r io.Reader, nodes []*vertex) error {
	source, err := c.readUint32(r)
	if err != nil {
		return err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return err
	}

	target, err := c.readUint32(r)
	if err != nil {
		return err
	}

	if int(source) >= len(nodes) || nodes[int(source)] == nil {
		nodes[int(source)] = &vertex{id: int(source), connections: make(map[int][]uint32)}
	}

	nodes[int(source)].connections[int(level)] = append(nodes[int(source)].connections[int(level)], target)
	return nil
}

func (c *deserializer) readLinks(r io.Reader, nodes []*vertex) error {
	source, err := c.readUint32(r)
	if err != nil {
		return err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return err
	}

	length, err := c.readUint16(r)
	if err != nil {
		return err
	}

	targets, err := c.readUint32Slice(r, int(length))
	if err != nil {
		return err
	}

	if int(source) >= len(nodes) || nodes[int(source)] == nil {
		return fmt.Errorf("source node does not exist")
	}

	nodes[int(source)].connections[int(level)] = targets
	return nil
}

func (c *deserializer) readAddTombstone(r io.Reader, tombstones map[int]struct{}) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	tombstones[int(id)] = struct{}{}

	return nil
}

func (c *deserializer) readRemoveTombstone(r io.Reader, tombstones map[int]struct{}) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	delete(tombstones, int(id))

	return nil
}

func (c *deserializer) readClearLinks(r io.Reader, nodes []*vertex) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	if int(id) > len(nodes) {
		// node is out of bounds, so it can't exist, nothing to do here
		return nil
	}

	if nodes[id] == nil {
		// node has been deleted or never existed, nothing to do
		return nil
	}

	nodes[id].connections = map[int][]uint32{}
	fmt.Printf("links cleared for node %d\n", id)
	return nil
}

func (c *deserializer) readDeleteNode(r io.Reader, nodes []*vertex) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	if int(id) > len(nodes) {
		// node is out of bounds, so it can't exist, nothing to do here
		return nil
	}

	nodes[id] = nil
	return nil
}

func (c *deserializer) readUint32(r io.Reader) (uint32, error) {
	var value uint32
	err := binary.Read(r, binary.LittleEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("reading uint32: %v", err)
	}

	return value, nil
}

func (c *deserializer) readUint16(r io.Reader) (uint16, error) {
	var value uint16
	err := binary.Read(r, binary.LittleEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("reading uint16: %v", err)
	}

	return value, nil
}

func (c *deserializer) readCommitType(r io.Reader) (hnswCommitType, error) {
	var value uint8
	err := binary.Read(r, binary.LittleEndian, &value)
	if err != nil {
		return 0, errors.Wrapf(err, "reading commit type (uint8)")
	}

	return hnswCommitType(value), nil
}

func (c *deserializer) readUint32Slice(r io.Reader, length int) ([]uint32, error) {
	value := make([]uint32, length)
	err := binary.Read(r, binary.LittleEndian, &value)
	if err != nil {
		return nil, fmt.Errorf("reading []uint32: %v", err)
	}

	return value, nil
}
