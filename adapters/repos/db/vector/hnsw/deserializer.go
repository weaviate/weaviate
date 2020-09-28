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

type Deserializer struct{}

func NewDeserializer() *Deserializer {
	return &Deserializer{}
}

type DeserializationResult struct {
	nodes      []*vertex
	entrypoint uint32
	level      uint16
	tombstones map[int]struct{}
}

func (c *Deserializer) Do(fd *os.File,
	initialState *DeserializationResult) (*DeserializationResult, error) {
	out := initialState
	if out == nil {
		out = &DeserializationResult{
			nodes:      make([]*vertex, initialSize),
			tombstones: make(map[int]struct{}),
		}
	}

	for {
		ct, err := c.ReadCommitType(fd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		switch ct {
		case AddNode:
			err = c.ReadNode(fd, out)
		case SetEntryPointMaxLevel:
			var entrypoint uint32
			var level uint16
			entrypoint, level, err = c.ReadEP(fd)
			out.entrypoint = entrypoint
			out.level = level
		case AddLinkAtLevel:
			err = c.ReadLink(fd, out)
		case ReplaceLinksAtLevel:
			err = c.ReadLinks(fd, out)
		case AddTombstone:
			err = c.ReadAddTombstone(fd, out.tombstones)
		case RemoveTombstone:
			err = c.ReadRemoveTombstone(fd, out.tombstones)
		case ClearLinks:
			err = c.ReadClearLinks(fd, out)
		case DeleteNode:
			err = c.ReadDeleteNode(fd, out)
		case ResetIndex:
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

func (c *Deserializer) ReadNode(r io.Reader, res *DeserializationResult) error {
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

func (c *Deserializer) ReadEP(r io.Reader) (uint32, uint16, error) {
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

func (c *Deserializer) ReadLink(r io.Reader, res *DeserializationResult) error {
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

	newNodes, err := growIndexToAccomodateNode(res.nodes, int(source))
	if err != nil {
		return err
	}

	res.nodes = newNodes

	if res.nodes[int(source)] == nil {
		res.nodes[int(source)] = &vertex{id: int(source), connections: make(map[int][]uint32)}
	}

	res.nodes[int(source)].connections[int(level)] = append(res.nodes[int(source)].connections[int(level)], target)
	return nil
}

func (c *Deserializer) ReadLinks(r io.Reader, res *DeserializationResult) error {
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

	newNodes, err := growIndexToAccomodateNode(res.nodes, int(source))
	if err != nil {
		return err
	}

	res.nodes = newNodes

	if res.nodes[int(source)] == nil {
		res.nodes[int(source)] = &vertex{id: int(source), connections: map[int][]uint32{}}
	}
	res.nodes[int(source)].connections[int(level)] = targets
	return nil
}

func (c *Deserializer) ReadAddTombstone(r io.Reader, tombstones map[int]struct{}) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	tombstones[int(id)] = struct{}{}

	return nil
}

func (c *Deserializer) ReadRemoveTombstone(r io.Reader, tombstones map[int]struct{}) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	delete(tombstones, int(id))

	return nil
}

func (c *Deserializer) ReadClearLinks(r io.Reader, res *DeserializationResult) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	if int(id) > len(res.nodes) {
		// node is out of bounds, so it can't exist, nothing to do here
		return nil
	}

	if res.nodes[id] == nil {
		// node has been deleted or never existed, nothing to do
		return nil
	}

	res.nodes[id].connections = map[int][]uint32{}
	fmt.Printf("links cleared for node %d\n", id)
	return nil
}

func (c *Deserializer) ReadDeleteNode(r io.Reader, res *DeserializationResult) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	if int(id) > len(res.nodes) {
		// node is out of bounds, so it can't exist, nothing to do here
		return nil
	}

	res.nodes[id] = nil
	return nil
}

func (c *Deserializer) readUint32(r io.Reader) (uint32, error) {
	var value uint32
	err := binary.Read(r, binary.LittleEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("reading uint32: %v", err)
	}

	return value, nil
}

func (c *Deserializer) readUint16(r io.Reader) (uint16, error) {
	var value uint16
	err := binary.Read(r, binary.LittleEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("reading uint16: %v", err)
	}

	return value, nil
}

func (c *Deserializer) ReadCommitType(r io.Reader) (HnswCommitType, error) {
	var value uint8
	err := binary.Read(r, binary.LittleEndian, &value)
	if err != nil {
		return 0, errors.Wrapf(err, "reading commit type (uint8)")
	}

	return HnswCommitType(value), nil
}

func (c *Deserializer) readUint32Slice(r io.Reader, length int) ([]uint32, error) {
	value := make([]uint32, length)
	err := binary.Read(r, binary.LittleEndian, &value)
	if err != nil {
		return nil, fmt.Errorf("reading []uint32: %v", err)
	}

	return value, nil
}
