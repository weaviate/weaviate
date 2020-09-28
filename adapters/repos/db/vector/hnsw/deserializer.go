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
	Nodes             []*vertex
	Entrypoint        uint32
	Level             uint16
	Tombstones        map[int]struct{}
	EntrypointChanged bool
}

func (c *Deserializer) Do(fd *os.File,
	initialState *DeserializationResult) (*DeserializationResult, error) {
	out := initialState
	if out == nil {
		out = &DeserializationResult{
			Nodes:      make([]*vertex, initialSize),
			Tombstones: make(map[int]struct{}),
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
			out.Entrypoint = entrypoint
			out.Level = level
			out.EntrypointChanged = true
		case AddLinkAtLevel:
			err = c.ReadLink(fd, out)
		case ReplaceLinksAtLevel:
			err = c.ReadLinks(fd, out)
		case AddTombstone:
			err = c.ReadAddTombstone(fd, out.Tombstones)
		case RemoveTombstone:
			err = c.ReadRemoveTombstone(fd, out.Tombstones)
		case ClearLinks:
			err = c.ReadClearLinks(fd, out)
		case DeleteNode:
			err = c.ReadDeleteNode(fd, out)
		case ResetIndex:
			out.Entrypoint = 0
			out.Level = 0
			out.Nodes = make([]*vertex, initialSize)
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

	newNodes, err := growIndexToAccomodateNode(res.Nodes, int(id))
	if err != nil {
		return err
	}

	res.Nodes = newNodes

	if res.Nodes[id] == nil {
		res.Nodes[id] = &vertex{level: int(level), id: int(id), connections: make(map[int][]uint32)}
	} else {
		res.Nodes[id].level = int(level)
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

	newNodes, err := growIndexToAccomodateNode(res.Nodes, int(source))
	if err != nil {
		return err
	}

	res.Nodes = newNodes

	if res.Nodes[int(source)] == nil {
		res.Nodes[int(source)] = &vertex{id: int(source), connections: make(map[int][]uint32)}
	}

	res.Nodes[int(source)].connections[int(level)] = append(res.Nodes[int(source)].connections[int(level)], target)
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

	newNodes, err := growIndexToAccomodateNode(res.Nodes, int(source))
	if err != nil {
		return err
	}

	res.Nodes = newNodes

	if res.Nodes[int(source)] == nil {
		res.Nodes[int(source)] = &vertex{id: int(source), connections: map[int][]uint32{}}
	}
	res.Nodes[int(source)].connections[int(level)] = targets
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

	if int(id) > len(res.Nodes) {
		// node is out of bounds, so it can't exist, nothing to do here
		return nil
	}

	if res.Nodes[id] == nil {
		// node has been deleted or never existed, nothing to do
		return nil
	}

	res.Nodes[id].connections = map[int][]uint32{}
	fmt.Printf("links cleared for node %d\n", id)
	return nil
}

func (c *Deserializer) ReadDeleteNode(r io.Reader, res *DeserializationResult) error {
	id, err := c.readUint32(r)
	if err != nil {
		return err
	}

	if int(id) > len(res.Nodes) {
		// node is out of bounds, so it can't exist, nothing to do here
		return nil
	}

	res.Nodes[id] = nil
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
