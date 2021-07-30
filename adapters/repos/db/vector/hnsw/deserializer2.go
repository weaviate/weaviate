//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Deserializer2 struct {
	logger logrus.FieldLogger
}

func NewDeserializer2(logger logrus.FieldLogger) *Deserializer2 {
	return &Deserializer2{logger: logger}
}

type DeserializationResult2 struct {
	Nodes             []*vertex
	Entrypoint        uint64
	Level             uint16
	Tombstones        map[uint64]struct{}
	EntrypointChanged bool
}

func (c *Deserializer2) Do(fd *bufio.Reader,
	initialState *DeserializationResult) (*DeserializationResult, error) {
	out := initialState
	if out == nil {
		out = &DeserializationResult{
			Nodes:      make([]*vertex, initialSize),
			Tombstones: make(map[uint64]struct{}),
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
			var entrypoint uint64
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
			err = errors.Errorf("unrecognized commit type %d", ct)
		}
		if err != nil {
			// do not return nil, err, because the err could be a recovarble one
			return out, errors.Errorf("failed to deserialize: %s", err)
		}
	}

	return out, nil
}

func (c *Deserializer2) ReadNode(r io.Reader, res *DeserializationResult) error {
	id, err := c.readUint64(r)
	if err != nil {
		return err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, id, c.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[id] == nil {
		res.Nodes[id] = &vertex{level: int(level), id: id, connections: make(map[int][]uint64)}
	} else {
		res.Nodes[id].level = int(level)
	}
	return nil
}

func (c *Deserializer2) ReadEP(r io.Reader) (uint64, uint16, error) {
	id, err := c.readUint64(r)
	if err != nil {
		return 0, 0, err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return 0, 0, err
	}

	return id, level, nil
}

func (c *Deserializer2) ReadLink(r io.Reader, res *DeserializationResult) error {
	source, err := c.readUint64(r)
	if err != nil {
		return errors.Errorf("failed to read source node id: %v", err)
	}

	level, err := c.readUint16(r)
	if err != nil {
		return  errors.Errorf("failed to read source node level: %v", err)
	}

	target, err := c.readUint64(r)
	if err != nil {
		return errors.Errorf("failed to read target node id: %v", err)
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, source, c.logger)
	if err != nil {
		return errors.Errorf("failed to grow index: %v", err)
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[int(source)] == nil {
		res.Nodes[int(source)] = &vertex{id: source, connections: make(map[int][]uint64)}
	}

	res.Nodes[int(source)].connections[int(level)] = append(res.Nodes[int(source)].connections[int(level)], target)
	return nil
}

func (c *Deserializer2) ReadLinks(r io.Reader, res *DeserializationResult) error {
	source, err := c.readUint64(r)
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

	targets, err := c.readUint64Slice(r, int(length))
	if err != nil {
		return err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, source, c.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[int(source)] == nil {
		res.Nodes[int(source)] = &vertex{id: source, connections: map[int][]uint64{}}
	}
	res.Nodes[int(source)].connections[int(level)] = targets
	return nil
}

func (c *Deserializer2) ReadAddTombstone(r io.Reader, tombstones map[uint64]struct{}) error {
	id, err := c.readUint64(r)
	if err != nil {
		return err
	}

	tombstones[id] = struct{}{}

	return nil
}

func (c *Deserializer2) ReadRemoveTombstone(r io.Reader, tombstones map[uint64]struct{}) error {
	id, err := c.readUint64(r)
	if err != nil {
		return err
	}

	delete(tombstones, id)

	return nil
}

func (c *Deserializer2) ReadClearLinks(r io.Reader, res *DeserializationResult) error {
	id, err := c.readUint64(r)
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

	res.Nodes[id].connections = map[int][]uint64{}
	return nil
}

func (c *Deserializer2) ReadDeleteNode(r io.Reader, res *DeserializationResult) error {
	id, err := c.readUint64(r)
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

func (c *Deserializer2) readUint64(r io.Reader) (uint64, error) {
	var value uint64
	tmpBuf := make([]byte, 8)
	if _, err := r.Read(tmpBuf); err != nil {
		return value, errors.Errorf("failed to read uint64: %s", err)
	}

	value = binary.LittleEndian.Uint64(tmpBuf)

	return value, nil
}

func (c *Deserializer2) readUint16(r io.Reader) (uint16, error) {
	var value uint16
	tmpBuf := make([]byte, 2)
	if _, err := r.Read(tmpBuf); err != nil {
		return value, errors.Errorf("failed to read uint16: %s", err)
	}

	value = binary.LittleEndian.Uint16(tmpBuf)

	return value, nil
}

func (c *Deserializer2) ReadCommitType(r io.Reader) (HnswCommitType, error) {
	tmpBuf := make([]byte, 1)
	if _, err := r.Read(tmpBuf); err != nil {
		return 0, errors.Errorf("failed to read commit type: %s", err)
	}

	return HnswCommitType(tmpBuf[0]), nil
}

func (c *Deserializer2) readUint64Slice(r io.Reader, length int) ([]uint64, error) {
	values := make([]uint64, length)
	for i := 0; i < length; i++ {
		v, err := c.readUint64(r)
		if err != nil {
			return values, errors.Errorf("failed to read uint64: %s", err)
		}
		values = append(values, v)

	}

	return values, nil
}
