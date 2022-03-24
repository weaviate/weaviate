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
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Deserializer struct {
	logger logrus.FieldLogger
}

type DeserializationResult struct {
	Nodes             []*vertex
	Entrypoint        uint64
	Level             uint16
	Tombstones        map[uint64]struct{}
	EntrypointChanged bool

	// If there is no entry for the links at a level to be replaced, we must
	// assume that all links were appended and prior state must exist
	// Similarly if we run into a "Clear" we need to explicitly set the replace
	// flag, so that future appends aren't always appended and we run into a
	// situation where reading multiple condensed logs in succession leads to too
	// many connections as discovered in
	// https://github.com/semi-technologies/weaviate/issues/1868
	LinksReplaced map[uint64]map[uint16]struct{}
}

func (dr DeserializationResult) ReplaceLinks(node uint64, level uint16) bool {
	levels, ok := dr.LinksReplaced[node]
	if !ok {
		return false
	}

	_, ok = levels[level]
	return ok
}

func NewDeserializer(logger logrus.FieldLogger) *Deserializer {
	return &Deserializer{logger: logger}
}

func (c *Deserializer) Do(fd *bufio.Reader,
	initialState *DeserializationResult, keepLinkReplaceInformation bool) (*DeserializationResult, int, error) {
	validLength := 0
	out := initialState
	if out == nil {
		out = &DeserializationResult{
			Nodes:         make([]*vertex, initialSize),
			Tombstones:    make(map[uint64]struct{}),
			LinksReplaced: make(map[uint64]map[uint16]struct{}),
		}
	}

	for {
		ct, err := c.ReadCommitType(fd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, validLength, err
		}

		var readThisRound int

		switch ct {
		case AddNode:
			err = c.ReadNode(fd, out)
			readThisRound = 10
		case SetEntryPointMaxLevel:
			var entrypoint uint64
			var level uint16
			entrypoint, level, err = c.ReadEP(fd)
			out.Entrypoint = entrypoint
			out.Level = level
			out.EntrypointChanged = true
			readThisRound = 10
		case AddLinkAtLevel:
			err = c.ReadLink(fd, out)
			readThisRound = 18
		case AddLinksAtLevel:
			readThisRound, err = c.ReadAddLinks(fd, out)
		case ReplaceLinksAtLevel:
			readThisRound, err = c.ReadLinks(fd, out, keepLinkReplaceInformation)
		case AddTombstone:
			err = c.ReadAddTombstone(fd, out.Tombstones)
			readThisRound = 8
		case RemoveTombstone:
			err = c.ReadRemoveTombstone(fd, out.Tombstones)
			readThisRound = 8
		case ClearLinks:
			err = c.ReadClearLinks(fd, out, keepLinkReplaceInformation)
			readThisRound = 8
		case ClearLinksAtLevel:
			err = c.ReadClearLinksAtLevel(fd, out, keepLinkReplaceInformation)
			readThisRound = 10
		case DeleteNode:
			err = c.ReadDeleteNode(fd, out)
			readThisRound = 8
		case ResetIndex:
			out.Entrypoint = 0
			out.Level = 0
			out.Nodes = make([]*vertex, initialSize)
		default:
			err = errors.Errorf("unrecognized commit type %d", ct)
		}
		if err != nil {
			// do not return nil, err, because the err could be a recoverable one
			return out, validLength, err
		} else {
			validLength += 1 + readThisRound // 1 byte for commit type
		}
	}

	return out, validLength, nil
}

func (c *Deserializer) ReadNode(r io.Reader, res *DeserializationResult) error {
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

func (c *Deserializer) ReadEP(r io.Reader) (uint64, uint16, error) {
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

func (c *Deserializer) ReadLink(r io.Reader, res *DeserializationResult) error {
	source, err := c.readUint64(r)
	if err != nil {
		return err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return err
	}

	target, err := c.readUint64(r)
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
		res.Nodes[int(source)] = &vertex{id: source, connections: make(map[int][]uint64)}
	}

	res.Nodes[int(source)].connections[int(level)] = append(res.Nodes[int(source)].connections[int(level)], target)
	return nil
}

func (c *Deserializer) ReadLinks(r io.Reader, res *DeserializationResult,
	keepReplaceInfo bool) (int, error) {
	source, err := c.readUint64(r)
	if err != nil {
		return 0, err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return 0, err
	}

	length, err := c.readUint16(r)
	if err != nil {
		return 0, err
	}

	targets, err := c.readUint64Slice(r, int(length))
	if err != nil {
		return 0, err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, source, c.logger)
	if err != nil {
		return 0, err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[int(source)] == nil {
		res.Nodes[int(source)] = &vertex{id: source, connections: map[int][]uint64{}}
	}
	res.Nodes[int(source)].connections[int(level)] = targets

	if keepReplaceInfo {
		// mark the replace flag for this node and level, so that new commit logs
		// generated on this result (condensing) do not lose information

		if _, ok := res.LinksReplaced[source]; !ok {
			res.LinksReplaced[source] = map[uint16]struct{}{}
		}

		res.LinksReplaced[source][level] = struct{}{}
	}

	return 12 + int(length)*8, nil
}

func (c *Deserializer) ReadAddLinks(r io.Reader,
	res *DeserializationResult) (int, error) {
	source, err := c.readUint64(r)
	if err != nil {
		return 0, err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return 0, err
	}

	length, err := c.readUint16(r)
	if err != nil {
		return 0, err
	}

	targets, err := c.readUint64Slice(r, int(length))
	if err != nil {
		return 0, err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, source, c.logger)
	if err != nil {
		return 0, err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[int(source)] == nil {
		res.Nodes[int(source)] = &vertex{id: source, connections: map[int][]uint64{}}
	}
	res.Nodes[int(source)].connections[int(level)] = append(
		res.Nodes[int(source)].connections[int(level)], targets...)

	return 12 + int(length)*8, nil
}

func (c *Deserializer) ReadAddTombstone(r io.Reader, tombstones map[uint64]struct{}) error {
	id, err := c.readUint64(r)
	if err != nil {
		return err
	}

	tombstones[id] = struct{}{}

	return nil
}

func (c *Deserializer) ReadRemoveTombstone(r io.Reader, tombstones map[uint64]struct{}) error {
	id, err := c.readUint64(r)
	if err != nil {
		return err
	}

	delete(tombstones, id)

	return nil
}

func (c *Deserializer) ReadClearLinks(r io.Reader, res *DeserializationResult,
	keepReplaceInfo bool) error {
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

func (c *Deserializer) ReadClearLinksAtLevel(r io.Reader, res *DeserializationResult,
	keepReplaceInfo bool) error {
	id, err := c.readUint64(r)
	if err != nil {
		return err
	}

	level, err := c.readUint16(r)
	if err != nil {
		return err
	}

	if int(id) > len(res.Nodes) {
		// node is out of bounds, so it can't exist, nothing to do here
		return nil
	}

	if keepReplaceInfo {
		// mark the replace flag for this node and level, so that new commit logs
		// generated on this result (condensing) do not lose information

		if _, ok := res.LinksReplaced[id]; !ok {
			res.LinksReplaced[id] = map[uint16]struct{}{}
		}

		res.LinksReplaced[id][level] = struct{}{}
	}

	if res.Nodes[id] == nil {
		if !keepReplaceInfo {
			// node has been deleted or never existed and we are not looking at a
			// single log in isolation, nothing to do
			return nil
		}

		// we need to keep the replace info, meaning we have to explicitly create
		// this node in order to be able to store the "clear links" information for
		// it
		res.Nodes[id] = &vertex{
			id:          id,
			connections: map[int][]uint64{},
		}
	}

	if res.Nodes[id].connections == nil {
		res.Nodes[id].connections = map[int][]uint64{}
	} else {
		res.Nodes[id].connections[int(level)] = []uint64{}
	}

	if keepReplaceInfo {
		// mark the replace flag for this node and level, so that new commit logs
		// generated on this result (condensing) do not lose information

		if _, ok := res.LinksReplaced[id]; !ok {
			res.LinksReplaced[id] = map[uint16]struct{}{}
		}

		res.LinksReplaced[id][level] = struct{}{}
	}

	return nil
}

func (c *Deserializer) ReadDeleteNode(r io.Reader, res *DeserializationResult) error {
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

func (c *Deserializer) readUint64(r io.Reader) (uint64, error) {
	var value uint64
	tmpBuf := make([]byte, 8)
	_, err := io.ReadFull(r, tmpBuf)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read uint64")
	}

	value = binary.LittleEndian.Uint64(tmpBuf)

	return value, nil
}

func (c *Deserializer) readUint16(r io.Reader) (uint16, error) {
	var value uint16
	tmpBuf := make([]byte, 2)
	_, err := io.ReadFull(r, tmpBuf)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read uint16")
	}

	value = binary.LittleEndian.Uint16(tmpBuf)

	return value, nil
}

func (c *Deserializer) ReadCommitType(r io.Reader) (HnswCommitType, error) {
	tmpBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, tmpBuf); err != nil {
		return 0, errors.Wrap(err, "failed to read commit type")
	}

	return HnswCommitType(tmpBuf[0]), nil
}

func (c *Deserializer) readUint64Slice(r io.Reader, length int) ([]uint64, error) {
	values := make([]uint64, length)
	for i := range values {
		value, err := c.readUint64(r)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read []uint64")
		}
		values[i] = value
	}

	return values, nil
}
