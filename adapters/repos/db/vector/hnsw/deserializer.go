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
	"bufio"
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

type Deserializer struct {
	logger                   logrus.FieldLogger
	reusableBuffer           []byte
	reusableConnectionsSlice []uint64
}

type DeserializationResult struct {
	Nodes             []*vertex
	Entrypoint        uint64
	Level             uint16
	Tombstones        map[uint64]struct{}
	EntrypointChanged bool
	PQData            compressionhelpers.PQData
	Compressed        bool

	// If there is no entry for the links at a level to be replaced, we must
	// assume that all links were appended and prior state must exist
	// Similarly if we run into a "Clear" we need to explicitly set the replace
	// flag, so that future appends aren't always appended and we run into a
	// situation where reading multiple condensed logs in succession leads to too
	// many connections as discovered in
	// https://github.com/weaviate/weaviate/issues/1868
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

func (d *Deserializer) resetResusableBuffer(size int) {
	if size <= cap(d.reusableBuffer) {
		d.reusableBuffer = d.reusableBuffer[:size]
	} else {
		d.reusableBuffer = make([]byte, size, size*2)
	}
}

func (d *Deserializer) resetReusableConnectionsSlice(size int) {
	if size <= cap(d.reusableConnectionsSlice) {
		d.reusableConnectionsSlice = d.reusableConnectionsSlice[:size]
	} else {
		d.reusableConnectionsSlice = make([]uint64, size, size*2)
	}
}

func (d *Deserializer) Do(fd *bufio.Reader,
	initialState *DeserializationResult, keepLinkReplaceInformation bool,
) (*DeserializationResult, int, error) {
	validLength := 0
	out := initialState
	if out == nil {
		out = &DeserializationResult{
			Nodes:         make([]*vertex, cache.InitialSize),
			Tombstones:    make(map[uint64]struct{}),
			LinksReplaced: make(map[uint64]map[uint16]struct{}),
		}
	}

	for {
		ct, err := d.ReadCommitType(fd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, validLength, err
		}

		var readThisRound int

		switch ct {
		case AddNode:
			err = d.ReadNode(fd, out)
			readThisRound = 10
		case SetEntryPointMaxLevel:
			var entrypoint uint64
			var level uint16
			entrypoint, level, err = d.ReadEP(fd)
			out.Entrypoint = entrypoint
			out.Level = level
			out.EntrypointChanged = true
			readThisRound = 10
		case AddLinkAtLevel:
			err = d.ReadLink(fd, out)
			readThisRound = 18
		case AddLinksAtLevel:
			readThisRound, err = d.ReadAddLinks(fd, out)
		case ReplaceLinksAtLevel:
			readThisRound, err = d.ReadLinks(fd, out, keepLinkReplaceInformation)
		case AddTombstone:
			err = d.ReadAddTombstone(fd, out.Tombstones)
			readThisRound = 8
		case RemoveTombstone:
			err = d.ReadRemoveTombstone(fd, out.Tombstones)
			readThisRound = 8
		case ClearLinks:
			err = d.ReadClearLinks(fd, out, keepLinkReplaceInformation)
			readThisRound = 8
		case ClearLinksAtLevel:
			err = d.ReadClearLinksAtLevel(fd, out, keepLinkReplaceInformation)
			readThisRound = 10
		case DeleteNode:
			err = d.ReadDeleteNode(fd, out)
			readThisRound = 8
		case ResetIndex:
			out.Entrypoint = 0
			out.Level = 0
			out.Nodes = make([]*vertex, cache.InitialSize)
		case AddPQ:
			err = d.ReadPQ(fd, out)
			readThisRound = 9
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

func (d *Deserializer) ReadNode(r io.Reader, res *DeserializationResult) error {
	id, err := d.readUint64(r)
	if err != nil {
		return err
	}

	level, err := d.readUint16(r)
	if err != nil {
		return err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, id, d.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[id] == nil {
		res.Nodes[id] = &vertex{level: int(level), id: id, connections: make([][]uint64, level+1)}
	} else {
		maybeGrowConnectionsForLevel(&res.Nodes[id].connections, level)
		res.Nodes[id].level = int(level)
	}
	return nil
}

func (d *Deserializer) ReadEP(r io.Reader) (uint64, uint16, error) {
	id, err := d.readUint64(r)
	if err != nil {
		return 0, 0, err
	}

	level, err := d.readUint16(r)
	if err != nil {
		return 0, 0, err
	}

	return id, level, nil
}

func (d *Deserializer) ReadLink(r io.Reader, res *DeserializationResult) error {
	source, err := d.readUint64(r)
	if err != nil {
		return err
	}

	level, err := d.readUint16(r)
	if err != nil {
		return err
	}

	target, err := d.readUint64(r)
	if err != nil {
		return err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, source, d.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[int(source)] == nil {
		res.Nodes[int(source)] = &vertex{id: source, connections: make([][]uint64, level+1)}
	}

	maybeGrowConnectionsForLevel(&res.Nodes[int(source)].connections, level)

	res.Nodes[int(source)].connections[int(level)] = append(res.Nodes[int(source)].connections[int(level)], target)
	return nil
}

func (d *Deserializer) ReadLinks(r io.Reader, res *DeserializationResult,
	keepReplaceInfo bool,
) (int, error) {
	d.resetResusableBuffer(12)
	_, err := io.ReadFull(r, d.reusableBuffer)
	if err != nil {
		return 0, err
	}

	source := binary.LittleEndian.Uint64(d.reusableBuffer[0:8])
	level := binary.LittleEndian.Uint16(d.reusableBuffer[8:10])
	length := binary.LittleEndian.Uint16(d.reusableBuffer[10:12])

	targets, err := d.readUint64Slice(r, int(length))
	if err != nil {
		return 0, err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, source, d.logger)
	if err != nil {
		return 0, err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[int(source)] == nil {
		res.Nodes[int(source)] = &vertex{id: source, connections: make([][]uint64, level+1)}
	}

	maybeGrowConnectionsForLevel(&res.Nodes[int(source)].connections, level)
	res.Nodes[int(source)].connections[int(level)] = make([]uint64, len(targets))
	copy(res.Nodes[int(source)].connections[int(level)], targets)

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

func (d *Deserializer) ReadAddLinks(r io.Reader,
	res *DeserializationResult,
) (int, error) {
	d.resetResusableBuffer(12)
	_, err := io.ReadFull(r, d.reusableBuffer)
	if err != nil {
		return 0, err
	}

	source := binary.LittleEndian.Uint64(d.reusableBuffer[0:8])
	level := binary.LittleEndian.Uint16(d.reusableBuffer[8:10])
	length := binary.LittleEndian.Uint16(d.reusableBuffer[10:12])

	targets, err := d.readUint64Slice(r, int(length))
	if err != nil {
		return 0, err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, source, d.logger)
	if err != nil {
		return 0, err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[int(source)] == nil {
		res.Nodes[int(source)] = &vertex{id: source, connections: make([][]uint64, level+1)}
	}

	maybeGrowConnectionsForLevel(&res.Nodes[int(source)].connections, level)

	res.Nodes[int(source)].connections[int(level)] = append(
		res.Nodes[int(source)].connections[int(level)], targets...)

	return 12 + int(length)*8, nil
}

func (d *Deserializer) ReadAddTombstone(r io.Reader, tombstones map[uint64]struct{}) error {
	id, err := d.readUint64(r)
	if err != nil {
		return err
	}

	tombstones[id] = struct{}{}

	return nil
}

func (d *Deserializer) ReadRemoveTombstone(r io.Reader, tombstones map[uint64]struct{}) error {
	id, err := d.readUint64(r)
	if err != nil {
		return err
	}

	delete(tombstones, id)

	return nil
}

func (d *Deserializer) ReadClearLinks(r io.Reader, res *DeserializationResult,
	keepReplaceInfo bool,
) error {
	id, err := d.readUint64(r)
	if err != nil {
		return err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, id, d.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[id] == nil {
		// node has been deleted or never existed, nothing to do
		return nil
	}

	res.Nodes[id].connections = make([][]uint64, len(res.Nodes[id].connections))
	return nil
}

func (d *Deserializer) ReadClearLinksAtLevel(r io.Reader, res *DeserializationResult,
	keepReplaceInfo bool,
) error {
	id, err := d.readUint64(r)
	if err != nil {
		return err
	}

	level, err := d.readUint16(r)
	if err != nil {
		return err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, id, d.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
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
			connections: make([][]uint64, level+1),
		}
	}

	if res.Nodes[id].connections == nil {
		res.Nodes[id].connections = make([][]uint64, level+1)
	} else {
		maybeGrowConnectionsForLevel(&res.Nodes[id].connections, level)
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

func (d *Deserializer) ReadDeleteNode(r io.Reader, res *DeserializationResult) error {
	id, err := d.readUint64(r)
	if err != nil {
		return err
	}

	newNodes, changed, err := growIndexToAccomodateNode(res.Nodes, id, d.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	res.Nodes[id] = nil
	return nil
}

func (d *Deserializer) ReadTileEncoder(r io.Reader, res *DeserializationResult, i uint16) (compressionhelpers.PQEncoder, error) {
	bins, err := d.readFloat64(r)
	if err != nil {
		return nil, err
	}
	mean, err := d.readFloat64(r)
	if err != nil {
		return nil, err
	}
	stdDev, err := d.readFloat64(r)
	if err != nil {
		return nil, err
	}
	size, err := d.readFloat64(r)
	if err != nil {
		return nil, err
	}
	s1, err := d.readFloat64(r)
	if err != nil {
		return nil, err
	}
	s2, err := d.readFloat64(r)
	if err != nil {
		return nil, err
	}
	segment, err := d.readUint16(r)
	if err != nil {
		return nil, err
	}
	encDistribution, err := d.readByte(r)
	if err != nil {
		return nil, err
	}
	return compressionhelpers.RestoreTileEncoder(bins, mean, stdDev, size, s1, s2, segment, encDistribution), nil
}

func (d *Deserializer) ReadKMeansEncoder(r io.Reader, res *DeserializationResult, i uint16) (compressionhelpers.PQEncoder, error) {
	ds := int(res.PQData.Dimensions / res.PQData.M)
	centers := make([][]float32, 0, res.PQData.Ks)
	for k := uint16(0); k < res.PQData.Ks; k++ {
		center := make([]float32, 0, ds)
		for i := 0; i < ds; i++ {
			c, err := d.readFloat32(r)
			if err != nil {
				return nil, err
			}
			center = append(center, c)
		}
		centers = append(centers, center)
	}
	kms := compressionhelpers.NewKMeansWithCenters(
		int(res.PQData.Ks),
		ds,
		int(i),
		centers,
	)
	return kms, nil
}

func (d *Deserializer) ReadPQ(r io.Reader, res *DeserializationResult) error {
	dims, err := d.readUint16(r)
	if err != nil {
		return err
	}
	enc, err := d.readByte(r)
	if err != nil {
		return err
	}
	ks, err := d.readUint16(r)
	if err != nil {
		return err
	}
	m, err := d.readUint16(r)
	if err != nil {
		return err
	}
	dist, err := d.readByte(r)
	if err != nil {
		return err
	}
	useBitsEncoding, err := d.readByte(r)
	if err != nil {
		return err
	}
	encoder := compressionhelpers.Encoder(enc)
	res.PQData = compressionhelpers.PQData{
		Dimensions:          dims,
		EncoderType:         encoder,
		Ks:                  ks,
		M:                   m,
		EncoderDistribution: byte(dist),
		UseBitsEncoding:     useBitsEncoding != 0,
	}
	var encoderReader func(io.Reader, *DeserializationResult, uint16) (compressionhelpers.PQEncoder, error)
	switch encoder {
	case compressionhelpers.UseTileEncoder:
		encoderReader = d.ReadTileEncoder
	case compressionhelpers.UseKMeansEncoder:
		encoderReader = d.ReadKMeansEncoder
	default:
		return errors.New("Unsuported encoder type")
	}
	for i := uint16(0); i < m; i++ {
		encoder, err := encoderReader(r, res, i)
		if err != nil {
			return err
		}
		res.PQData.Encoders = append(res.PQData.Encoders, encoder)
	}
	res.Compressed = true

	return nil
}

func (d *Deserializer) readUint64(r io.Reader) (uint64, error) {
	var value uint64
	d.resetResusableBuffer(8)
	_, err := io.ReadFull(r, d.reusableBuffer)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read uint64")
	}

	value = binary.LittleEndian.Uint64(d.reusableBuffer)

	return value, nil
}

func (d *Deserializer) readFloat64(r io.Reader) (float64, error) {
	var value float64
	d.resetResusableBuffer(8)
	_, err := io.ReadFull(r, d.reusableBuffer)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read float64")
	}

	bits := binary.LittleEndian.Uint64(d.reusableBuffer)
	value = math.Float64frombits(bits)

	return value, nil
}

func (d *Deserializer) readFloat32(r io.Reader) (float32, error) {
	var value float32
	d.resetResusableBuffer(4)
	_, err := io.ReadFull(r, d.reusableBuffer)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read float32")
	}

	bits := binary.LittleEndian.Uint32(d.reusableBuffer)
	value = math.Float32frombits(bits)

	return value, nil
}

func (d *Deserializer) readUint16(r io.Reader) (uint16, error) {
	var value uint16
	d.resetResusableBuffer(2)
	_, err := io.ReadFull(r, d.reusableBuffer)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read uint16")
	}

	value = binary.LittleEndian.Uint16(d.reusableBuffer)

	return value, nil
}

func (d *Deserializer) readByte(r io.Reader) (byte, error) {
	d.resetResusableBuffer(1)
	_, err := io.ReadFull(r, d.reusableBuffer)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read byte")
	}

	return d.reusableBuffer[0], nil
}

func (d *Deserializer) ReadCommitType(r io.Reader) (HnswCommitType, error) {
	d.resetResusableBuffer(1)
	if _, err := io.ReadFull(r, d.reusableBuffer); err != nil {
		return 0, errors.Wrap(err, "failed to read commit type")
	}

	return HnswCommitType(d.reusableBuffer[0]), nil
}

func (d *Deserializer) readUint64Slice(r io.Reader, length int) ([]uint64, error) {
	d.resetResusableBuffer(length * 8)
	d.resetReusableConnectionsSlice(length)
	_, err := io.ReadFull(r, d.reusableBuffer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read uint64 slice")
	}

	for i := range d.reusableConnectionsSlice {
		d.reusableConnectionsSlice[i] = binary.LittleEndian.Uint64(d.reusableBuffer[i*8 : (i+1)*8])
	}

	return d.reusableConnectionsSlice, nil
}

// If the connections array is to small to contain the current target-levelit
// will be grown. Otherwise, nothing happens.
func maybeGrowConnectionsForLevel(connsPtr *[][]uint64, level uint16) {
	conns := *connsPtr
	if len(conns) <= int(level) {
		// we need to grow the connections slice
		newConns := make([][]uint64, level+1)
		copy(newConns, conns)
		*connsPtr = newConns
	}
}
