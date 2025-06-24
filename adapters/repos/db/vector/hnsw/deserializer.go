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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
)

type Deserializer struct {
	logger                   logrus.FieldLogger
	reusableBuffer           []byte
	reusableConnectionsSlice []uint64
}

type DeserializationResult struct {
	Nodes             []*vertex
	NodesDeleted      map[uint64]struct{}
	Entrypoint        uint64
	Level             uint16
	Tombstones        map[uint64]struct{}
	TombstonesDeleted map[uint64]struct{}
	EntrypointChanged bool
	CompressionPQData *compressionhelpers.PQData
	CompressionSQData *compressionhelpers.SQData
	MuveraEnabled     bool
	EncoderMuvera     *multivector.MuveraData
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

func (d *Deserializer) Do(fd *bufio.Reader, initialState *DeserializationResult, keepLinkReplaceInformation bool) (*DeserializationResult, int, error) {
	validLength := 0
	out := initialState
	commitTypeMetrics := make(map[HnswCommitType]int)
	if out == nil {
		out = &DeserializationResult{
			Nodes:             make([]*vertex, cache.InitialSize),
			NodesDeleted:      make(map[uint64]struct{}),
			Tombstones:        make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		}
	}

	for {
		ct, err := ReadCommitType(fd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, validLength, err
		}
		commitTypeMetrics[ct]++
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
			err = d.ReadRemoveTombstone(fd, out.Tombstones, out.TombstonesDeleted)
			readThisRound = 8
		case ClearLinks:
			err = d.ReadClearLinks(fd, out, keepLinkReplaceInformation)
			readThisRound = 8
		case ClearLinksAtLevel:
			err = d.ReadClearLinksAtLevel(fd, out, keepLinkReplaceInformation)
			readThisRound = 10
		case DeleteNode:
			err = d.ReadDeleteNode(fd, out, out.NodesDeleted)
			readThisRound = 8
		case ResetIndex:
			out.Entrypoint = 0
			out.Level = 0
			out.Nodes = make([]*vertex, cache.InitialSize)
			out.Tombstones = make(map[uint64]struct{})
		case AddPQ:
			var totalRead int
			totalRead, err = d.ReadPQ(fd, out)
			readThisRound = 9 + totalRead
		case AddSQ:
			err = d.ReadSQ(fd, out)
			readThisRound = 10
		case AddMuvera:
			var totalRead int
			totalRead, err = d.ReadMuvera(fd, out)
			readThisRound = 20 + totalRead
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
	for commitType, count := range commitTypeMetrics {
		d.logger.WithFields(logrus.Fields{"action": "hnsw_deserialization", "ops": count}).Debugf("hnsw commit logger %s", commitType)
	}
	return out, validLength, nil
}

func (d *Deserializer) ReadNode(r io.Reader, res *DeserializationResult) error {
	id, err := d.readUint64(r)
	if err != nil {
		return err
	}

	level, err := readUint16(r)
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

	level, err := readUint16(r)
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

	level, err := readUint16(r)
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

func (d *Deserializer) ReadRemoveTombstone(r io.Reader, tombstones map[uint64]struct{}, tombstonesDeleted map[uint64]struct{}) error {
	id, err := d.readUint64(r)
	if err != nil {
		return err
	}

	_, ok := tombstones[id]
	if !ok {
		// Tombstone is not present but may exist in older commit log
		// wWe need to keep track of it so we can delete it later
		tombstonesDeleted[id] = struct{}{}
	} else {
		// Tombstone is present, we can delete it
		delete(tombstones, id)
	}

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

	level, err := readUint16(r)
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

func (d *Deserializer) ReadDeleteNode(r io.Reader, res *DeserializationResult, nodesDeleted map[uint64]struct{}) error {
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
	nodesDeleted[id] = struct{}{}
	return nil
}

func ReadTileEncoder(r io.Reader, res *compressionhelpers.PQData, i uint16) (compressionhelpers.PQEncoder, error) {
	bins, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	mean, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	stdDev, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	size, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	s1, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	s2, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	segment, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	encDistribution, err := readByte(r)
	if err != nil {
		return nil, err
	}
	return compressionhelpers.RestoreTileEncoder(bins, mean, stdDev, size, s1, s2, segment, encDistribution), nil
}

func ReadKMeansEncoder(r io.Reader, data *compressionhelpers.PQData, i uint16) (compressionhelpers.PQEncoder, error) {
	ds := int(data.Dimensions / data.M)
	centers := make([][]float32, 0, data.Ks)
	for k := uint16(0); k < data.Ks; k++ {
		center := make([]float32, 0, ds)
		for i := 0; i < ds; i++ {
			c, err := readFloat32(r)
			if err != nil {
				return nil, err
			}
			center = append(center, c)
		}
		centers = append(centers, center)
	}
	kms := compressionhelpers.NewKMeansEncoderWithCenters(
		int(data.Ks),
		ds,
		int(i),
		centers,
	)
	return kms, nil
}

func (d *Deserializer) ReadPQ(r io.Reader, res *DeserializationResult) (int, error) {
	dims, err := readUint16(r)
	if err != nil {
		return 0, err
	}
	enc, err := readByte(r)
	if err != nil {
		return 0, err
	}
	ks, err := readUint16(r)
	if err != nil {
		return 0, err
	}
	m, err := readUint16(r)
	if err != nil {
		return 0, err
	}
	dist, err := readByte(r)
	if err != nil {
		return 0, err
	}
	useBitsEncoding, err := readByte(r)
	if err != nil {
		return 0, err
	}
	encoder := compressionhelpers.Encoder(enc)
	pqData := compressionhelpers.PQData{
		Dimensions:          dims,
		EncoderType:         encoder,
		Ks:                  ks,
		M:                   m,
		EncoderDistribution: byte(dist),
		UseBitsEncoding:     useBitsEncoding != 0,
	}
	var encoderReader func(io.Reader, *compressionhelpers.PQData, uint16) (compressionhelpers.PQEncoder, error)
	var totalRead int
	switch encoder {
	case compressionhelpers.UseTileEncoder:
		encoderReader = ReadTileEncoder
		totalRead = 51 * int(pqData.M)
	case compressionhelpers.UseKMeansEncoder:
		encoderReader = ReadKMeansEncoder
		totalRead = int(pqData.Dimensions) * int(pqData.Ks) * 4
	default:
		return 0, errors.New("Unsuported encoder type")
	}

	for i := uint16(0); i < m; i++ {
		encoder, err := encoderReader(r, &pqData, i)
		if err != nil {
			return 0, err
		}
		pqData.Encoders = append(pqData.Encoders, encoder)
	}
	res.Compressed = true

	res.CompressionPQData = &pqData

	return totalRead, nil
}

func (d *Deserializer) ReadSQ(r io.Reader, res *DeserializationResult) error {
	a, err := readFloat32(r)
	if err != nil {
		return err
	}
	b, err := readFloat32(r)
	if err != nil {
		return err
	}
	dims, err := readUint16(r)
	if err != nil {
		return err
	}
	res.CompressionSQData = &compressionhelpers.SQData{
		A:          a,
		B:          b,
		Dimensions: dims,
	}
	res.Compressed = true

	return nil
}

func (d *Deserializer) ReadMuvera(r io.Reader, res *DeserializationResult) (int, error) {
	kSim, err := readUint32(r)
	if err != nil {
		return 0, err
	}
	numClusters, err := readUint32(r)
	if err != nil {
		return 0, err
	}
	dimensions, err := readUint32(r)
	if err != nil {
		return 0, err
	}
	dProjections, err := readUint32(r)
	if err != nil {
		return 0, err
	}
	repetitions, err := readUint32(r)
	if err != nil {
		return 0, err
	}

	totalRead := int(repetitions)*int(kSim)*int(dimensions)*4 +
		int(repetitions)*int(dProjections)*int(dimensions)*4

	gaussians := make([][][]float32, repetitions)
	for i := uint32(0); i < repetitions; i++ {
		gaussians[i] = make([][]float32, kSim)
		for j := uint32(0); j < kSim; j++ {
			gaussians[i][j] = make([]float32, dimensions)
			for k := uint32(0); k < dimensions; k++ {
				gaussians[i][j][k], err = readFloat32(r)
				if err != nil {
					return 0, err
				}
			}
		}
	}

	s := make([][][]float32, repetitions)
	for i := uint32(0); i < repetitions; i++ {
		s[i] = make([][]float32, dProjections)
		for j := uint32(0); j < dProjections; j++ {
			s[i][j] = make([]float32, dimensions)
			for k := uint32(0); k < dimensions; k++ {
				s[i][j][k], err = readFloat32(r)
				if err != nil {
					return 0, err
				}
			}
		}
	}
	muveraData := multivector.MuveraData{
		KSim:         kSim,
		NumClusters:  numClusters,
		Dimensions:   dimensions,
		DProjections: dProjections,
		Repetitions:  repetitions,
		Gaussians:    gaussians,
		S:            s,
	}
	res.EncoderMuvera = &muveraData
	res.MuveraEnabled = true
	return totalRead, nil
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

func readFloat64(r io.Reader) (float64, error) {
	var b [8]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, errors.Wrap(err, "failed to read float64")
	}

	bits := binary.LittleEndian.Uint64(b[:])
	return math.Float64frombits(bits), nil
}

func readFloat32(r io.Reader) (float32, error) {
	var b [4]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, errors.Wrap(err, "failed to read float32")
	}

	bits := binary.LittleEndian.Uint32(b[:])
	return math.Float32frombits(bits), nil
}

func readUint16(r io.Reader) (uint16, error) {
	var b [2]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, errors.Wrap(err, "failed to read uint16")
	}

	return binary.LittleEndian.Uint16(b[:]), nil
}

func readUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, errors.Wrap(err, "failed to read uint32")
	}

	return binary.LittleEndian.Uint32(b[:]), nil
}

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, errors.Wrap(err, "failed to read byte")
	}

	return b[0], nil
}

func ReadCommitType(r io.Reader) (HnswCommitType, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, errors.Wrap(err, "failed to read commit type")
	}

	return HnswCommitType(b[0]), nil
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
