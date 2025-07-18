//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func BenchmarkDeserializer2ReadUint64(b *testing.B) {
	b.StopTimer()

	randUint64 := rand.Uint64()

	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, uint64(randUint64))
	data := bytes.NewReader(val)
	logger, _ := test.NewNullLogger()
	d := NewDeserializer(logger)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		d.readUint64(reader)
	}
}

func BenchmarkDeserializer2ReadUint16(b *testing.B) {
	b.StopTimer()

	randUint16 := uint16(rand.Uint32())

	val := make([]byte, 2)
	binary.LittleEndian.PutUint16(val, randUint16)
	data := bytes.NewReader(val)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		readUint16(reader)
	}
}

func BenchmarkDeserializer2ReadCommitType(b *testing.B) {
	b.StopTimer()

	commitType := SetEntryPointMaxLevel

	val := make([]byte, 1)
	val[0] = byte(commitType)
	data := bytes.NewReader(val)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		ReadCommitType(reader)
	}
}

func BenchmarkDeserializer2ReadUint64Slice(b *testing.B) {
	b.StopTimer()

	uint64Slice := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	val := make([]byte, len(uint64Slice)*8)
	for i, v := range uint64Slice {
		binary.LittleEndian.PutUint64(val[i*8:], uint64(v))
	}

	data := bytes.NewReader(val)
	logger, _ := test.NewNullLogger()
	d := NewDeserializer(logger)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		d.readUint64Slice(reader, len(uint64Slice))
	}
}

func TestDeserializer2ReadCommitType(t *testing.T) {
	commitTypes := []HnswCommitType{
		AddNode,
		SetEntryPointMaxLevel,
		AddLinkAtLevel,
		ReplaceLinksAtLevel,
		AddTombstone,
		RemoveTombstone,
		ClearLinks,
		DeleteNode,
		ResetIndex,
		AddPQ,
	}
	for _, commitType := range commitTypes {
		b := make([]byte, 1)
		b[0] = byte(commitType)
		data := bytes.NewReader(b)
		reader := bufio.NewReader(data)
		res, err := ReadCommitType(reader)
		if err != nil {
			t.Errorf("Error reading commit type: %v", err)
		}
		if res != commitType {
			t.Errorf("Commit type is not equal")
		}

	}
}

func TestDeserializerReadDeleteNode(t *testing.T) {
	nodes := generateDummyVertices(4)
	res := &DeserializationResult{
		Nodes:        nodes,
		NodesDeleted: map[uint64]struct{}{},
	}
	ids := []uint64{2, 3, 4, 5, 6}

	for _, id := range ids {
		val := make([]byte, 8)
		binary.LittleEndian.PutUint64(val, id)
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)
		reader := bufio.NewReader(data)

		err := d.ReadDeleteNode(reader, res, res.NodesDeleted)
		if err != nil {
			t.Errorf("Error reading commit type: %v", err)
		}
	}

	for _, id := range ids {
		if _, ok := res.NodesDeleted[id]; !ok {
			t.Errorf("Node %d not marked deleted", id)
		}
	}
}

func TestDeserializerReadClearLinks(t *testing.T) {
	nodes := generateDummyVertices(4)
	res := &DeserializationResult{
		Nodes: nodes,
	}
	ids := []uint64{2, 3, 4, 5, 6}

	for _, id := range ids {
		val := make([]byte, 8)
		binary.LittleEndian.PutUint64(val, id)
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		err := d.ReadClearLinks(reader, res, true)
		if err != nil {
			t.Errorf("Error reading links: %v", err)
		}
	}
}

func dummyInitialDeserializerState() *DeserializationResult {
	conns, _ := packedconn.NewWithMaxLayer(15)
	return &DeserializationResult{
		LinksReplaced: make(map[uint64]map[uint16]struct{}),
		Nodes: []*vertex{
			nil,
			nil,
			{
				// This is a lower level than we will read, so this node will require
				// growing
				level: 1,
			},
			{
				// This is a lower level than we will read, so this node will require
				// growing
				level:       8,
				connections: conns,
			},
		},
	}
}

func TestDeserializerReadNode(t *testing.T) {
	res := dummyInitialDeserializerState()
	ids := []uint64{2, 3, 4, 5, 6}

	for _, id := range ids {
		val := make([]byte, 10)
		level := uint16(id * 2)
		binary.LittleEndian.PutUint64(val[:8], id)
		binary.LittleEndian.PutUint16(val[8:10], level)
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		err := d.ReadNode(reader, res)
		require.Nil(t, err)
		require.NotNil(t, res.Nodes[id])
		assert.Equal(t, int(level), res.Nodes[id].level)
	}
}

func TestDeserializerReadEP(t *testing.T) {
	ids := []uint64{2, 3, 4, 5, 6}

	for _, id := range ids {
		val := make([]byte, 10)
		level := uint16(id * 2)
		binary.LittleEndian.PutUint64(val[:8], id)
		binary.LittleEndian.PutUint16(val[8:10], level)
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		ep, l, err := d.ReadEP(reader)
		require.Nil(t, err)
		assert.Equal(t, id, ep)
		assert.Equal(t, level, l)
	}
}

func TestDeserializerReadLink(t *testing.T) {
	res := dummyInitialDeserializerState()
	ids := []uint64{2, 3, 4, 5, 6}

	for _, id := range ids {
		level := uint16(id * 2)
		target := id * 3
		val := make([]byte, 18)
		binary.LittleEndian.PutUint64(val[:8], id)
		binary.LittleEndian.PutUint16(val[8:10], level)
		binary.LittleEndian.PutUint64(val[10:18], target)
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		err := d.ReadLink(reader, res)
		require.Nil(t, err)
		require.NotNil(t, res.Nodes[id])
		conns := res.Nodes[id].connections.GetLayer(uint8(level))
		lastAddedConnection := conns[len(conns)-1]
		assert.Equal(t, target, lastAddedConnection)
	}
}

func TestDeserializerReadLinks(t *testing.T) {
	res := dummyInitialDeserializerState()
	ids := []uint64{2, 3, 4, 5, 6}

	for _, id := range ids {
		level := uint16(id * 2)
		connLen := uint16(id * 4)
		val := make([]byte, 12+connLen*8)
		binary.LittleEndian.PutUint64(val[:8], id)
		binary.LittleEndian.PutUint16(val[8:10], level)
		binary.LittleEndian.PutUint16(val[10:12], connLen)
		for i := 0; i < int(connLen); i++ {
			target := id + uint64(i)
			binary.LittleEndian.PutUint64(val[12+(i*8):12+(i*8+8)], target)
		}
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		_, err := d.ReadLinks(reader, res, true)
		require.Nil(t, err)
		require.NotNil(t, res.Nodes[id])
		conns := res.Nodes[id].connections.GetLayer(uint8(level))
		lastAddedConnection := conns[len(conns)-1]
		assert.Equal(t, id+uint64(connLen)-1, lastAddedConnection)
	}
}

func TestDeserializerTruncateReadLinks(t *testing.T) {
	res := dummyInitialDeserializerState()
	ids := []uint64{1, 2, 3, 4}

	for _, id := range ids {
		level := uint16(id * 2)
		var connLen uint16
		if id%2 == 0 {
			connLen = uint16(5000)
		} else {
			connLen = uint16(2000)
		}
		val := make([]byte, 12+connLen*8)
		binary.LittleEndian.PutUint64(val[:8], id)
		binary.LittleEndian.PutUint16(val[8:10], level)
		binary.LittleEndian.PutUint16(val[10:12], connLen)
		for i := 0; i < int(connLen); i++ {
			target := id + uint64(i)
			binary.LittleEndian.PutUint64(val[12+(i*8):12+(i*8+8)], target)
		}
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		_, err := d.ReadLinks(reader, res, true)
		require.Nil(t, err)
		require.NotNil(t, res.Nodes[id])
		conns := res.Nodes[id].connections.GetLayer(uint8(level))
		if id%2 == 0 {
			require.Equal(t, 4095, len(conns), "Expected connections to be truncated to 4095")
		} else {
			require.Equal(t, 2000, len(conns), "Expected connections to be read as is")
		}
	}
}

func TestDeserializerReadAddLinks(t *testing.T) {
	res := dummyInitialDeserializerState()
	ids := []uint64{2, 3, 4, 5, 6}

	for _, id := range ids {
		level := uint16(id * 2)
		connLen := uint16(id * 4)
		val := make([]byte, 12+connLen*8)
		binary.LittleEndian.PutUint64(val[:8], id)
		binary.LittleEndian.PutUint16(val[8:10], level)
		binary.LittleEndian.PutUint16(val[10:12], connLen)
		for i := 0; i < int(connLen); i++ {
			target := id + uint64(i)
			binary.LittleEndian.PutUint64(val[12+(i*8):12+(i*8+8)], target)
		}
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		_, err := d.ReadAddLinks(reader, res)
		require.Nil(t, err)
		require.NotNil(t, res.Nodes[id])
		conns := res.Nodes[id].connections.GetLayer(uint8(level))
		lastAddedConnection := conns[len(conns)-1]
		assert.Equal(t, id+uint64(connLen)-1, lastAddedConnection)
	}
}

func TestDeserializerTruncateReadAddLinks(t *testing.T) {
	res := dummyInitialDeserializerState()
	ids := []uint64{1, 2, 3, 4}

	for _, id := range ids {
		level := uint16(id * 2)
		var connLen uint16
		if id%2 == 0 {
			connLen = uint16(5000)
		} else {
			connLen = uint16(2000)
		}
		val := make([]byte, 12+connLen*8)
		binary.LittleEndian.PutUint64(val[:8], id)
		binary.LittleEndian.PutUint16(val[8:10], level)
		binary.LittleEndian.PutUint16(val[10:12], connLen)
		for i := 0; i < int(connLen); i++ {
			target := id + uint64(i)
			binary.LittleEndian.PutUint64(val[12+(i*8):12+(i*8+8)], target)
		}
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		_, err := d.ReadAddLinks(reader, res)
		require.Nil(t, err)
		require.NotNil(t, res.Nodes[id])
		conns := res.Nodes[id].connections.GetLayer(uint8(level))
		if id%2 == 0 {
			require.Equal(t, 4095, len(conns), "Expected connections to be truncated to 4095")
		} else {
			require.Equal(t, 2000, len(conns), "Expected connections to be read as is")
		}
	}
}

func TestDeserializerAddTombstone(t *testing.T) {
	tombstones := map[uint64]struct{}{}
	ids := []uint64{2, 3, 4, 5, 6}

	for _, id := range ids {
		val := make([]byte, 8)
		binary.LittleEndian.PutUint64(val[:8], id)
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		err := d.ReadAddTombstone(reader, tombstones)
		require.Nil(t, err)
	}

	expected := map[uint64]struct{}{
		2: {},
		3: {},
		4: {},
		5: {},
		6: {},
	}

	assert.Equal(t, expected, tombstones)
}

func TestDeserializerRemoveTombstone(t *testing.T) {
	tombstones := map[uint64]struct{}{
		1: {},
		2: {},
		3: {},
		4: {},
		5: {},
	}
	ids := []uint64{2, 3, 4, 5, 7}
	deletedTombstones := map[uint64]struct{}{
		6: {},
	}

	for _, id := range ids {
		val := make([]byte, 8)
		binary.LittleEndian.PutUint64(val[:8], id)
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		err := d.ReadRemoveTombstone(reader, tombstones, deletedTombstones)
		require.Nil(t, err)
	}

	expectedTombstones := map[uint64]struct{}{
		1: {},
	}

	expectedDeletedTombstones := map[uint64]struct{}{
		6: {},
		7: {},
	}

	assert.Equal(t, expectedTombstones, tombstones)
	assert.Equal(t, expectedDeletedTombstones, deletedTombstones)
}

func TestDeserializerClearLinksAtLevel(t *testing.T) {
	conns, _ := packedconn.NewWithMaxLayer(3)
	res := &DeserializationResult{
		LinksReplaced: make(map[uint64]map[uint16]struct{}),
		Nodes: []*vertex{
			nil,
			nil,
			{
				// This is a lower level than we will read, so this node will require
				// growing
				level: 1,
			},
			{
				// This is a lower level than we will read, so this node will require
				// growing
				level:       4,
				connections: conns,
			},
			nil,
			nil,
		},
	}
	ids := []uint64{2, 3, 4, 5, 6}

	for _, id := range ids {
		level := uint16(id * 2)
		val := make([]byte, 10)
		binary.LittleEndian.PutUint64(val[:8], id)
		binary.LittleEndian.PutUint16(val[8:10], level)
		data := bytes.NewReader(val)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer(logger)

		reader := bufio.NewReader(data)

		err := d.ReadClearLinksAtLevel(reader, res, true)
		require.Nil(t, err)
	}
}

func TestDeserializerTotalReadPQ(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	commitLogger, err := NewCommitLogger(rootPath, "tmpLogger", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	dimensions := 16
	centroids := 16

	t.Run("add pq data to the first log", func(t *testing.T) {
		data, _ := testinghelpers.RandomVecs(20, 0, dimensions)
		kms := make([]compressionhelpers.PQEncoder, 4)
		for i := 0; i < 4; i++ {
			kms[i] = compressionhelpers.NewKMeansEncoder(
				dimensions,
				4,
				int(i),
			)
			err := kms[i].Fit(data)
			require.Nil(t, err)
		}
		pqData := compressionhelpers.PQData{
			Ks:                  uint16(centroids),
			M:                   4,
			Dimensions:          uint16(dimensions),
			EncoderType:         compressionhelpers.UseKMeansEncoder,
			EncoderDistribution: byte(compressionhelpers.NormalEncoderDistribution),
			UseBitsEncoding:     false,
			TrainingLimit:       100_000,
			Encoders:            kms,
		}

		commitLogger.AddPQCompression(pqData)
		require.Nil(t, commitLogger.Flush())
		require.Nil(t, commitLogger.Shutdown(ctx))
	})

	t.Run("deserialize the first log", func(t *testing.T) {
		nullLogger, _ := test.NewNullLogger()
		commitLoggerPath := rootPath + "/tmpLogger.hnsw.commitlog.d"

		fileName, found, err := getCurrentCommitLogFileName(commitLoggerPath)
		require.Nil(t, err)
		require.True(t, found)

		t.Logf("name: %v\n", fileName)

		fd, err := os.Open(commitLoggerPath + "/" + fileName)
		require.Nil(t, err)

		defer fd.Close()
		fdBuf := bufio.NewReaderSize(fd, 256*1024)

		_, deserializeSize, err := NewDeserializer(nullLogger).Do(fdBuf, nil, true)
		require.Nil(t, err)

		require.Equal(t, 4*centroids*dimensions+10, deserializeSize)
		t.Logf("deserializeSize: %v\n", deserializeSize)
	})
}

func TestDeserializerTotalReadMUVERA(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	commitLogger, err := NewCommitLogger(rootPath, "tmpLogger", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	repetitions := 2
	ksim := 2
	dimensions := 5
	dprojections := 2
	t.Run("add muvera data to the first log", func(t *testing.T) {
		gaussians := [][][]float32{
			{
				{1, 2, 3, 4, 5}, // cluster 1
				{1, 2, 3, 4, 5}, // cluster 2
			}, // rep 1
			{
				{5, 6, 7, 8, 9}, // cluster 1
				{5, 6, 7, 8, 9}, // cluster 2
			}, // rep 2
		} // (repetitions, kSim, dimensions)

		s := [][][]float32{
			{
				{-1, 1, 1, -1, 1}, // dprojection 1
				{1, -1, 1, 1, -1}, // dprojection 2
			}, // rep 1
			{
				{-1, 1, 1, -1, 1}, // dprojection 1
				{1, -1, 1, 1, -1}, // dprojection 2
			}, // rep 2
		} // (repetitions, dProjections, dimensions)

		muveraData := multivector.MuveraData{
			KSim:         uint32(ksim),
			NumClusters:  uint32(math.Pow(2, float64(ksim))),
			Dimensions:   uint32(dimensions),
			DProjections: uint32(dprojections),
			Repetitions:  uint32(repetitions),
			Gaussians:    gaussians,
			S:            s,
		}

		commitLogger.AddMuvera(muveraData)
		require.Nil(t, commitLogger.Flush())
		require.Nil(t, commitLogger.Shutdown(ctx))
	})

	t.Run("deserialize the first log", func(t *testing.T) {
		nullLogger, _ := test.NewNullLogger()
		commitLoggerPath := rootPath + "/tmpLogger.hnsw.commitlog.d"

		fileName, found, err := getCurrentCommitLogFileName(commitLoggerPath)
		require.Nil(t, err)
		require.True(t, found)

		t.Logf("name: %v\n", fileName)

		fd, err := os.Open(commitLoggerPath + "/" + fileName)
		require.Nil(t, err)

		defer fd.Close()
		fdBuf := bufio.NewReaderSize(fd, 256*1024)

		_, deserializeSize, err := NewDeserializer(nullLogger).Do(fdBuf, nil, true)
		require.Nil(t, err)

		gaussianSize := 4 * repetitions * ksim * dimensions
		randomSize := 4 * repetitions * dprojections * dimensions
		require.Equal(t, gaussianSize+randomSize+21, deserializeSize)
		t.Logf("deserializeSize: %v\n", deserializeSize)
	})
}

func TestDeserializerTotalReadRQ(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	commitLogger, err := NewCommitLogger(rootPath, "tmpLogger", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	dimension := uint32(10)
	bits := uint32(8)
	rotation := compressionhelpers.FastRotation{
		OutputDim: 4,
		Rounds:    5,
		Swaps: [][]compressionhelpers.Swap{
			{
				{I: 0, J: 2},
				{I: 1, J: 3},
			},
			{
				{I: 4, J: 6},
				{I: 5, J: 7},
			},
			{
				{I: 8, J: 10},
				{I: 9, J: 11},
			},
			{
				{I: 12, J: 14},
				{I: 13, J: 15},
			},
			{
				{I: 16, J: 18},
				{I: 17, J: 19},
			},
		},
		Signs: [][]float32{
			{1, -1, 1, -1},
			{1, -1, 1, -1},
			{1, -1, 1, -1},
			{1, -1, 1, -1},
			{1, -1, 1, -1},
		},
	}
	t.Run("add rotational quantization data to the first log", func(t *testing.T) {
		rqData := compressionhelpers.RQData{
			InputDim: dimension,
			Bits:     bits,
			Rotation: rotation,
		}

		err = commitLogger.AddRQCompression(rqData)
		require.Nil(t, err)
		require.Nil(t, commitLogger.Flush())
		require.Nil(t, commitLogger.Shutdown(ctx))
	})

	t.Run("deserialize the first log", func(t *testing.T) {
		nullLogger, _ := test.NewNullLogger()
		commitLoggerPath := rootPath + "/tmpLogger.hnsw.commitlog.d"

		fileName, found, err := getCurrentCommitLogFileName(commitLoggerPath)
		require.Nil(t, err)
		require.True(t, found)

		t.Logf("name: %v\n", fileName)

		fd, err := os.Open(commitLoggerPath + "/" + fileName)
		require.Nil(t, err)

		defer fd.Close()
		fdBuf := bufio.NewReaderSize(fd, 256*1024)

		_, deserializeSize, err := NewDeserializer(nullLogger).Do(fdBuf, nil, true)
		require.Nil(t, err)

		swapSize := 2 * rotation.Rounds * (rotation.OutputDim / 2) * 2
		signSize := 4 * rotation.Rounds * rotation.OutputDim
		require.Equal(t, int(swapSize+signSize+17), deserializeSize)
		t.Logf("deserializeSize: %v\n", deserializeSize)
	})
}
