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
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
)

func BenchmarkDeserializerReadUint64(b *testing.B) {
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

func BenchmarkDeserializer2ReadUint64(b *testing.B) {
	b.StopTimer()

	randUint64 := rand.Uint64()

	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, uint64(randUint64))
	data := bytes.NewReader(val)
	logger, _ := test.NewNullLogger()
	d := NewDeserializer2(logger)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		d.readUint64(reader)
	}
}

func BenchmarkDeserializerReadUint16(b *testing.B) {
	b.StopTimer()

	randUint16 := uint16(rand.Uint32())

	val := make([]byte, 2)
	binary.LittleEndian.PutUint16(val, randUint16)
	data := bytes.NewReader(val)
	logger, _ := test.NewNullLogger()
	d := NewDeserializer(logger)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		d.readUint16(reader)
	}
}

func BenchmarkDeserializer2ReadUint16(b *testing.B) {
	b.StopTimer()

	randUint16 := uint16(rand.Uint32())

	val := make([]byte, 2)
	binary.LittleEndian.PutUint16(val, randUint16)
	data := bytes.NewReader(val)
	logger, _ := test.NewNullLogger()
	d := NewDeserializer2(logger)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		d.readUint16(reader)
	}
}

func BenchmarkDeserializerReadCommitType(b *testing.B) {
	b.StopTimer()

	commitType := SetEntryPointMaxLevel

	val := make([]byte, 1)
	val[0] = byte(commitType)
	data := bytes.NewReader(val)
	logger, _ := test.NewNullLogger()
	d := NewDeserializer(logger)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		d.ReadCommitType(reader)
	}
}

func BenchmarkDeserializer2ReadCommitType(b *testing.B) {
	b.StopTimer()

	commitType := SetEntryPointMaxLevel

	val := make([]byte, 1)
	val[0] = byte(commitType)
	data := bytes.NewReader(val)
	logger, _ := test.NewNullLogger()
	d := NewDeserializer2(logger)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		d.ReadCommitType(reader)
	}
}

func BenchmarkDeserializerReadUint64Slice(b *testing.B) {
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

func BenchmarkDeserializer2ReadUint64Slice(b *testing.B) {
	b.StopTimer()

	uint64Slice := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	val := make([]byte, len(uint64Slice)*8)
	for i, v := range uint64Slice {
		binary.LittleEndian.PutUint64(val[i*8:], uint64(v))
	}

	data := bytes.NewReader(val)
	logger, _ := test.NewNullLogger()
	d := NewDeserializer2(logger)
	reader := bufio.NewReader(data)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		d.readUint64Slice(reader, len(uint64Slice))
	}
}

func TestDeserializer2EqualsOriginalDeserializer(t *testing.T) {
	for i := 0; i < 100_000; i++ {
		randUint64 := rand.Uint64()

		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(randUint64))
		data := bytes.NewReader(b)

		logger, _ := test.NewNullLogger()
		d1 := NewDeserializer(logger)
		d2 := NewDeserializer2(logger)
		res1, _ := d1.readUint64(data)
		data.Reset(b)
		res2, err := d2.readUint64(data)
		if err != nil {
			t.Errorf("Error reading uint64 from data: %v", err)
		}
		if res1 != res2 {
			t.Errorf("Deserializers are not equal")
		}
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
	}
	for _, commitType := range commitTypes {
		b := make([]byte, 1)
		b[0] = byte(commitType)
		data := bytes.NewReader(b)
		logger, _ := test.NewNullLogger()
		d := NewDeserializer2(logger)
		reader := bufio.NewReader(data)
		res, err := d.ReadCommitType(reader)
		if err != nil {
			t.Errorf("Error reading commit type: %v", err)
		}
		if res != commitType {
			t.Errorf("Commit type is not equal")
		}

	}
}
