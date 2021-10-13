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
	_ "fmt"
	"math/rand"
	"testing"

	std_bufio "bufio"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func BenchmarkCondensorUint64Write(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor(logger)
	// c.newLog =  NewWriterSize(c.newLogFile, 1*1024*1024)
	c.newLog = std_bufio.NewWriterSize(c.newLogFile, 1*1024*1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeUint64(c.newLog, rand.Uint64())
	}
}

func BenchmarkCondensor2NewUint64Write(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor2(logger)
	c.newLog = NewWriterSize(c.newLogFile, 1*1024*1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeUint64(c.newLog, rand.Uint64())
	}
}

func BenchmarkCondensorUint16Write(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor(logger)
	c.newLog = std_bufio.NewWriterSize(c.newLogFile, 1*1024*1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeUint16(c.newLog, uint16(rand.Uint32()))
	}
}

func BenchmarkCondensor2NewUint16Write(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor2(logger)
	c.newLog = NewWriterSize(c.newLogFile, 1*1024*1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeUint16(c.newLog, uint16(rand.Uint32()))
	}
}

func BenchmarkCondensorWriteCommitType(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor(logger)
	c.newLog = std_bufio.NewWriterSize(c.newLogFile, 1*1024*1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeCommitType(c.newLog, HnswCommitType(1))
	}
}

func BenchmarkCondensor2WriteCommitType(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor2(logger)
	c.newLog = NewWriterSize(c.newLogFile, 1*1024*1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeCommitType(c.newLog, HnswCommitType(1))
	}
}

func BenchmarkCondensorWriteUint64Slice(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor(logger)
	c.newLog = std_bufio.NewWriterSize(c.newLogFile, 1*1024*1024)
	testInts := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		testInts[i] = rand.Uint64()
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeUint64Slice(c.newLog, testInts)
	}
}

func BenchmarkCondensor2WriteUint64Slice(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor2(logger)
	c.newLog = NewWriterSize(c.newLogFile, 1*1024*1024)
	testInts := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		testInts[i] = rand.Uint64()
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeUint64Slice(c.newLog, testInts)
	}
}

func TestNewCondensorEqualsOld(t *testing.T) {
	logger1, _ := test.NewNullLogger()
	c1 := NewMemoryCondensor(logger1)
	c1.newLog = std_bufio.NewWriterSize(c1.newLogFile, 1*1024*1024)

	err := c1.writeUint64(c1.newLog, uint64(1))
	if err != nil {
		t.Error(err)
	}

	logger2, _ := test.NewNullLogger()
	c2 := NewMemoryCondensor2(logger2)
	c2.newLog = NewWriterSize(c1.newLogFile, 1*1024*1024)
	c2.writeUint64(c2.newLog, uint64(1))

	assert.Equal(t, c1.newLogFile, c2.newLogFile)
}
