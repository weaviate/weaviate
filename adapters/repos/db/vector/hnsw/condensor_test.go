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
	_ "fmt"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
)

func BenchmarkCondensor2NewUint64Write(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor(logger)
	c.newLog = NewWriterSize(c.newLogFile, 1*1024*1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeUint64(c.newLog, rand.Uint64())
	}
}

func BenchmarkCondensor2NewUint16Write(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor(logger)
	c.newLog = NewWriterSize(c.newLogFile, 1*1024*1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeUint16(c.newLog, uint16(rand.Uint32()))
	}
}

func BenchmarkCondensor2WriteCommitType(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor(logger)
	c.newLog = NewWriterSize(c.newLogFile, 1*1024*1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.writeCommitType(c.newLog, HnswCommitType(1))
	}
}

func BenchmarkCondensor2WriteUint64Slice(b *testing.B) {
	b.StopTimer()
	logger, _ := test.NewNullLogger()
	c := NewMemoryCondensor(logger)
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
