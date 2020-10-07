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

// +build integrationTest

package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendInvertedIndexWithFrequency(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	shard, err := NewShard("extend_invert_benchmark", &Index{Config: IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: "Test"}})
	require.Nil(t, err)

	prop := []byte("testprop")
	var before []byte

	err = shard.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("testbucket"))
		if err != nil {
			return err
		}

		b := bytes.NewBuffer(nil)

		// checksum
		_, err = b.Write([]uint8{0, 0, 0, 0})
		if err != nil {
			return err
		}

		fakeEntries := 625000
		// doc count
		count := uint32(fakeEntries)
		err = binary.Write(b, binary.LittleEndian, &count)
		if err != nil {
			return err
		}

		for i := 0; i < fakeEntries; i++ {
			// doc id
			_, err = b.Write([]uint8{1, 2, 3, 4})
			if err != nil {
				return err
			}
			// frequency
			_, err = b.Write([]uint8{1, 2, 3, 4})
			if err != nil {
				return err
			}

		}

		before = b.Bytes()
		bucket.Put(prop, before)
		return nil
	})
	require.Nil(t, err)

	var after []byte
	err = shard.db.Update(func(tx *bolt.Tx) error {
		// before := time.Now()
		bucket := tx.Bucket([]byte("testbucket"))
		err := shard.extendInvertedIndexItemWithFrequency(bucket, inverted.Countable{Data: prop}, 15, 0.5)
		if err != nil {
			return err
		}

		after = bucket.Get(prop)

		return nil
	})
	require.Nil(t, err)

	var updatedDocCount uint32
	r := bytes.NewReader(after[4:])
	err = binary.Read(r, binary.LittleEndian, &updatedDocCount)
	require.Nil(t, err)

	assert.Equal(t, uint32(625001), updatedDocCount)

	assert.Equal(t, before[8:], after[8:(len(after))-8],
		"without the meta and the extension, the rest should be unchanged")

	r = bytes.NewReader(after[len(after)-8:])
	var newDocID uint32
	var newFrequency float32
	err = binary.Read(r, binary.LittleEndian, &newDocID)
	require.Nil(t, err)
	err = binary.Read(r, binary.LittleEndian, &newFrequency)
	require.Nil(t, err)

	assert.Equal(t, uint32(15), newDocID)
	assert.Equal(t, float32(0.5), newFrequency)
}

func TestExtendInvertedIndexWithOutFrequency(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	shard, err := NewShard("extend_invert_benchmark_no_frequency", &Index{Config: IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: "Test"}})
	require.Nil(t, err)

	prop := []byte("testprop")
	var before []byte

	err = shard.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("testbucket"))
		if err != nil {
			return err
		}

		b := bytes.NewBuffer(nil)

		// checksum
		_, err = b.Write([]uint8{0, 0, 0, 0})
		if err != nil {
			return err
		}

		fakeEntries := 625000
		// doc count
		count := uint32(fakeEntries)
		err = binary.Write(b, binary.LittleEndian, &count)
		if err != nil {
			return err
		}

		for i := 0; i < fakeEntries; i++ {
			// doc id
			_, err = b.Write([]uint8{1, 2, 3, 4})
			if err != nil {
				return err
			}
		}

		before = b.Bytes()
		bucket.Put(prop, before)
		return nil
	})
	require.Nil(t, err)

	var after []byte
	err = shard.db.Update(func(tx *bolt.Tx) error {
		// before := time.Now()
		bucket := tx.Bucket([]byte("testbucket"))
		err := shard.extendInvertedIndexItem(bucket, inverted.Countable{Data: prop}, 32)
		if err != nil {
			return err
		}

		after = bucket.Get(prop)

		return nil
	})
	require.Nil(t, err)

	var updatedDocCount uint32
	r := bytes.NewReader(after[4:])
	err = binary.Read(r, binary.LittleEndian, &updatedDocCount)
	require.Nil(t, err)

	assert.Equal(t, uint32(625001), updatedDocCount)

	assert.Equal(t, before[8:], after[8:(len(after))-4],
		"without the meta and the extension, the rest should be unchanged")

	r = bytes.NewReader(after[len(after)-4:])
	var newDocID uint32
	err = binary.Read(r, binary.LittleEndian, &newDocID)
	require.Nil(t, err)

	assert.Equal(t, uint32(32), newDocID)
}
