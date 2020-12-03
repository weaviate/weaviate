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

func getDocumentFrequencyValue(id uint32, frequency []byte) []byte {
	documentID := uint32(id)
	keyBuf := bytes.NewBuffer(nil)
	binary.Write(keyBuf, binary.LittleEndian, &documentID)
	if frequency != nil {
		binary.Write(keyBuf, binary.LittleEndian, &frequency)
	}
	return keyBuf.Bytes()
}

func TestExtendInvertedIndexWithFrequency(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()
	index, err := NewIndex(IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: "Test",
	}, &fakeSchemaGetter{}, nil, nil)
	require.Nil(t, err)
	shard, err := NewShard("extend_invert_benchmark", index)
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
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	index, err := NewIndex(IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: "Test",
	}, &fakeSchemaGetter{}, nil, nil)
	require.Nil(t, err)
	shard, err := NewShard("extend_invert_benchmark_no_frequency", index)
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

func TestCleanupInvertedIndexWithPropWithoutFrequency(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()
	index, err := NewIndex(IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: "Test",
	}, &fakeSchemaGetter{}, nil, nil)
	require.Nil(t, err)
	shard, err := NewShard("extend_invert_benchmark", index)
	require.Nil(t, err)

	documentID := uint32(15)

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

		fakeEntries := 2
		// doc count
		count := uint32(fakeEntries)
		err = binary.Write(b, binary.LittleEndian, &count)
		if err != nil {
			return err
		}

		// doc id
		_, err = b.Write(getDocumentFrequencyValue(10, nil))
		if err != nil {
			return err
		}

		// doc id
		_, err = b.Write(getDocumentFrequencyValue(documentID, nil))
		if err != nil {
			return err
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
		err := shard.tryDeleteFromInvertedIndicesProp(bucket, inverted.Countable{Data: prop}, []uint32{documentID}, false)
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

	afterDocIDs := after[8:]
	expectedDocIDs := getDocumentFrequencyValue(10, nil)

	assert.Equal(t, uint32(1), updatedDocCount)
	assert.Equal(t, expectedDocIDs, afterDocIDs)
}

func TestCleanupInvertedIndexWithFrequencyProp(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()
	index, err := NewIndex(IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: "Test",
	}, &fakeSchemaGetter{}, nil, nil)
	require.Nil(t, err)
	shard, err := NewShard("extend_invert_benchmark", index)
	require.Nil(t, err)

	documentID := uint32(15)
	frequency := []uint8{1, 2, 3, 4}

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

		fakeEntries := 3
		// doc count
		count := uint32(fakeEntries)
		err = binary.Write(b, binary.LittleEndian, &count)
		if err != nil {
			return err
		}

		// doc id with frequency
		_, err = b.Write(getDocumentFrequencyValue(10, frequency))
		if err != nil {
			return err
		}

		// doc id with frequency
		_, err = b.Write(getDocumentFrequencyValue(documentID, frequency))
		if err != nil {
			return err
		}

		// doc id with frequency
		_, err = b.Write(getDocumentFrequencyValue(11, frequency))
		if err != nil {
			return err
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
		err := shard.tryDeleteFromInvertedIndicesProp(bucket, inverted.Countable{Data: prop}, []uint32{documentID}, true)
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

	afterDocIDs := after[8:]
	expectedDocIDsBuffer := bytes.NewBuffer(nil)
	binary.Write(expectedDocIDsBuffer, binary.LittleEndian, getDocumentFrequencyValue(10, frequency))
	binary.Write(expectedDocIDsBuffer, binary.LittleEndian, getDocumentFrequencyValue(11, frequency))
	expectedDocIDs := expectedDocIDsBuffer.Bytes()

	assert.Equal(t, uint32(2), updatedDocCount)
	assert.Equal(t, expectedDocIDs, afterDocIDs)
}

func TestCleanupInvertedIndexDeleteAllDocumentIDs(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()
	index, err := NewIndex(IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: "Test",
	}, &fakeSchemaGetter{}, nil, nil)
	require.Nil(t, err)
	shard, err := NewShard("extend_invert_benchmark", index)
	require.Nil(t, err)

	documentID1 := uint32(11)
	documentID2 := uint32(15)

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

		fakeEntries := 2
		// doc count
		count := uint32(fakeEntries)
		err = binary.Write(b, binary.LittleEndian, &count)
		if err != nil {
			return err
		}

		// doc id
		_, err = b.Write(getDocumentFrequencyValue(documentID1, nil))
		if err != nil {
			return err
		}

		// doc id
		_, err = b.Write(getDocumentFrequencyValue(documentID2, nil))
		if err != nil {
			return err
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
		err := shard.tryDeleteFromInvertedIndicesProp(bucket, inverted.Countable{Data: prop}, []uint32{documentID1, documentID2}, false)
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

	afterDocIDs := after[8:]

	assert.Equal(t, uint32(0), updatedDocCount)
	assert.Equal(t, []byte{}, afterDocIDs)
}

func TestCleanupInvertedIndexWithNoPropsToClean(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()
	index, err := NewIndex(IndexConfig{
		RootPath: dirName, Kind: kind.Thing, ClassName: "Test",
	}, &fakeSchemaGetter{}, nil, nil)
	require.Nil(t, err)
	shard, err := NewShard("extend_invert_benchmark", index)
	require.Nil(t, err)

	documentID1 := uint32(11)
	documentID2 := uint32(15)
	documentIDNotInRow := uint32(20)

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

		fakeEntries := 2
		// doc count
		count := uint32(fakeEntries)
		err = binary.Write(b, binary.LittleEndian, &count)
		if err != nil {
			return err
		}

		// doc id
		_, err = b.Write(getDocumentFrequencyValue(documentID1, nil))
		if err != nil {
			return err
		}

		// doc id
		_, err = b.Write(getDocumentFrequencyValue(documentID2, nil))
		if err != nil {
			return err
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
		err := shard.tryDeleteFromInvertedIndicesProp(bucket, inverted.Countable{Data: prop}, []uint32{documentIDNotInRow}, false)
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

	afterDocIDs := after[8:]
	expectedDocIDsBuffer := bytes.NewBuffer(nil)
	binary.Write(expectedDocIDsBuffer, binary.LittleEndian, getDocumentFrequencyValue(documentID1, nil))
	binary.Write(expectedDocIDsBuffer, binary.LittleEndian, getDocumentFrequencyValue(documentID2, nil))
	expectedDocIDs := expectedDocIDsBuffer.Bytes()

	assert.Equal(t, uint32(2), updatedDocCount)
	assert.Equal(t, expectedDocIDs, afterDocIDs)
}
