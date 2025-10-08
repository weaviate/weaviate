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

package flat

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

const (
	metadataPrefix       = "meta"
	vectorMetadataBucket = "vector"
)

func (index *flat) getMetadataFile() string {
	if index.targetVector != "" {
		// This may be redundant as target vector is already validated in the schema
		cleanTarget := filepath.Clean(index.targetVector)
		cleanTarget = filepath.Base(cleanTarget)
		return fmt.Sprintf("%s_%s.db", metadataPrefix, cleanTarget)
	}
	return fmt.Sprintf("%s.db", metadataPrefix)
}

func (index *flat) removeMetadataFile() error {
	path := filepath.Join(index.rootPath, index.getMetadataFile())
	index.closeMetadata()
	err := os.Remove(path)
	if err != nil {
		return errors.Wrapf(err, "remove metadata file %q", path)
	}
	return nil
}

func (index *flat) closeMetadata() {
	index.metadataLock.Lock()
	defer index.metadataLock.Unlock()

	if index.metadata != nil {
		index.metadata.Close()
		index.metadata = nil
	}
}

func (index *flat) openMetadata() error {
	index.metadataLock.Lock()
	defer index.metadataLock.Unlock()

	if index.metadata != nil {
		return nil // Already open
	}

	path := filepath.Join(index.rootPath, index.getMetadataFile())
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return errors.Wrapf(err, "open %q", path)
	}

	index.metadata = db
	return nil
}

func (index *flat) initMetadata() error {
	err := index.openMetadata()
	if err != nil {
		return err
	}
	defer index.closeMetadata()

	err = index.metadata.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(vectorMetadataBucket))
		if err != nil {
			return errors.Wrap(err, "create bucket")
		}
		if b == nil {
			return errors.New("failed to create or get bucket")
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "init metadata bucket")
	}

	index.initDimensions()

	return nil
}

func (index *flat) initDimensions() {
	dims, err := index.fetchDimensions()
	if err != nil {
		index.logger.Warnf("flat index unable to fetch dimensions: %v", err)
	}

	if dims == 0 {
		dims = index.calculateDimensions()
		if dims > 0 {
			// Backwards compatibility: set the dimensions in the metadata file
			err = index.setDimensions(dims)
			if err != nil {
				index.logger.Warnf("flat index unable to set dimensions: %v", err)
			}
		}
	}
	if dims > 0 {
		index.trackDimensionsOnce.Do(func() {
			atomic.StoreInt32(&index.dims, dims)
		})
	}
}

func (index *flat) fetchDimensions() (int32, error) {
	if index.metadata == nil {
		return 0, nil
	}

	var dimensions int32 = 0
	err := index.metadata.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vectorMetadataBucket))
		if b == nil {
			return nil
		}
		v := b.Get([]byte("dimensions"))
		if v == nil {
			return nil
		}
		dimensions = int32(binary.LittleEndian.Uint32(v))
		return nil
	})
	if err != nil {
		return 0, errors.Wrap(err, "fetch dimensions")
	}

	return dimensions, nil
}

func (index *flat) calculateDimensions() int32 {
	bucket := index.store.Bucket(index.getBucketName())
	if bucket == nil {
		return 0
	}
	cursor := bucket.Cursor()
	defer cursor.Close()

	var key []byte
	var v []byte
	const maxCursorSize = 100000
	i := 0
	for key, v = cursor.First(); key != nil; key, v = cursor.Next() {
		if len(v) > 0 {
			return int32(len(v) / 4)
		}
		if i > maxCursorSize {
			break
		}
		i++
	}
	return 0
}

func (index *flat) setDimensions(dimensions int32) error {
	err := index.openMetadata()
	if err != nil {
		return err
	}
	defer index.closeMetadata()

	err = index.metadata.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vectorMetadataBucket))
		if b == nil {
			return errors.New("failed to get bucket")
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(dimensions))
		return b.Put([]byte("dimensions"), buf)
	})
	if err != nil {
		return errors.Wrap(err, "set dimensions")
	}

	return nil
}
