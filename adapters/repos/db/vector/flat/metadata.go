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
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

var metadataPrefix = bytes.Repeat([]byte{0xFF}, 10)

func (index *flat) metadataKey(key []byte) []byte {
	return []byte(fmt.Sprintf("%s%s", metadataPrefix, key))
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
	key := index.metadataKey([]byte("dimensions"))
	v, err := index.store.Bucket(index.getBucketName()).Get(key)
	if v == nil || err == entlsmkv.NotFound {
		return 0, nil
	}
	if err != entlsmkv.NotFound {
		return 0, errors.Wrap(err, "fetch dimensions")
	}
	dimensions := int32(binary.LittleEndian.Uint32(v))
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
		if len(key) != 8 {
			continue
		}
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
	bucket := index.store.Bucket(index.getBucketName())
	if bucket == nil {
		return errors.New("failed to get bucket")
	}

	key := index.metadataKey([]byte("dimensions"))
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(dimensions))

	err := bucket.Put(key, buf)
	return err
}
