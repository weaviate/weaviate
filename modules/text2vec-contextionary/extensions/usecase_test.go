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

package extensions

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func Test_UseCase(t *testing.T) {
	storage := newFakeStorage()
	uc := NewUseCase(storage)

	t.Run("storing and loading something", func(t *testing.T) {
		err := uc.Store("concept1", []byte("value1"))
		require.Nil(t, err)

		err = uc.Store("concept2", []byte("value2"))
		require.Nil(t, err)

		val, err := uc.Load("concept1")
		require.Nil(t, err)
		assert.Equal(t, []byte("value1"), val)

		val, err = uc.Load("concept2")
		require.Nil(t, err)
		assert.Equal(t, []byte("value2"), val)

		vals, err := uc.LoadAll()
		require.Nil(t, err)
		assert.Equal(t, []byte("value1\nvalue2\n"), vals)
	})

	t.Run("when storing fails", func(t *testing.T) {
		storage.putError = fmt.Errorf("oops")
		err := uc.Store("concept1", []byte("value1"))
		assert.Equal(t, "store concept \"concept1\": oops", err.Error())
	})

	t.Run("when loading fails", func(t *testing.T) {
		storage.getError = fmt.Errorf("oops")
		_, err := uc.Load("concept1")
		assert.Equal(t, "load concept \"concept1\": oops", err.Error())
	})
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{
		store: map[string][]byte{},
	}
}

type fakeStorage struct {
	store    map[string][]byte
	getError error
	putError error
}

func (f *fakeStorage) Get(k []byte) ([]byte, error) {
	return f.store[string(k)], f.getError
}

func (f *fakeStorage) Put(k, v []byte) error {
	f.store[string(k)] = v
	return f.putError
}

func (f *fakeStorage) Scan(scan moduletools.ScanFn) error {
	var keys [][]byte
	for key := range f.store {
		keys = append(keys, []byte(key))
	}

	sort.Slice(keys, func(a, b int) bool {
		return bytes.Compare(keys[a], keys[b]) == -1
	})

	for _, key := range keys {
		scan(key, f.store[string(key)])
	}

	return nil
}
