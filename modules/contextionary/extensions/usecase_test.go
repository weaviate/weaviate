package extensions

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
