package backup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/stretchr/testify/mock"
)

func TestRestoreRequestValidation(t *testing.T) {
	var (
		cls         = "MyClass"
		storageType = "s3"
		id          = "1234"
		m           = createManager(nil, nil, nil)
		ctx         = context.Background()
		anyErr      = errors.New("any error")
		path        = "bucket/backups"
		req         = &BackupRequest{
			StorageType: storageType,
			ID:          id,
			Include:     []string{cls},
			Exclude:     []string{},
		}
	)

	_, err := m.Restore(ctx, nil, &BackupRequest{
		StorageType: storageType,
		ID:          id,
		Include:     []string{cls},
		Exclude:     []string{cls},
	})
	if err == nil {
		t.Errorf("must return an error for non empty include and exclude")
	}
	{ //  storage provider fails
		storage := &fakeStorage{}
		m2 := createManager(nil, storage, anyErr)
		_, err = m2.Restore(ctx, nil, &BackupRequest{
			StorageType: storageType,
			ID:          id,
			Include:     []string{cls},
			Exclude:     []string{},
		})
		if err == nil || !strings.Contains(err.Error(), storageType) {
			t.Errorf("must return an error if it fails to get storage provider: %v", err)
		}
	}
	{ //  fail to get meta data
		storage := &fakeStorage{}
		storage.On("GetObject", ctx, id, MetaDataFilename).Return(nil, anyErr)
		storage.On("DestinationPath", mock.Anything).Return(path)
		m2 := createManager(nil, storage, nil)
		_, err = m2.Restore(ctx, nil, req)
		if err == nil || !strings.Contains(err.Error(), "find") {
			t.Errorf("must return an error if it fails to get meta data: %v", err)
		}
		// meta data not found
		storage = &fakeStorage{}
		storage.On("DestinationPath", mock.Anything).Return(path)
		storage.On("GetObject", ctx, id, MetaDataFilename).Return(nil, backup.ErrNotFound{})
		m3 := createManager(nil, storage, nil)

		_, err = m3.Restore(ctx, nil, req)
		if _, ok := err.(backup.ErrNotFound); !ok {
			t.Errorf("must return an error if meta data doesn't exist: %v", err)
		}
	}

	{ //  failed backup
		storage := &fakeStorage{}
		bytes := marshalMeta(backup.BackupDescriptor{Status: string(backup.Failed)})
		storage.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		storage.On("DestinationPath", mock.Anything).Return(path)
		m2 := createManager(nil, storage, nil)
		_, err = m2.Restore(ctx, nil, req)
		if err == nil {
			t.Errorf("must return an error backup failed: %v", err)
		}
	}
	{ //  backup was successful requst include unknown class
		storage := &fakeStorage{}
		bytes := marshalMeta(
			backup.BackupDescriptor{
				Status:  string(backup.Success),
				Classes: []backup.ClassDescriptor{{Name: cls}},
			},
		)
		storage.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		storage.On("DestinationPath", mock.Anything).Return(path)
		m2 := createManager(nil, storage, nil)
		_, err = m2.Restore(ctx, nil, &BackupRequest{ID: id, Include: []string{"unknown"}})
		if err == nil || !strings.Contains(err.Error(), "unknown") {
			t.Errorf("must return an error if any class in 'include' doesn't exist in metadata: %v", err)
		}
	}
	{ //  backup was successful but class list is empty
		storage := &fakeStorage{}
		bytes := marshalMeta(
			backup.BackupDescriptor{
				Status:  string(backup.Success),
				Classes: []backup.ClassDescriptor{{Name: cls}},
			},
		)
		storage.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		storage.On("DestinationPath", mock.Anything).Return(path)
		m2 := createManager(nil, storage, nil)
		_, err = m2.Restore(ctx, nil, &BackupRequest{ID: id, Exclude: []string{cls}})
		if err == nil || !strings.Contains(err.Error(), "empty") {
			t.Errorf("must return an error resulting list of classes is empty: %v", err)
		}
	}
}

func marshalMeta(m backup.BackupDescriptor) []byte {
	bytes, _ := json.MarshalIndent(m, "", "")
	return bytes
}
