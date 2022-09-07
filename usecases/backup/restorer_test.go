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

package backup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/stretchr/testify/mock"
)

// ErrAny represent a random error
var ErrAny = errors.New("any error")

func TestRestoreStatus(t *testing.T) {
	var (
		backendType = "s3"
		id          = "1234"
		m           = createManager(nil, nil, nil)
		ctx         = context.Background()
		starTime    = time.Now().UTC()
		path        = "bucket/backups/123"
	)
	// initial state
	_, err := m.RestorationStatus(ctx, nil, backendType, id)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("must return an error if backup doesn't exist")
	}
	// active state
	m.restorer.lastStatus.reqStat = reqStat{
		Starttime: starTime,
		ID:        id,
		Status:    backup.Transferring,
		path:      path,
	}
	st, err := m.RestorationStatus(ctx, nil, backendType, id)
	if err != nil {
		t.Errorf("get active status: %v", err)
	}
	expected := RestoreStatus{Path: path, StartedAt: starTime, Status: backup.Transferring}
	if expected != st {
		t.Errorf("get active status: got=%v want=%v", st, expected)
	}
	// cached status
	m.restorer.lastStatus.reset()
	st.CompletedAt = starTime
	m.restoreStatusMap.Store("s3/"+id, st)
	st, err = m.RestorationStatus(ctx, nil, backendType, id)
	if err != nil {
		t.Errorf("fetch status from map: %v", err)
	}
	expected.CompletedAt = starTime
	if expected != st {
		t.Errorf("fetch status from map got=%v want=%v", st, expected)
	}
}

func TestRestoreRequestValidation(t *testing.T) {
	var (
		cls         = "MyClass"
		backendName = "s3"
		id          = "1234"
		m           = createManager(nil, nil, nil)
		ctx         = context.Background()
		path        = "bucket/backups"
		req         = &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{},
		}
	)

	_, err := m.Restore(ctx, nil, &BackupRequest{
		Backend: backendName,
		ID:      id,
		Include: []string{cls},
		Exclude: []string{cls},
	})
	if err == nil {
		t.Errorf("must return an error for non empty include and exclude")
	}
	{ //  backend provider fails
		backend := &fakeBackend{}
		m2 := createManager(nil, backend, ErrAny)
		_, err = m2.Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{},
		})
		if err == nil || !strings.Contains(err.Error(), backendName) {
			t.Errorf("must return an error if it fails to get backend provider: %v", err)
		}
	}
	{ //  fail to get meta data
		backend := &fakeBackend{}
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(nil, ErrAny)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err = m2.Restore(ctx, nil, req)
		if err == nil || !strings.Contains(err.Error(), "find") {
			t.Errorf("must return an error if it fails to get meta data: %v", err)
		}
		// meta data not found
		backend = &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(nil, backup.ErrNotFound{})
		m3 := createManager(nil, backend, nil)

		_, err = m3.Restore(ctx, nil, req)
		if _, ok := err.(backup.ErrNotFound); !ok {
			t.Errorf("must return an error if meta data doesn't exist: %v", err)
		}
	}

	{ //  failed backup
		backend := &fakeBackend{}
		bytes := marshalMeta(backup.BackupDescriptor{Status: string(backup.Failed)})
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err = m2.Restore(ctx, nil, req)
		if err == nil {
			t.Errorf("must return an error backup failed: %v", err)
		}
	}
	{ //  backup was successful requst include unknown class
		backend := &fakeBackend{}
		bytes := marshalMeta(
			backup.BackupDescriptor{
				Status:  string(backup.Success),
				Classes: []backup.ClassDescriptor{{Name: cls}},
			},
		)
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err = m2.Restore(ctx, nil, &BackupRequest{ID: id, Include: []string{"unknown"}})
		if err == nil || !strings.Contains(err.Error(), "unknown") {
			t.Errorf("must return an error if any class in 'include' doesn't exist in metadata: %v", err)
		}
	}
	{ //  backup was successful but class list is empty
		backend := &fakeBackend{}
		bytes := marshalMeta(
			backup.BackupDescriptor{
				Status:  string(backup.Success),
				Classes: []backup.ClassDescriptor{{Name: cls}},
			},
		)
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, backend, nil)
		_, err = m2.Restore(ctx, nil, &BackupRequest{ID: id, Exclude: []string{cls}})
		if err == nil || !strings.Contains(err.Error(), "empty") {
			t.Errorf("must return an error resulting list of classes is empty: %v", err)
		}
	}
	{ //  one class exists already in DB
		backend := &fakeBackend{}
		sourcer := &fakeSourcer{}
		sourcer.On("ClassExists", cls).Return(true)
		bytes := marshalMeta(
			backup.BackupDescriptor{
				Status:  string(backup.Success),
				Classes: []backup.ClassDescriptor{{Name: cls}},
			},
		)
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(sourcer, backend, nil)
		_, err = m2.Restore(ctx, nil, &BackupRequest{ID: id})
		if err == nil || !strings.Contains(err.Error(), cls) {
			t.Errorf("must return an error if a class exits already: %v", err)
		}
		uerr := backup.ErrUnprocessable{}
		if !errors.As(err, &uerr) {
			t.Errorf("error want=%v got=%v", uerr, err)
		}

	}
}

func marshalMeta(m backup.BackupDescriptor) []byte {
	bytes, _ := json.MarshalIndent(m, "", "")
	return bytes
}
