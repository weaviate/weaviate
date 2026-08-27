//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modstgfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_StoreBackup(t *testing.T) {
	backupRelativePath := filepath.Join("./backups", "some", "nested", "dir")
	backupAbsolutePath := t.TempDir()

	ctx := context.Background()

	t.Run("fails init fs module with empty backup path", func(t *testing.T) {
		module := New()
		err := module.initBackupBackend(ctx, "")

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "empty backup path provided")
	})

	t.Run("fails init fs module with relative backup path", func(t *testing.T) {
		module := New()
		err := module.initBackupBackend(ctx, backupRelativePath)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "relative backup path provided")
	})

	t.Run("inits backup module with absolute backup path", func(t *testing.T) {
		module := New()
		err := module.initBackupBackend(ctx, backupAbsolutePath)

		assert.Nil(t, err)

		_, err = os.Stat(backupAbsolutePath)
		assert.Nil(t, err)
	})
}

func TestResolvePath(t *testing.T) {
	tests := []struct {
		name         string
		backupsPath  string
		overridePath string
		wantPath     string
		wantErr      string
	}{
		{
			name:        "uses config path when no override",
			backupsPath: "/var/backups",
			wantPath:    "/var/backups",
		},
		{
			name:         "override replaces config path",
			backupsPath:  "/var/backups",
			overridePath: "/tmp/exports",
			wantPath:     "/tmp/exports",
		},
		{
			name:    "empty config path without override returns error",
			wantErr: "backup path must not be empty",
		},
		{
			name:         "empty config path with override succeeds",
			overridePath: "/tmp/exports",
			wantPath:     "/tmp/exports",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Module{backupsPath: tt.backupsPath}
			p, err := m.resolvePath(tt.overridePath)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantPath, p)
			}
		})
	}
}

// stubReader yields payload and then fails with readErr, or ends cleanly when
// readErr is nil. It records the error Write signals back to the producer.
type stubReader struct {
	payload    *bytes.Reader
	readErr    error
	closedWith error
}

func (s *stubReader) Read(p []byte) (int, error) {
	if s.payload.Len() > 0 {
		return s.payload.Read(p)
	}
	if s.readErr != nil {
		return 0, s.readErr
	}
	return 0, io.EOF
}

func (s *stubReader) Close() error { return nil }

func (s *stubReader) CloseWithError(err error) error {
	s.closedWith = err
	return nil
}

func TestWriteStoresOnlyCompleteFiles(t *testing.T) {
	readErr := errors.New("scan failed")

	tests := []struct {
		name string
		// content already at the target path before the write, if any
		existing    string
		payload     string
		readErr     error
		wantContent string
		wantMissing bool
	}{
		{
			name:        "complete copy stores the file",
			payload:     "full-payload",
			wantContent: "full-payload",
		},
		{
			name:        "empty payload stores an empty file",
			wantContent: "",
		},
		{
			name:        "read failing mid-stream stores nothing",
			payload:     "truncated-par",
			readErr:     readErr,
			wantMissing: true,
		},
		{
			name:        "read failing before any byte stores nothing",
			readErr:     readErr,
			wantMissing: true,
		},
		{
			name:        "shorter rewrite leaves no trailing bytes",
			existing:    "a-much-longer-previous-payload",
			payload:     "short",
			wantContent: "short",
		},
		{
			name:        "failed rewrite keeps the existing file",
			existing:    "a-much-longer-previous-payload",
			payload:     "short",
			readErr:     readErr,
			wantContent: "a-much-longer-previous-payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			basePath := t.TempDir()
			backupPath := filepath.Join(basePath, "backup-1", "chunk-0")

			if tt.existing != "" {
				require.NoError(t, os.MkdirAll(filepath.Dir(backupPath), os.ModePerm))
				require.NoError(t, os.WriteFile(backupPath, []byte(tt.existing), os.ModePerm))
			}

			m := &Module{backupsPath: basePath, logger: logrus.New()}
			r := &stubReader{payload: bytes.NewReader([]byte(tt.payload)), readErr: tt.readErr}

			written, err := m.Write(context.Background(), "backup-1", "chunk-0", "", "", r)

			if tt.readErr != nil {
				require.ErrorIs(t, err, tt.readErr)
				require.ErrorIs(t, r.closedWith, tt.readErr)
			} else {
				require.NoError(t, err)
				require.NoError(t, r.closedWith)
			}
			// written counts bytes read off the producer, not bytes stored: on the
			// error path the copied bytes are discarded rather than kept.
			assert.Equal(t, int64(len(tt.payload)), written)

			content, err := os.ReadFile(backupPath)
			if tt.wantMissing {
				require.ErrorIs(t, err, os.ErrNotExist, "no file may be left behind")
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantContent, string(content))
			}

			_, err = os.Stat(backupPath + ".tmp")
			require.ErrorIs(t, err, os.ErrNotExist, "temp file must not survive the write")
		})
	}
}
