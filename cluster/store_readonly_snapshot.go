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

package cluster

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

// These mirror the on-disk layout of raft's FileSnapshotStore (which we cannot
// instantiate on a read-only follower because its constructor write-tests the
// directory). Keep them in sync with hashicorp/raft/file_snapshot.go.
const (
	roSnapDirName    = "snapshots"
	roSnapMetaFile   = "meta.json"
	roSnapStateFile  = "state.bin"
	roSnapTmpSuffix  = ".tmp"
	roSnapVersionMin = raft.SnapshotVersion(0)
	roSnapVersionMax = raft.SnapshotVersionMax
)

// readOnlySnapshotStore is a raft.SnapshotStore that reads the snapshots a
// writer persisted, without ever writing. A read-only follower uses it in place
// of raft.FileSnapshotStore: the latter's constructor performs a write
// permission probe (os.Create + os.Remove of a temp file under the raft dir),
// which fails on a kernel-read-only mount. This shim implements only the read
// half of the interface (List + Open); Create returns an error because a
// follower never produces snapshots.
type readOnlySnapshotStore struct {
	// path is <raftWorkDir>/snapshots, matching FileSnapshotStore's layout.
	path   string
	logger logrus.FieldLogger
}

var _ raft.SnapshotStore = (*readOnlySnapshotStore)(nil)

func newReadOnlySnapshotStore(workDir string, logger logrus.FieldLogger) *readOnlySnapshotStore {
	return &readOnlySnapshotStore{
		path:   filepath.Join(workDir, roSnapDirName),
		logger: logger,
	}
}

// roSnapshotMeta is the on-disk meta.json shape (SnapshotMeta plus a CRC),
// mirroring raft's unexported fileSnapshotMeta.
type roSnapshotMeta struct {
	raft.SnapshotMeta
	CRC []byte
}

func (s *readOnlySnapshotStore) Create(raft.SnapshotVersion, uint64, uint64,
	raft.Configuration, uint64, raft.Transport,
) (raft.SnapshotSink, error) {
	return nil, errors.New("read-only follower: cannot create raft snapshots")
}

// List returns the known snapshots, newest (highest index) first.
func (s *readOnlySnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	metas, err := s.getSnapshots()
	if err != nil {
		return nil, err
	}
	out := make([]*raft.SnapshotMeta, 0, len(metas))
	for _, m := range metas {
		meta := m.SnapshotMeta
		out = append(out, &meta)
	}
	return out, nil
}

func (s *readOnlySnapshotStore) getSnapshots() ([]*roSnapshotMeta, error) {
	entries, err := os.ReadDir(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			// No snapshots directory in the copy: a fresh writer that never
			// snapshotted. Not an error — recovery proceeds from the log.
			return nil, nil
		}
		return nil, fmt.Errorf("scan snapshot directory %q: %w", s.path, err)
	}

	var metas []*roSnapshotMeta
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), roSnapTmpSuffix) {
			// a temporary (incomplete) snapshot from an interrupted write
			continue
		}
		meta, err := s.readMeta(e.Name())
		if err != nil {
			s.logger.WithField("snapshot", e.Name()).
				Warnf("read-only follower: skipping unreadable snapshot meta: %v", err)
			continue
		}
		if meta.Version < roSnapVersionMin || meta.Version > roSnapVersionMax {
			s.logger.WithField("snapshot", e.Name()).
				Warnf("read-only follower: skipping snapshot with unsupported version %d", meta.Version)
			continue
		}
		metas = append(metas, meta)
	}

	// newest first (highest index)
	sort.Slice(metas, func(i, j int) bool {
		if metas[i].Term != metas[j].Term {
			return metas[i].Term > metas[j].Term
		}
		return metas[i].Index > metas[j].Index
	})
	return metas, nil
}

func (s *readOnlySnapshotStore) readMeta(name string) (*roSnapshotMeta, error) {
	metaPath := filepath.Join(s.path, name, roSnapMetaFile)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	meta := &roSnapshotMeta{}
	if err := json.NewDecoder(bufio.NewReader(fh)).Decode(meta); err != nil {
		return nil, fmt.Errorf("decode %q: %w", metaPath, err)
	}
	return meta, nil
}

// Open returns the state of the snapshot with the given id, verifying its CRC
// exactly as FileSnapshotStore does.
func (s *readOnlySnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	meta, err := s.readMeta(id)
	if err != nil {
		return nil, nil, fmt.Errorf("read meta for snapshot %q: %w", id, err)
	}

	statePath := filepath.Join(s.path, id, roSnapStateFile)
	fh, err := os.Open(statePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open state file for snapshot %q: %w", id, err)
	}

	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := io.Copy(stateHash, fh); err != nil {
		fh.Close()
		return nil, nil, fmt.Errorf("read state file for snapshot %q: %w", id, err)
	}
	if computed := stateHash.Sum(nil); !bytes.Equal(meta.CRC, computed) {
		fh.Close()
		return nil, nil, fmt.Errorf("snapshot %q CRC mismatch: stored %x computed %x", id, meta.CRC, computed)
	}

	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		fh.Close()
		return nil, nil, fmt.Errorf("seek state file for snapshot %q: %w", id, err)
	}

	sm := meta.SnapshotMeta
	return &sm, &roBufferedFile{r: bufio.NewReader(fh), f: fh}, nil
}

// roBufferedFile buffers reads while keeping the underlying file closeable,
// mirroring raft's bufferedFile.
type roBufferedFile struct {
	r *bufio.Reader
	f *os.File
}

func (b *roBufferedFile) Read(p []byte) (int, error) { return b.r.Read(p) }
func (b *roBufferedFile) Close() error               { return b.f.Close() }
