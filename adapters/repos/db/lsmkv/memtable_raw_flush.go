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

package lsmkv

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

func (m *Memtable) rawFlush() (rerr error) {
	// close the commit log first, this also forces it to be fsynced. If
	// something fails there, don't proceed with flushing. The commit log will
	// only be deleted at the very end, if the flush was successful
	// (indicated by a successful close of the flush file - which indicates a
	// successful fsync)

	if err := m.commitlog.close(); err != nil {
		return errors.Wrap(err, "close commit log file")
	}

	if m.Size() == 0 {
		// this is an empty memtable, nothing to do
		// however, we still have to cleanup the commit log, otherwise we will
		// attempt to recover from it on the next cycle
		if err := m.commitlog.delete(); err != nil {
			return errors.Wrap(err, "delete commit log file")
		}
		return nil
	}

	tmpMemtablePath := m.path + ".mem.tmp"

	f, err := os.OpenFile(tmpMemtablePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer func() {
		if rerr != nil {
			f.Close()
			os.Remove(tmpMemtablePath)
		}
	}()

	w := bufio.NewWriter(f)

	err = SerializeMemtable(m, w)
	if err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	err = os.Rename(tmpMemtablePath, strings.TrimSuffix(tmpMemtablePath, ".tmp"))
	if err != nil {
		return err
	}

	// fsync parent directory
	err = fsync(filepath.Dir(m.path))
	if err != nil {
		return err
	}

	return nil
}
