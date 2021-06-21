package lsmkv

import (
	"io/ioutil"
	"path/filepath"

	"github.com/pkg/errors"
)

func (b *Bucket) recoverFromCommitLogs() error {
	list, err := ioutil.ReadDir(b.dir)
	if err != nil {
		return err
	}

	var walFileNames []string
	for _, fileInfo := range list {
		if filepath.Ext(fileInfo.Name()) != ".wal" {
			// skip, this could be commit log, etc.
			continue
		}

		walFileNames = append(walFileNames, fileInfo.Name())
	}

	if len(walFileNames) == 0 {
		// nothing to recover from
		return nil
	}

	// recover from each log
	for _, fname := range walFileNames {
		if err := b.parseWALIntoMemtable(filepath.Join(b.dir, fname)); err != nil {
			return errors.Wrapf(err, "ingest wal %q", fname)
		}
	}

	if b.active.size > 0 {
		return b.FlushAndSwitch()
	}

	return nil
}

func (b *Bucket) parseWALIntoMemtable(fname string) error {
	return nil
}
