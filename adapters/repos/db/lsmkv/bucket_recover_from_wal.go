package lsmkv

import (
	"io/ioutil"
	"os"
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
			// skip, this could be disk segments, etc.
			continue
		}

		if filepath.Join(b.dir, fileInfo.Name()) == b.active.path+".wal" {
			// this is the new one which was just created
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

	// delete the commit logs as we can now be sure that they are part of a disk
	// segment
	for _, fname := range walFileNames {
		if err := os.RemoveAll(filepath.Join(b.dir, fname)); err != nil {
			return errors.Wrap(err, "clean up commit log")
		}
	}

	return nil
}

func (b *Bucket) parseWALIntoMemtable(fname string) error {
	return newCommitLoggerParser(fname, b.active).Do()
}
