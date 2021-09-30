//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"bufio"
	"os"
	"syscall"

	"github.com/pkg/errors"
)

type MmapCondensorReader struct {
	reader *bufio.Reader
	target []byte
}

func newMmapCondensorReader() *MmapCondensorReader {
	return &MmapCondensorReader{}
}

func (r *MmapCondensorReader) Do(source *os.File, index mmapIndex,
	targetName string) error {
	r.reader = bufio.NewReaderSize(source, 1024*1024)

	scratchFile, err := os.Create(targetName)
	if err != nil {
		return err
	}

	size := index.Size()
	if err := scratchFile.Truncate(int64(index.Size())); err != nil {
		return errors.Wrap(err, "truncate scratch file to size")
	}

	mmapSpace, err := syscall.Mmap(int(scratchFile.Fd()), 0, size,
		syscall.PROT_WRITE, syscall.MAP_PRIVATE)
	if err != nil {
		return errors.Wrap(err, "mmap scratch file")
	}

	r.target = mmapSpace

	if err := r.loop(); err != nil {
		return err
	}

	if err := syscall.Munmap(r.target); err != nil {
		return errors.Wrap(err, "munmap scratch file")
	}

	if err := scratchFile.Close(); err != nil {
		return errors.Wrap(err, "close scratch file")
	}

	return nil
}

func (r *MmapCondensorReader) loop() error {
	// TODO: iterate through commit log
	// TODO: get offset for specific part
	// TODO: write into target at correct position
	return nil
}
