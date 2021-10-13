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
	"io"
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
)

type MmapCondensor struct {
	connectionsPerLevel int
}

func NewMmapCondensor(connectionsPerLevel int) *MmapCondensor {
	return &MmapCondensor{connectionsPerLevel: connectionsPerLevel}
}

func (c *MmapCondensor) Do(fileName string) error {
	fd, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, "open commit log to be condensed")
	}

	index, err := c.analyze(fd)
	if err != nil {
		return errors.Wrap(err, "analyze commit log and build index")
	}

	index.calculateOffsets()

	// "rewind" file so we can read it again, this time into the mmap file
	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "rewind uncondensed")
	}

	if err := c.read(fd, index, fileName+".scratch.tmp"); err != nil {
		return errors.Wrap(err, "read uncondensed into mmap file")
	}

	spew.Dump(index)
	spew.Dump(index.Size())
	return nil
}

func (c *MmapCondensor) analyze(file *os.File) (mmapIndex, error) {
	return newMmapCondensorAnalyzer(c.connectionsPerLevel).Do(file)
}

func (c *MmapCondensor) read(source *os.File, index mmapIndex,
	targetName string) error {
	return newMmapCondensorReader().Do(source, index, targetName)
}

func (ind *mmapIndex) calculateOffsets() {
	for i := range ind.nodes {
		if i == 0 {
			// offset for the first element is 0, nothing to do
			continue
		}

		// we now have the guarantee that elem i-1 exists
		ind.nodes[i].offset = ind.nodes[i-1].offset + uint64(ind.nodes[i-1].Size(ind.connectionsPerLevel))
	}
}

// Size can only return a useful result if offsets have been calculated prior
// to calling Size()
func (ind *mmapIndex) Size() int {
	if len(ind.nodes) == 0 {
		return -1
	}

	return int(ind.nodes[len(ind.nodes)-1].offset) +
		ind.nodes[len(ind.nodes)-1].Size(ind.connectionsPerLevel)
}
