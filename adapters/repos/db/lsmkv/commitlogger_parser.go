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

package lsmkv

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/pkg/errors"
)

type commitloggerParser struct {
	path     string
	memtable *Memtable
	reader   io.Reader
}

func newCommitLoggerParser(path string, activeMemtable *Memtable) *commitloggerParser {
	return &commitloggerParser{path: path, memtable: activeMemtable}
}

func (p *commitloggerParser) Do() error {
	f, err := os.Open(p.path)
	if err != nil {
		return err
	}

	p.reader = bufio.NewReader(f)

	for {
		var commitType CommitType

		err := binary.Read(p.reader, binary.LittleEndian, &commitType)
		if err == io.EOF {
			break
		}

		if err != nil {
			return errors.Wrap(err, "read commit type")
		}

		switch commitType {
		case CommitTypeReplace:
			if err := p.parseReplaceNode(); err != nil {
				return errors.Wrap(err, "read replace node")
			}
		case CommitTypeCollection:
			if err := p.parseCollectionNode(); err != nil {
				return errors.Wrap(err, "read collection node")
			}
		}
	}

	return f.Close()
}

func (p *commitloggerParser) parseReplaceNode() error {
	n, err := ParseReplaceNode(p.reader, p.memtable.secondaryIndices)
	if err != nil {
		return err
	}

	var opts []SecondaryKeyOption
	if p.memtable.secondaryIndices > 0 {
		for i, secKey := range n.secondaryKeys {
			opts = append(opts, WithSecondaryKey(i, secKey))
		}
	}

	if n.tombstone {
		return p.memtable.setTombstone(n.primaryKey, opts...)
	}
	return p.memtable.put(n.primaryKey, n.value, opts...)
}

func (p *commitloggerParser) parseCollectionNode() error {
	n, err := ParseCollectionNode(p.reader)
	if err != nil {
		return err
	}

	return p.memtable.append(n.primaryKey, n.values)
}
