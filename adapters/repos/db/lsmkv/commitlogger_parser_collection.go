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
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/diskio"
)

func (p *commitloggerParser) doCollection() error {
	metered := diskio.NewMeteredReader(p.r, p.metrics.TrackStartupReadWALDiskIO)
	p.reader = bufio.NewReaderSize(metered, 1*1024*1024)

	for {
		var commitType CommitType

		err := binary.Read(p.reader, binary.LittleEndian, &commitType)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return errors.Wrap(err, "read commit type")
		}

		if CommitTypeCollection.Is(commitType) {
			if err := p.parseCollectionNode(); err != nil {
				return errors.Wrap(err, "read collection node")
			}
		} else {
			return errors.Errorf("found a %s commit on collection bucket", commitType.String())
		}
	}

	return nil
}

func (p *commitloggerParser) parseCollectionNode() error {
	n, err := ParseCollectionNode(p.reader)
	if err != nil {
		return err
	}

	if p.strategy == StrategyMapCollection {
		return p.parseMapNode(n)
	}
	return p.memtable.append(n.primaryKey, n.values)
}

func (p *commitloggerParser) parseMapNode(n segmentCollectionNode) error {
	for _, val := range n.values {
		mp := MapPair{}
		if err := mp.FromBytes(val.value, false); err != nil {
			return err
		}
		mp.Tombstone = val.tombstone

		if err := p.memtable.appendMapSorted(n.primaryKey, mp); err != nil {
			return err
		}
	}

	return nil
}
