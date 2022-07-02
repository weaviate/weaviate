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
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/diskio"
)

type commitloggerParser struct {
	path     string
	strategy string
	memtable *Memtable
	reader   io.Reader
	metrics  *Metrics
	unique   map[string]segmentReplaceNode
}

func newCommitLoggerParser(path string, activeMemtable *Memtable,
	strategy string, metrics *Metrics) *commitloggerParser {
	return &commitloggerParser{
		path:     path,
		memtable: activeMemtable,
		strategy: strategy,
		metrics:  metrics,
		unique:   map[string]segmentReplaceNode{},
	}
}

func (p *commitloggerParser) Do() error {
	f, err := os.Open(p.path)
	if err != nil {
		return err
	}

	metered := diskio.NewMeteredReader(f, p.metrics.TrackStartupReadWALDiskIO)
	p.reader = bufio.NewReaderSize(metered, 1*1024*1024)

	count := 0
	for {
		fmt.Printf("read commit %d\n", count)
		count++
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

	count = 0
	if p.strategy == StrategyReplace {
		for _, node := range p.unique {
			var opts []SecondaryKeyOption
			if p.memtable.secondaryIndices > 0 {
				for i, secKey := range node.secondaryKeys {
					opts = append(opts, WithSecondaryKey(i, secKey))
				}
			}
			if node.tombstone {
				p.memtable.setTombstone(node.primaryKey, opts...)
			} else {
				p.memtable.put(node.primaryKey, node.value, opts...)
			}

			fmt.Printf("inserted %d elements\n", count)
		}
	}

	return f.Close()
}

func (p *commitloggerParser) parseReplaceNode() error {
	n, err := ParseReplaceNode(p.reader, p.memtable.secondaryIndices)
	if err != nil {
		return err
	}

	// var opts []SecondaryKeyOption
	// if p.memtable.secondaryIndices > 0 {
	// 	for i, secKey := range n.secondaryKeys {
	// 		opts = append(opts, WithSecondaryKey(i, secKey))
	// 	}
	// }

	if !n.tombstone {
		p.unique[string(n.primaryKey)] = n
	} else {
		if existing, ok := p.unique[string(n.primaryKey)]; ok {
			existing.tombstone = true
			p.unique[string(n.primaryKey)] = existing
		} else {
			p.unique[string(n.primaryKey)] = n
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
