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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

func (p *commitloggerParser) doCollection() error {
	for {
		var version uint8

		err := binary.Read(p.checksumReader, binary.LittleEndian, &version)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return errors.Wrap(err, "read commit version")
		}

		switch version {
		case 0:
			{
				err = p.parseCollectionNodeV0()
			}
		case 1:
			{
				err = p.parseCollectionNodeV1()
			}
		default:
			{
				return fmt.Errorf("unsupported commit version %d", version)
			}
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *commitloggerParser) parseCollectionNodeV0() error {
	var commitType CommitType

	err := binary.Read(p.reader, binary.LittleEndian, &commitType)
	if err != nil {
		return errors.Wrap(err, "read commit type")
	}

	if !CommitTypeReplace.Is(commitType) {
		return errors.Errorf("found a %s commit on a collection bucket", commitType.String())
	}

	return p.parseCollectionNode(p.reader)
}

func (p *commitloggerParser) parseCollectionNodeV1() error {
	commitType, reader, err := p.doRecord()
	if err != nil {
		return err
	}

	if !CommitTypeCollection.Is(commitType) {
		return errors.Errorf("found a %s commit on a collection bucket", commitType.String())
	}

	return p.parseCollectionNode(reader)
}

func (p *commitloggerParser) parseCollectionNode(reader io.Reader) error {
	n, err := ParseCollectionNode(reader)
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
