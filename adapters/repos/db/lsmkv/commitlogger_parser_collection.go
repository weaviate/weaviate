package lsmkv

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/diskio"
)

func (p *commitloggerParser) doCollection() error {
	f, err := os.Open(p.path)
	if err != nil {
		return err
	}

	metered := diskio.NewMeteredReader(f, p.metrics.TrackStartupReadWALDiskIO)
	p.reader = bufio.NewReaderSize(metered, 1*1024*1024)

	for {
		var commitType CommitType

		err := binary.Read(p.reader, binary.LittleEndian, &commitType)
		if err == io.EOF {
			break
		}

		if err != nil {
			f.Close()
			return errors.Wrap(err, "read commit type")
		}

		switch commitType {
		case CommitTypeReplace:
			f.Close()
			return errors.Errorf("found a replace commit on collection bucket")
		case CommitTypeCollection:
			if err := p.parseCollectionNode(); err != nil {
				f.Close()
				return errors.Wrap(err, "read collection node")
			}
		}
	}

	return f.Close()
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
