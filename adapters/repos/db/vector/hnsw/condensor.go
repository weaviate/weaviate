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

package hnsw

import (
	"bufio"
	"fmt"
	"math"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/rwhasher"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/commitlog"
	"github.com/weaviate/weaviate/entities/diskio"
)

type MemoryCondensor struct {
	commitLogger *commitlog.Logger

	logger logrus.FieldLogger
}

func NewMemoryCondensor(logger logrus.FieldLogger) *MemoryCondensor {
	return &MemoryCondensor{logger: logger}
}

func (c *MemoryCondensor) Do(fileName string) error {
	c.logger.WithField("action", "hnsw_condensing").Infof("start hnsw condensing")
	defer c.logger.WithField("action", "hnsw_condensing_complete").Infof("completed hnsw condensing")

	fd, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, "open commit log to be condensed")
	}
	defer fd.Close()

	fdBuf := bufio.NewReaderSize(fd, 256*1024)

	var reader rwhasher.ReaderHasher
	var checksumReader commitlog.ChecksumReader

	checksumFileName := commitLogChecksumFileName(fileName)
	hasChecksumFile, err := diskio.FileExists(checksumFileName)
	if err != nil {
		return errors.Wrapf(err, "verifying checksum file existence %q", checksumFileName)
	}

	if hasChecksumFile {
		checksumFile, err := os.Open(checksumFileName)
		if err != nil {
			return errors.Wrapf(err, "opening checksum file %q for reading", checksumFileName)
		}
		defer checksumFile.Close()

		reader = rwhasher.NewCRC32Reader(fdBuf)
		checksumReader = commitlog.NewCRC32ChecksumReader(bufio.NewReader(checksumFile))
	} else {
		reader = commitlog.NewNoopReaderHasher(fdBuf)
		checksumReader = commitlog.NewNoopChecksumReader()
	}

	res, _, _, err := NewDeserializer(c.logger).Do(reader, checksumReader, nil, true)
	if err != nil {
		return errors.Wrap(err, "read commit log to be condensed")
	}

	newCommitFileName := fmt.Sprintf("%s.condensed", fileName)
	newLogFile, err := os.OpenFile(newCommitFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return errors.Wrap(err, "open new commit log file for writing")
	}

	newChecksumFileName := commitLogChecksumFileName(newCommitFileName)

	checksumLogger, err := commitlog.OpenCRC32ChecksumLogger(newChecksumFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return errors.Wrapf(err, "create commit log checksum file %q", checksumFileName)
	}

	c.commitLogger = commitlog.NewLoggerWithFile(newLogFile, checksumLogger)

	if res.Compressed {
		if res.CompressionPQData != nil {
			if err := c.commitLogger.AddPQCompression(*res.CompressionPQData); err != nil {
				return fmt.Errorf("write pq data: %w", err)
			}
		} else if res.CompressionSQData != nil {
			if err := c.commitLogger.AddSQCompression(*res.CompressionSQData); err != nil {
				return fmt.Errorf("write sq data: %w", err)
			}
		} else {
			return errors.Wrap(err, "unavailable compression data")
		}
	}

	for _, node := range res.Nodes {
		if node == nil {
			// nil nodes occur when we've grown, but not inserted anything yet
			continue
		}

		if node.level > 0 {
			// nodes are implicitly added when they are first linked, if the level is
			// not zero we know this node was new. If the level is zero it doesn't
			// matter if it gets added explicitly or implicitly
			if err := c.commitLogger.AddNode(node.id, node.level); err != nil {
				return errors.Wrapf(err, "write node %d to commit log", node.id)
			}
		}

		for level, links := range node.connections {
			if res.ReplaceLinks(node.id, uint16(level)) {

				if len(links) > math.MaxUint16 {
					// TODO: investigate why we get such massive connections
					targetLength := len(links)

					links = links[:math.MaxUint16]

					c.logger.WithField("action", "condense_commit_log").
						WithField("original_length", targetLength).
						WithField("maximum_length", math.MaxUint16).
						Warning("condensor length of connections would overflow uint16, cutting off")
				}

				if err := c.commitLogger.ReplaceLinksAtLevel(node.id, level, links); err != nil {
					return errors.Wrapf(err,
						"write links for node %d at level %d to commit log", node.id, level)
				}
			} else {
				if err := c.commitLogger.AddLinksAtLevel(node.id, level, links); err != nil {
					return errors.Wrapf(err,
						"write links for node %d at level %d to commit log", node.id, level)
				}
			}
		}
	}

	if res.EntrypointChanged {
		if err := c.commitLogger.SetEntryPointWithMaxLayer(res.Entrypoint,
			int(res.Level)); err != nil {
			return errors.Wrap(err, "write entrypoint to commit log")
		}
	}

	for ts := range res.Tombstones {
		// If the tombstone was later removed, consolidate the two operations into a noop
		if _, ok := res.TombstonesDeleted[ts]; ok {
			continue
		}

		if err := c.commitLogger.AddTombstone(ts); err != nil {
			return errors.Wrapf(err,
				"write tombstone for node %d to commit log", ts)
		}
	}

	for rmts := range res.TombstonesDeleted {
		// If the tombstone was added previously, consolidate the two operations into a noop
		if _, ok := res.Tombstones[rmts]; ok {
			continue
		}

		if err := c.commitLogger.RemoveTombstone(rmts); err != nil {
			return errors.Wrapf(err,
				"write removed tombstone for node %d to commit log", rmts)
		}
	}

	for nodesDeleted := range res.NodesDeleted {
		if err := c.commitLogger.DeleteNode(nodesDeleted); err != nil {
			return errors.Wrapf(err,
				"write deleted node %d to commit log", nodesDeleted)
		}
	}

	if err := c.commitLogger.Close(); err != nil {
		return errors.Wrap(err, "close new checksum commit log")
	}

	if err := os.Remove(fileName); err != nil {
		return errors.Wrap(err, "cleanup old (uncondensed) commit log")
	}

	if hasChecksumFile {
		if err := os.Remove(checksumFileName); err != nil {
			return errors.Wrap(err, "cleanup old (uncondensed) checksum commit log")
		}
	}

	return nil
}
