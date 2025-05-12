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
	"errors"
	"fmt"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type segmentCleanerReplace struct {
	w                        io.WriteSeeker
	bufw                     *bufio.Writer
	cursor                   *segmentCursorReplace
	keyExistsFn              keyExistsOnUpperSegmentsFunc
	version                  uint16
	level                    uint16
	secondaryIndexCount      uint16
	scratchSpacePath         string
	enableChecksumValidation bool
}

func newSegmentCleanerReplace(w io.WriteSeeker, cursor *segmentCursorReplace,
	keyExistsFn keyExistsOnUpperSegmentsFunc, level, secondaryIndexCount uint16,
	scratchSpacePath string, enableChecksumValidation bool,
) *segmentCleanerReplace {
	return &segmentCleanerReplace{
		w:                        w,
		bufw:                     bufio.NewWriterSize(w, 256*1024),
		cursor:                   cursor,
		keyExistsFn:              keyExistsFn,
		version:                  segmentindex.ChooseHeaderVersion(enableChecksumValidation),
		level:                    level,
		secondaryIndexCount:      secondaryIndexCount,
		scratchSpacePath:         scratchSpacePath,
		enableChecksumValidation: enableChecksumValidation,
	}
}

func (p *segmentCleanerReplace) do(shouldAbort cyclemanager.ShouldAbortCallback) error {
	if err := p.init(); err != nil {
		return fmt.Errorf("init: %w", err)
	}

	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(p.bufw),
		segmentindex.WithChecksumsDisabled(!p.enableChecksumValidation),
	)

	indexKeys, err := p.writeKeys(segmentFile, shouldAbort)
	if err != nil {
		return fmt.Errorf("write keys: %w", err)
	}

	if err := p.writeIndexes(segmentFile, indexKeys); err != nil {
		return fmt.Errorf("write indices: %w", err)
	}

	// flush buffered, so we can safely seek on underlying writer
	if err := p.bufw.Flush(); err != nil {
		return fmt.Errorf("flush buffered: %w", err)
	}

	var dataEnd uint64 = segmentindex.HeaderSize
	if l := len(indexKeys); l > 0 {
		dataEnd = uint64(indexKeys[l-1].ValueEnd)
	}

	if err := p.writeHeader(segmentFile, dataEnd); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if _, err := segmentFile.WriteChecksum(); err != nil {
		return fmt.Errorf("write compactorSet segment checksum: %w", err)
	}

	return nil
}

func (p *segmentCleanerReplace) init() error {
	// write a dummy header as its contents are not known yet.
	// file will be sought to the beginning and overwritten with actual header
	// at the very end

	if _, err := p.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return fmt.Errorf("write empty header: %w", err)
	}
	return nil
}

func (p *segmentCleanerReplace) writeKeys(f *segmentindex.SegmentFile,
	shouldAbort cyclemanager.ShouldAbortCallback,
) ([]segmentindex.Key, error) {
	// the (dummy) header was already written, this is our initial offset
	offset := segmentindex.HeaderSize

	var indexKeys []segmentindex.Key
	var indexKey segmentindex.Key
	var node segmentReplaceNode
	var err error
	var keyExists bool

	i := 0
	for node, err = p.cursor.firstWithAllKeys(); err == nil || errors.Is(err, lsmkv.Deleted); node, err = p.cursor.nextWithAllKeys() {
		i++
		if i%100 == 0 && shouldAbort() {
			return nil, fmt.Errorf("should abort requested")
		}

		keyExists, err = p.keyExistsFn(node.primaryKey)
		if err != nil {
			break
		}
		if keyExists {
			continue
		}
		nodeCopy := node
		nodeCopy.offset = offset
		indexKey, err = nodeCopy.KeyIndexAndWriteTo(f.BodyWriter())
		if err != nil {
			break
		}
		offset = indexKey.ValueEnd
		indexKeys = append(indexKeys, indexKey)
	}

	if !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	return indexKeys, nil
}

func (p *segmentCleanerReplace) writeIndexes(f *segmentindex.SegmentFile,
	keys []segmentindex.Key,
) error {
	indexes := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: p.secondaryIndexCount,
		ScratchSpacePath:    p.scratchSpacePath,
		ObserveWrite: monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
			"strategy":  StrategyReplace,
			"operation": "cleanupWriteIndices",
		}),
	}
	_, err := f.WriteIndexes(indexes)
	return err
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (p *segmentCleanerReplace) writeHeader(f *segmentindex.SegmentFile,
	startOfIndex uint64,
) error {
	if _, err := p.w.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to beginning to write header: %w", err)
	}

	h := &segmentindex.Header{
		Level:            p.level,
		Version:          p.version,
		SecondaryIndices: p.secondaryIndexCount,
		Strategy:         segmentindex.StrategyReplace,
		IndexStart:       startOfIndex,
	}
	// We have to write directly to compactor writer,
	// since it has seeked back to start. The following
	// call to f.WriteHeader will not write again.
	if _, err := h.WriteTo(p.w); err != nil {
		return err
	}

	if _, err := f.WriteHeader(h); err != nil {
		return err
	}

	if _, err := p.w.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek to end after writing header: %w", err)
	}

	p.bufw.Reset(p.w)

	return nil
}
