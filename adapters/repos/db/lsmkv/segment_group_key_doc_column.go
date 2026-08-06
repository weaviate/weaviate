//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/keydoccolumn"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
)

// initKeyDocColumn builds and attaches the index, once, while the
// segment group is being constructed — before the bucket exists, so before any
// flush, which leaves the flush hook a target from the start.
//
// It reads the disk segments alone: the memtables are empty at this point, and
// what is unflushed at query time is layered on then instead.
func (sg *SegmentGroup) initKeyDocColumn(maxIdGetter roaringset.MaxIdGetterFunc) error {
	if maxIdGetter == nil {
		return errors.New("no max ID getter given")
	}

	before := time.Now()
	cursors, release := sg.newRoaringSetCursors()
	defer release()

	merged := roaringset.NewCombinedCursor(cursors, false, concurrency.SROAR_MERGE)
	idx, err := keydoccolumn.BuildFromCursor(merged, maxIdGetter(), sg.logger)
	if err != nil {
		return err
	}

	sg.keyDocColumn.Store(idx)
	info := idx.Info()
	sg.logger.WithFields(logrus.Fields{
		"action":      "keydoccolumn_build",
		"bucket":      filepath.Base(sg.dir),
		"took":        time.Since(before).String(),
		"keys":        info.Keys,
		"key_width":   info.KeyWidth,
		"docid_width": info.DocIDWidth,
		"size_mb":     fmt.Sprintf("%.3f", float64(info.SizeBytes)/1024/1024),
	}).Debug("key/doc column built")
	return nil
}

// mergeMemtableIntoKeyDocColumn feeds a just-flushed memtable to the index, from
// the segment swap that removes it — so the flush becomes visible as a segment
// and as index state in one critical section, and no query falls between the two.
//
// This is worth doing under flushLock, which gates every read and write on the
// bucket, only because the memtable is sealed here: writers have drained and it
// is no longer active, so it is read without the copy [Memtable.newRoaringSetCursor]
// makes for readers of a live one. That copy dominated the read, and skipping it
// leaves work of the same order as this switch's other arms.
//
// If the index declines the flush, it is detached. The layer is appended only
// after every check that can fail, so a failure leaves the index without the
// flushed documents — and once the swap lands, those documents live in the new
// segment alone, which the index never reads. Keeping it would mean answering
// ContainsAny with silently missing results.
func (sg *SegmentGroup) mergeMemtableIntoKeyDocColumn(flushing memtable) error {
	idx := sg.keyDocColumn.Load()
	if idx == nil {
		return nil
	}
	if flushing == nil || flushing.Size() == 0 {
		return nil // nothing was flushed; the swap discards this memtable
	}
	// sealed: the memtable is durable, out of active use and its writers have
	// drained, so the index reads it without paying for a copy it cannot use
	if err := idx.MergeMemtableByCursor(flushing.newSealedRoaringSetCursor()); err != nil {
		sg.keyDocColumn.Store(nil)
		return err
	}
	return nil
}
