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

package tachiomfresh

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
)

// Delete marks documents as deleted in the version map. Their
// posting entries are dropped when a split or a merge next rewrites the
// posting, or a cleanup task does (cleanup.go). Searches skip them
// meanwhile. The caller drops the doc record, as it writes it (see
// AddMulti).
// copied from HFresh.Delete
func (t *TachiomFresh) Delete(ids ...uint64) error {
	for _, id := range ids {
		start := time.Now()
		version, err := t.VersionMap.MarkDeleted(context.Background(), id)
		if err != nil {
			return errors.Wrapf(err, "failed to mark doc %d as deleted", id)
		}
		if version == 0 {
			return hfresh.ErrVectorNotFound
		}
		t.docs.remove(id)
		// a staged document leaves the staging bucket (unstageDocument,
		// staging.go)
		if !t.built.Load() {
			t.stageMu.Lock()
			if !t.built.Load() {
				if err := t.unstageDocument(id); err != nil {
					t.stageMu.Unlock()
					return err
				}
			}
			t.stageMu.Unlock()
		}
		t.metrics.DeleteVector(start)
	}

	return nil
}

// copied from HFresh.DeleteMulti, without the muvera bucket
func (t *TachiomFresh) DeleteMulti(ids ...uint64) error {
	return t.Delete(ids...)
}
