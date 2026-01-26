package hfresh

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

// doAnalyze keeps the mapping postingID -> []vectorID in sync with the posting store by analyzing
// the posting and updating the posting map accordingly.
func (h *HFresh) doAnalyze(ctx context.Context, postingID uint64) error {
	start := time.Now()
	defer h.metrics.AnalyzeDuration(start)

	h.postingLocks.Lock(postingID)
	defer h.postingLocks.Unlock(postingID)
	defer h.taskQueue.AnalyzeDone(postingID)

	if !h.Centroids.Exists(postingID) {
		return nil
	}

	// load the posting metadata
	meta, err := h.PostingMap.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return errors.Wrapf(err, "failed to get posting %d for analyze operation", postingID)
	}

	// if the metadata was loaded from disk or the posting doesn't have a mapping entry yet, it might not be in sync with
	// the posting store. load the posting from disk to do the analysis.
	if meta.fromDisk || err != nil {
		p, err := h.PostingStore.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				h.logger.WithField("postingID", postingID).
					Debug("posting not found, skipping analyze operation")
				return nil
			}

			return errors.Wrapf(err, "failed to get posting %d for analyze operation", postingID)
		}

		// update the posting map in-memory cache and persist the vector IDs
		err = h.PostingMap.SetVectorIDs(ctx, postingID, p)
		if err != nil {
			return errors.Wrapf(err, "failed to set vector IDs for posting %d", postingID)
		}
	} else {
		// only persist the in-memory cache to disk
		err = h.PostingMap.Persist(ctx, postingID)
		if err != nil {
			return errors.Wrapf(err, "failed to persist posting %d", postingID)
		}
	}

	return nil
}
