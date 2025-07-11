package spfresh

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

// Updater handles write operations for the SPFresh index.
type Updater struct {
	UserConfig *UserConfig   // UserConfig contains user-defined settings for the rebuilder.
	SPTAG      SPTAG         // SPTAG provides access to the SPTAG index for centroid operations.
	Store      PostingStore  // Used for managing persistence of postings.
	Logger     *logrus.Entry // Logger for logging operations and errors.
	VersionMap *VersionMap   // VersionMap provides access to vector versions.
	IDs        *common.MonotonicCounter[uint64]
}

func (u *Updater) Insert(ctx context.Context, id uint64, vector []float32) error {
	// search the nearest centroid
	ps, err := u.SPTAG.Search(vector, 1)
	if err != nil {
		return errors.Wrap(err, "failed to search for nearest centroid")
	}

	// register the vector in the version map
	version := u.VersionMap.Increment(id)

	v := Vector{
		ID:      id,
		Version: version,
		Data:    vector,
	}

	var postingID uint64

	// if there are no postings, create a new one
	if len(ps) == 0 {
		postingID = u.IDs.Next()

		// persist the new posting first
		err = u.Store.Put(ctx, postingID, Posting{v})
		if err != nil {
			return errors.Wrapf(err, "failed to persist new posting %d", postingID)
		}
	} else {
		postingID = ps[0]

		// append the new vector to the existing posting
		err = u.Store.Append(ctx, postingID, &v)
		if err != nil {
			return errors.Wrapf(err, "failed to append vector %d to posting %d", id, postingID)
		}
	}

	// use the vector as the centroid and register it in the SPTAG
	err = u.SPTAG.Upsert(postingID, vector)
	if err != nil {
		return errors.Wrapf(err, "failed to upsert new centroid %d", postingID)
	}

	return nil
}
