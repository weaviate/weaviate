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

package inverted

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entsInverted "github.com/weaviate/weaviate/entities/inverted"
)

// servedByKeyDocColumn reports whether a key/doc column can answer op. ContainsAll
// is left out: its fold stops once the running intersection empties, after about
// two keys on a near-unique property.
func servedByKeyDocColumn(op filters.Operator) bool {
	return op == filters.ContainsAny || op == filters.ContainsNone
}

// docBitmapContainsColumn answers the reader's batch from the key/doc column,
// as a deny list when denyList is set. The caller opens the reader only on a view holding a column.
func (s *Searcher) docBitmapContainsColumn(ctx context.Context,
	reader *lsmkv.RoaringSetKeyDocColumnReader, denyList bool,
) (docBitmap, error) {
	// Resolve and Materialize run on this goroutine alone
	defer shrinkBudgetTo(ctx, 1)()

	res, err := reader.Resolve(ctx)
	if err != nil {
		return docBitmap{}, err
	}
	if res == nil {
		return docBitmap{}, fmt.Errorf("%w: key/doc column reader opened on a view without a column",
			entsInverted.ErrInternal)
	}

	// The query's pool, so the result returns to it on release as the fold's does.
	bm, put := res.Materialize(s.bitmapFactory.BufPool())
	return docBitmap{docIDs: bm, release: put, isDenyList: denyList}, nil
}
