//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

type propValuePair struct {
	prop     string
	operator filters.Operator

	// set for all values that can be served by an inverted index, i.e. anything
	// that's not a geoRange
	value []byte

	// only set if operator=OperatorWithinGeoRange, as that cannot be served by a
	// byte value from an inverted index
	valueGeoRange      *filters.GeoRange
	docIDs             docBitmap
	children           []*propValuePair
	hasFilterableIndex bool
	hasSearchableIndex bool
}

func newPropValuePair() propValuePair {
	return propValuePair{docIDs: newDocBitmap()}
}

func (pv *propValuePair) DocIds() []uint64 {
	return pv.docIDs.IDs()
} 

func (pv *propValuePair) fetchDocIDs(s *Searcher, limit int) error {
	if pv.operator.OnValue() {
		var bucketName string
		if pv.hasFilterableIndex {
			bucketName = "filterable_properties"
		} else if pv.hasSearchableIndex {
			bucketName = "searchable_properties"
		} else {
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		b := lsmkv.NewBucketProxy(s.store.Bucket(bucketName), []byte(pv.prop), s.propIds) 

		// TODO text_rbm_inverted_index find better way check whether prop len
		if b == nil && strings.HasSuffix(pv.prop, filters.InternalPropertyLength) {  //FIXME check that propname length will be the internal propname length name
			return errors.Errorf("Property length must be indexed to be filterable! " +
				"add `IndexPropertyLength: true` to the invertedIndexConfig." +
				"Geo-coordinates, phone numbers and data blobs are not supported by property length.")
		}

		if b == nil && pv.operator == filters.OperatorIsNull {
			return errors.Errorf("Nullstate must be indexed to be filterable! " +
				"add `indexNullState: true` to the invertedIndexConfig")
		}

		if b == nil && (pv.prop == filters.InternalPropCreationTimeUnix ||
			pv.prop == filters.InternalPropLastUpdateTimeUnix) {
			return errors.Errorf("timestamps must be indexed to be filterable! " +
				"add `indexTimestamps: true` to the invertedIndexConfig")
		}

		if b == nil && pv.operator != filters.OperatorWithinGeoRange {
			// a nil bucket is ok for a WithinGeoRange filter, as this query is not
			// served by the inverted index, but propagated to a secondary index in
			// .docPointers()
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		ctx := context.TODO() // TODO: pass through instead of spawning new
		dbm, err := s.docBitmap(ctx, []byte(pv.prop), b, limit, pv)
		if err != nil {
			return err
		}
		pv.docIDs = dbm
	} else {
	
		for i, child := range pv.children {
			i, child := i, child
			
				// Explicitly set the limit to 0 (=unlimited) as this is a nested filter,
				// otherwise we run into situations where each subfilter on their own
				// runs into the limit, possibly yielding in "less than limit" results
				// after merging.
				err := child.fetchDocIDs(s, 0)
				if err != nil {
					return errors.Wrapf(err, "nested child %d", i)
				}

				return nil
			
		}
		
	}

	return nil
}

func (pv *propValuePair) mergeDocIDs() (*docBitmap, error) {
	if pv.operator.OnValue() {
		return &pv.docIDs, nil
	}

	if pv.operator != filters.OperatorAnd && pv.operator != filters.OperatorOr {
		return nil, fmt.Errorf("unsupported operator: %s", pv.operator.Name())
	}
	if len(pv.children) == 0 {
		return nil, fmt.Errorf("no children for operator: %s", pv.operator.Name())
	}

	dbms := make([]*docBitmap, len(pv.children))
	for i, child := range pv.children {
		dbm, err := child.mergeDocIDs()
		if err != nil {
			return nil, errors.Wrapf(err, "retrieve doc bitmap of child %d", i)
		}
		dbms[i] = dbm
	}

	mergeRes := dbms[0].docIDs.Clone()
	mergeFn := mergeRes.And
	if pv.operator == filters.OperatorOr {
		mergeFn = mergeRes.Or
	}

	for i := 1; i < len(dbms); i++ {
		mergeFn(dbms[i].docIDs)
	}

	return &docBitmap{
		docIDs: roaringset.Condense(mergeRes),
	}, nil
}
