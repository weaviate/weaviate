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
	"github.com/weaviate/weaviate/entities/models"
)

type propValuePair struct {
	prop     string
	operator filters.Operator

	// set for all values that can be served by an inverted index, i.e. anything
	// that's not a geoRange
	_value []byte

	// only set if operator=OperatorWithinGeoRange, as that cannot be served by a
	// byte value from an inverted index
	valueGeoRange      *filters.GeoRange
	docIDs             docBitmap
	children           []*propValuePair
	hasFilterableIndex bool
	hasSearchableIndex bool
  
	Class              *models.Class // The schema
}

func newPropValuePair(class *models.Class) (*propValuePair, error) {
	if class == nil {
		return nil, errors.Errorf("class must not be nil")
	}
  
	return &propValuePair{docIDs: newDocBitmap(), Class: class}, nil
}
  
func (pv *propValuePair) SetValue(value []byte) {
	pv._value = value
}

func (pv *propValuePair) Value() []byte {
	//copy pv.value so other code can't modify it
	if pv._value == nil {
		return nil
	}
	value := make([]byte, len(pv._value))
	copy(value, pv._value)
	return value
}

  func (pv *propValuePair) DocIds() []uint64 {
	return pv.docIDs.IDs()
}

func (pv *propValuePair) fetchDocIDs(s *Searcher, limit int) error {
	if pv.operator.OnValue() {

		// TODO text_rbm_inverted_index find better way check whether prop len
		if strings.HasSuffix(pv.prop, filters.InternalPropertyLength) &&
			!pv.Class.InvertedIndexConfig.IndexPropertyLength {
			return errors.Errorf("Property length must be indexed to be filterable! add `IndexPropertyLength: true` to the invertedIndexConfig in %v.  Geo-coordinates, phone numbers and data blobs are not supported by property length.", pv.Class.Class)
		}

		if pv.operator == filters.OperatorIsNull && !pv.Class.InvertedIndexConfig.IndexNullState {
			return errors.Errorf("Nullstate must be indexed to be filterable! Add `indexNullState: true` to the invertedIndexConfig")
		}

		if (pv.prop == filters.InternalPropCreationTimeUnix ||
			pv.prop == filters.InternalPropLastUpdateTimeUnix) &&
			!pv.Class.InvertedIndexConfig.IndexTimestamps {
			return errors.Errorf("Timestamps must be indexed to be filterable! Add `IndexTimestamps: true` to the InvertedIndexConfig in %v", pv.Class.Class)
		}

		var bucketName string
		if pv.hasFilterableIndex {
			bucketName = "filterable_properties"
		} else if pv.hasSearchableIndex {
			bucketName = "searchable_properties"
		} else {
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		b := s.store.Bucket(bucketName) //We wrap this later

		// TODO text_rbm_inverted_index find better way check whether prop len
		if strings.HasSuffix(pv.prop, filters.InternalPropertyLength) &&
			!pv.Class.InvertedIndexConfig.IndexPropertyLength {
			return errors.Errorf("Property length must be indexed to be filterable! add `IndexPropertyLength: true` to the invertedIndexConfig. Geo-coordinates, phone numbers and data blobs are not supported by property length.")
		}

		if pv.operator == filters.OperatorIsNull {
			if !pv.Class.InvertedIndexConfig.IndexNullState {
				return errors.Errorf("Nullstate must be indexed to be filterable! add `indexNullState: true` to the invertedIndexConfig")
			}
		}

		if pv.prop == filters.InternalPropCreationTimeUnix || pv.prop == filters.InternalPropLastUpdateTimeUnix {
			if !pv.Class.InvertedIndexConfig.IndexTimestamps {
				return errors.Errorf("timestamps must be indexed to be filterable! add `indexTimestamps: true` to the invertedIndexConfig")
			}
		}

		bproxy,err := lsmkv.NewBucketProxy(b, pv.prop, s.propIds)
		if err != nil {
			return err
		}

		ctx := context.TODO() // TODO: pass through instead of spawning new
		dbm, err := s.docBitmap(ctx, []byte(pv.prop), bproxy, limit, pv)
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

	mergeRes := dbms[0].DocIDs.Clone()
	mergeFn := mergeRes.And
	if pv.operator == filters.OperatorOr {
		mergeFn = mergeRes.Or
	}

	for i := 1; i < len(dbms); i++ {
		mergeFn(dbms[i].DocIDs)
	}

	return &docBitmap{
		DocIDs: roaringset.Condense(mergeRes),
	}, nil
}
