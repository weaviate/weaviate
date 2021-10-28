package inverted

import (
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func (pv *propValuePair) cacheable() bool {
	if pv.children != nil {
		// nested filters not yet cachable
		return false
	}

	if pv.operator != filters.OperatorEqual {
		// non exact matches not yet cachable
		return false
	}

	// single level exact match filters can be cached
	return true
}

func (pv *propValuePair) fetchHashes(s *Searcher) error {
	if pv.operator.OnValue() {
		if pv.prop == "id" {
			pv.prop = helpers.PropertyNameID
			pv.hasFrequency = false
		}

		bucketName := helpers.HashBucketFromPropNameLSM(pv.prop)
		b := s.store.Bucket(bucketName)
		if b == nil && pv.operator != filters.OperatorWithinGeoRange {
			return errors.Errorf("hash bucket for prop %s not found - is it indexed?", pv.prop)
		}

		hash, err := b.Get(pv.value)
		if err != nil {
			return err
		}

		pv.docIDs.checksum = hash
	} else {
		return errors.Errorf("nested filters not supported yet")
	}

	return nil
}
