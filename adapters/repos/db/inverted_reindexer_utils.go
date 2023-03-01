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

package db

import (
	"regexp"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

func GetPropNameAndIndexTypeFromBucketName(bucketName string) (string, PropertyIndexType) {
	propRegexpGroup := "(?P<propName>.*)"

	types := []struct {
		indexType    PropertyIndexType
		bucketNameFn func(string) string
	}{
		{
			IndexTypePropNull,
			helpers.BucketFromPropNameNullLSM,
		},
		{
			IndexTypePropLength,
			helpers.BucketFromPropNameLengthLSM,
		},
		{
			IndexTypePropValue,
			helpers.BucketFromPropNameLSM,
		},
		{
			IndexTypeHashPropNull,
			helpers.HashBucketFromPropNameNullLSM,
		},
		{
			IndexTypeHashPropLength,
			helpers.HashBucketFromPropNameLengthLSM,
		},
		{
			IndexTypeHashPropValue,
			helpers.HashBucketFromPropNameLSM,
		},
	}

	for _, t := range types {
		r, err := regexp.Compile("^" + t.bucketNameFn(propRegexpGroup) + "$")
		if err != nil {
			continue
		}
		matches := r.FindStringSubmatch(bucketName)
		if len(matches) > 0 {
			return matches[r.SubexpIndex("propName")], t.indexType
		}
	}
	return "", 0
}

type reindexablePropertyChecker struct {
	reindexables map[string]map[PropertyIndexType]struct{}
}

func newReindexablePropertyChecker(reindexableProperties []ReindexableProperty) *reindexablePropertyChecker {
	reindexables := map[string]map[PropertyIndexType]struct{}{}
	for _, property := range reindexableProperties {
		if _, ok := reindexables[property.PropertyName]; !ok {
			reindexables[property.PropertyName] = map[PropertyIndexType]struct{}{}
		}
		reindexables[property.PropertyName][property.IndexType] = struct{}{}
	}
	return &reindexablePropertyChecker{reindexables}
}

func (c *reindexablePropertyChecker) isReindexable(propName string, indexType PropertyIndexType) bool {
	if _, ok := c.reindexables[propName]; !ok {
		return false
	} else if _, ok := c.reindexables[propName][indexType]; !ok {
		return false
	}
	return true
}
