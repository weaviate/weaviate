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

package reindex

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// SearchableBucketStrategyReader returns the LSM bucket strategy for
// the named property's searchable bucket on this collection, or "" if
// no searchable bucket exists. The reindex service uses the strategy
// string to decide which kind of retokenize sub-task to schedule.
type SearchableBucketStrategyReader interface {
	SearchableBucketStrategy(className entschema.ClassName, propName string) (strategy string)
}

// ValidateTokenizationChange validates the body for
// `PUT /v1/schema/{class}/indexes/{prop}` with
// `{searchable:{tokenization:X}}` and returns the searchable bucket's
// LSM strategy. Distinct from
// [ValidateFilterableTokenizationChange]: this one REQUIRES a
// searchable bucket.
func ValidateTokenizationChange(
	bucketReader SearchableBucketStrategyReader,
	class *models.Class,
	collection, propName, targetTokenization string,
) (bucketStrategy string, err error) {
	var targetProp *models.Property
	for _, p := range class.Properties {
		if p.Name == propName {
			targetProp = p
			break
		}
	}
	if targetProp == nil {
		return "", fmt.Errorf("property %q not found", propName)
	}

	dt, ok := entschema.AsPrimitive(targetProp.DataType)
	if !ok || (dt != entschema.DataTypeText && dt != entschema.DataTypeTextArray) {
		return "", fmt.Errorf("property %q is not a text type", propName)
	}

	if !entschema.IsValidTokenization(targetTokenization) {
		return "", fmt.Errorf("invalid tokenization %q", targetTokenization)
	}

	if targetProp.Tokenization == targetTokenization {
		return "", fmt.Errorf("property %q already uses tokenization %q", propName, targetTokenization)
	}

	bucketStrategy = bucketReader.SearchableBucketStrategy(entschema.ClassName(collection), propName)
	if bucketStrategy == "" {
		return "", fmt.Errorf("searchable bucket not found for property %q", propName)
	}
	return bucketStrategy, nil
}
