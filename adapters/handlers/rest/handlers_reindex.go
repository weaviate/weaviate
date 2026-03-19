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

package rest

import (
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// buildUnitMaps creates per-replica unit IDs and maps from shard ownership.
// ShardOwnership returns map[nodeName][]shardName (node→shards it owns).
// Unit ID format: "shardName__nodeName".
func buildUnitMaps(shardOwnership map[string][]string) (unitIDs []string, unitToShard, unitToNode map[string]string) {
	unitToShard = make(map[string]string)
	unitToNode = make(map[string]string)

	for nodeName, shards := range shardOwnership {
		for _, shardName := range shards {
			unitID := fmt.Sprintf("%s__%s", shardName, nodeName)
			unitIDs = append(unitIDs, unitID)
			unitToShard[unitID] = shardName
			unitToNode[unitID] = nodeName
		}
	}
	sort.Strings(unitIDs)
	return unitIDs, unitToShard, unitToNode
}

func validateRangeableProperties(class *models.Class, propNames []string) error {
	propsByName := make(map[string]*models.Property, len(class.Properties))
	for _, p := range class.Properties {
		propsByName[p.Name] = p
	}

	for _, pn := range propNames {
		prop, ok := propsByName[pn]
		if !ok {
			return fmt.Errorf("property %q not found", pn)
		}
		dt, ok := entschema.AsPrimitive(prop.DataType)
		if !ok || (dt != entschema.DataTypeInt && dt != entschema.DataTypeNumber && dt != entschema.DataTypeDate) {
			return fmt.Errorf("property %q is not a numeric type (int, number, date)", pn)
		}
		if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
			return fmt.Errorf("property %q already has indexRangeFilters enabled", pn)
		}
	}
	return nil
}

func validateTokenizationChange(
	appState *state.State,
	class *models.Class,
	collection, propName, targetTokenization string,
) (bucketStrategy string, err error) {
	// Find the property.
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

	// Validate target tokenization.
	validTokenizations := map[string]struct{}{
		models.PropertyTokenizationWord:       {},
		models.PropertyTokenizationLowercase:  {},
		models.PropertyTokenizationWhitespace: {},
		models.PropertyTokenizationField:      {},
		models.PropertyTokenizationTrigram:    {},
		models.PropertyTokenizationGse:        {},
		models.PropertyTokenizationKagomeKr:   {},
		models.PropertyTokenizationKagomeJa:   {},
		models.PropertyTokenizationGseCh:      {},
	}
	if _, ok := validTokenizations[targetTokenization]; !ok {
		return "", fmt.Errorf("invalid tokenization %q", targetTokenization)
	}

	if targetProp.Tokenization == targetTokenization {
		return "", fmt.Errorf("property %q already uses tokenization %q", propName, targetTokenization)
	}

	// Detect bucket strategy from the first shard's searchable bucket.
	className := entschema.ClassName(collection)
	idx := appState.DB.GetIndex(className)
	if idx == nil {
		return "", fmt.Errorf("collection index not found")
	}

	idx.ForEachShard(func(_ string, shard db.ShardLike) error {
		if bucketStrategy != "" {
			return nil
		}
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		bucket := shard.Store().Bucket(bucketName)
		if bucket != nil {
			bucketStrategy = bucket.Strategy()
		}
		return nil
	})
	if bucketStrategy == "" {
		return "", fmt.Errorf("searchable bucket not found for property %q", propName)
	}

	return bucketStrategy, nil
}
