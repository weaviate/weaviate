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
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// ValidateTokenizationChange is distinct from
// [ValidateFilterableTokenizationChange]: this one covers the coupled
// searchable+filterable retokenize and returns the bucket strategy the
// migration must preserve.
func ValidateTokenizationChange(
	class *models.Class,
	propName, targetTokenization string,
	reindexTasks []*distributedtask.Task,
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

	// change-tokenization preserves the bucket's existing strategy, derived from
	// RAFT-consistent state (durable stamp, else class flag/task list) — the
	// stamp keeps a stamped-blockmax property on StrategyInverted after its task
	// ages out.
	return lsmkv.DefaultSearchableStrategy(
		dbreindex.SearchablePropertyIsBlockmax(class, propName, reindexTasks)), nil
}

// NormalizeSearchableAlgorithm canonicalises algorithm to
// "wand"/"blockmax", accepting aliases like "block-max"/"bmw"
// (case-insensitive). Returns "" for anything else, so the dispatcher's
// allowlist treats a new algorithm as a missing case, not silent
// acceptance.
func NormalizeSearchableAlgorithm(s string) string {
	// Strip surrounding whitespace before any other transform — a body
	// like {"algorithm":" blockmax "} should not be rejected on a stray
	// space.
	trimmed := strings.TrimSpace(s)
	lower := strings.ToLower(trimmed)
	// Strip ASCII separators that callers sometimes inject (e.g.
	// "block-max", "block_max"). Done after lowercasing so the set is
	// minimal.
	stripped := strings.ReplaceAll(strings.ReplaceAll(lower, "-", ""), "_", "")
	switch stripped {
	case "blockmax", "blockmaxwand", "bmw":
		return models.IndexStatusAlgorithmBlockmax
	case "wand":
		return models.IndexStatusAlgorithmWand
	}
	return ""
}
