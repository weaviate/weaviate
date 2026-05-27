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

	"github.com/weaviate/weaviate/entities/models"
)

// ValidateBodyExclusivity guards against ambiguous request bodies that
// the switch-based dispatch in updateIndex would otherwise silently
// misroute. A request like `{searchable:{rebuild:true},
// filterable:{rebuild:true}}` matches the first arm (searchable.rebuild)
// and silently ignores filterable.rebuild — the user gets a 202 but only
// half the requested work runs.
//
// Searchable.Tokenization combined with Searchable.Enabled is allowed:
// enable-searchable REQUIRES a tokenization, so they count as one verb.
func ValidateBodyExclusivity(body *models.IndexUpdateRequest) error {
	if body == nil {
		return fmt.Errorf("request body required")
	}

	var groupsSet []string

	if body.Searchable != nil {
		var verbs []string
		if body.Searchable.Enabled {
			verbs = append(verbs, "searchable.enabled")
		}
		if body.Searchable.Rebuild {
			verbs = append(verbs, "searchable.rebuild")
		}
		if body.Searchable.Algorithm != "" {
			verbs = append(verbs, "searchable.algorithm")
		}
		if body.Searchable.Tokenization != "" && !body.Searchable.Enabled {
			verbs = append(verbs, "searchable.tokenization")
		}
		if body.Searchable.Cancel {
			verbs = append(verbs, "searchable.cancel")
		}
		if len(verbs) > 1 {
			return fmt.Errorf("conflicting fields in searchable: %v — set exactly one of enabled, rebuild, algorithm, tokenization, or cancel (tokenization combined with enabled is allowed)", verbs)
		}
		if len(verbs) == 1 {
			groupsSet = append(groupsSet, "searchable")
		}
	}

	if body.Filterable != nil {
		var verbs []string
		if body.Filterable.Enabled {
			verbs = append(verbs, "filterable.enabled")
		}
		if body.Filterable.Rebuild {
			verbs = append(verbs, "filterable.rebuild")
		}
		if body.Filterable.Tokenization != "" && !body.Filterable.Enabled {
			verbs = append(verbs, "filterable.tokenization")
		}
		if body.Filterable.Cancel {
			verbs = append(verbs, "filterable.cancel")
		}
		if len(verbs) > 1 {
			return fmt.Errorf("conflicting fields in filterable: %v — set exactly one of enabled, rebuild, tokenization, or cancel", verbs)
		}
		if len(verbs) == 1 {
			groupsSet = append(groupsSet, "filterable")
		}
	}

	if body.Rangeable != nil {
		var verbs []string
		if body.Rangeable.Enabled {
			verbs = append(verbs, "rangeable.enabled")
		}
		if body.Rangeable.Rebuild {
			verbs = append(verbs, "rangeable.rebuild")
		}
		if body.Rangeable.Cancel {
			verbs = append(verbs, "rangeable.cancel")
		}
		if len(verbs) > 1 {
			return fmt.Errorf("conflicting fields in rangeable: %v — set exactly one of enabled, rebuild, or cancel", verbs)
		}
		if len(verbs) == 1 {
			groupsSet = append(groupsSet, "rangeable")
		}
	}

	if len(groupsSet) > 1 {
		return fmt.Errorf("multiple index groups set in one request (%v) — issue separate requests, one per group", groupsSet)
	}
	if len(groupsSet) == 0 {
		return fmt.Errorf("no actionable change detected; set one of: " +
			"searchable.algorithm, searchable.cancel, searchable.enabled, searchable.rebuild, searchable.tokenization, " +
			"filterable.cancel, filterable.enabled, filterable.rebuild, filterable.tokenization, " +
			"rangeable.cancel, rangeable.enabled, rangeable.rebuild")
	}
	return nil
}

// RequestedCancel assumes ValidateBodyExclusivity has already
// guaranteed at most one cancel field is set across the body.
func RequestedCancel(body *models.IndexUpdateRequest) (string, bool) {
	switch {
	case body.Searchable != nil && body.Searchable.Cancel:
		return "searchable", true
	case body.Filterable != nil && body.Filterable.Cancel:
		return "filterable", true
	case body.Rangeable != nil && body.Rangeable.Cancel:
		return "rangeable", true
	}
	return "", false
}

// NormalizeSearchableAlgorithm returns "" (which callers map to a 400)
// for unsupported values. The reverse direction (BlockMax→WAND) is not
// supported: the repair-searchable migration only writes
// blockmax-format segments.
func NormalizeSearchableAlgorithm(value string) string {
	switch strings.ToLower(strings.ReplaceAll(value, "_", "")) {
	case "blockmaxwand", "blockmax", "bmw":
		return "BlockMaxWAND"
	}
	return ""
}
