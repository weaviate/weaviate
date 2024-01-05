//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import "strings"

// determine which field values not to return
func determineResponseFields(fields []*string) map[string]struct{} {
	fieldsToKeep := map[string]struct{}{
		"class": {}, "properties": {}, "creationTimeUnix": {},
		"lastUpdateTimeUnix": {}, "key": {}, "id": {},
	}

	if len(fields) > 0 {
		// check if "ALL" option is provided
		for _, field := range fields {
			fieldToKeep := strings.ToLower(*field)
			if fieldToKeep == "all" {
				return fieldsToKeep
			}
		}

		fieldsToKeep = make(map[string]struct{})
		// iterate over the provided fields
		for _, field := range fields {
			fieldToKeep := strings.ToLower(*field)
			fieldsToKeep[fieldToKeep] = struct{}{}
		}
	}

	return fieldsToKeep
}
