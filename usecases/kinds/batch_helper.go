/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package kinds

import "strings"

// determine which field values not to return
func determineResponseFields(fields []*string) map[string]int {
	fieldsToKeep := map[string]int{"class": 0, "schema": 0, "creationtimeunix": 0, "key": 0, "id": 0}

	if len(fields) > 0 {

		// check if "ALL" option is provided
		for _, field := range fields {
			fieldToKeep := strings.ToLower(*field)
			if fieldToKeep == "all" {
				return fieldsToKeep
			}
		}

		fieldsToKeep = make(map[string]int)
		// iterate over the provided fields
		for _, field := range fields {
			fieldToKeep := strings.ToLower(*field)
			fieldsToKeep[fieldToKeep] = 0
		}
	}

	return fieldsToKeep
}
