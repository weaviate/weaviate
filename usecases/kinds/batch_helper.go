package kinds

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
