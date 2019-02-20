package batch

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/models"
)

// determine which field values not to return
func determineResponseFields(fields []*string, isThingsCreate bool) map[string]int {
	fieldsToKeep := map[string]int{"@class": 0, "schema": 0, "creationtimeunix": 0, "key": 0, "actionid": 0}

	// convert to things instead of actions
	if isThingsCreate {
		delete(fieldsToKeep, "actionid")
		fieldsToKeep["thingid"] = 0
	}

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

func errPayloadFromSingleErr(err error) *models.ErrorResponse {
	return &models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{{
		Message: fmt.Sprintf("%s", err),
	}}}
}
