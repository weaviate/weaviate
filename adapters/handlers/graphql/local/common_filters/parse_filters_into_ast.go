//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package common_filters

import (
	"encoding/json"
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/handlers/rest/filterext"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
)

// Extract the filters from the arguments of a Local->Get or Local->Meta query.
func ExtractFilters(args map[string]interface{}, rootClass string) (*filters.LocalFilter, error) {
	where, wherePresent := args["where"]
	if !wherePresent {
		// No filters; all is fine!
		return nil, nil
	} else {
		whereMap := where.(map[string]interface{}) // guaranteed by GraphQL to be a map.
		filter, err := filterMapToModel(whereMap)
		if err != nil {
			return nil, fmt.Errorf("failed to extract filters: %s", err)
		}

		return filterext.Parse(filter, rootClass)
	}
}

func filterMapToModel(m map[string]interface{}) (*models.WhereFilter, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed convert map to models.WhereFilter: %s", err)
	}

	var filter models.WhereFilter
	err = json.Unmarshal(b, &filter)
	if err != nil {
		return nil, fmt.Errorf("failed convert map to models.WhereFilter: %s", err)
	}

	return &filter, nil
}
