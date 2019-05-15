/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package common_filters

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/usecases/config"
)

// ExtractAnalyticsProps from GraphQL arguments
func ExtractAnalyticsProps(args map[string]interface{}, cfg config.AnalyticsEngine) (filters.AnalyticsProps, error) {
	var res = filters.AnalyticsProps{}

	if !cfg.Enabled {
		return res, nil
	}

	// no need to check for the fields to be present. Graphql guarantees that
	// they are present. If they are not present, something was not correctly set
	// up and panicking would be fine in this case (that should never happen).
	res.UseAnaltyicsEngine = args["useAnalyticsEngine"].(bool)
	res.ForceRecalculate = args["forceRecalculate"].(bool)

	if res.UseAnaltyicsEngine == false && res.ForceRecalculate == true {
		return res, fmt.Errorf("invalid arguments: 'forceRecalculate' cannot be set to true if " +
			"'useAnalyticsEngine' is set to false")
	}

	return res, nil
}
