package common_filters

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/config"
)

// AnalyticsProps will be extracted from the graphql args of analytics
// functions (such as GetMeta and Aggregate). They tell the connectors whether
// to use an external analytics engine if such an engine is configured.
type AnalyticsProps struct {
	UseAnaltyicsEngine bool
	ForceRecalculate   bool
}

// ExtractAnalyticsProps from GraphQL arguments
func ExtractAnalyticsProps(args map[string]interface{}, cfg config.AnalyticsEngine) (AnalyticsProps, error) {
	var res = AnalyticsProps{}

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
