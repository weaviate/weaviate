package filters

// AnalyticsProps will be extracted from the graphql args of analytics
// functions (such as GetMeta and Aggregate). They tell the connectors whether
// to use an external analytics engine if such an engine is configured.
type AnalyticsProps struct {
	UseAnaltyicsEngine bool
	ForceRecalculate   bool
}
