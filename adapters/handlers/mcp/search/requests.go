package search

type SearchWithHybridArgs struct {
	Collection       string   `json:"collection" jsonschema_description:"Name of collection to get tenants from"`
	Query            string   `json:"query,omitempty" jsonschema_description:"The plain-text query to search the collection on"`
	TargetProperties []string `json:"targetProperties,omitempty" jsonschema_description:"Names of properties to perform BM25 querying on"`
}
