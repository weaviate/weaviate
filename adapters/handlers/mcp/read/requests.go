package read

type GetTenantsArgs struct {
	Collection string   `json:"collection" jsonschema_description:"Name of collection to get tenants from"`
	Tenants    []string `json:"tenants,omitempty" jsonschema_description:"Names of tenants to get"`
}
