package create

type InsertOneArgs struct {
	Collection string         `json:"collection" jsonschema_description:"Name of collection to insert into"`
	Properties map[string]any `json:"properties,omitempty" jsonschema_description:"Properties of the object to insert"`
}
