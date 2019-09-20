package classification

import "github.com/semi-technologies/weaviate/entities/schema"

type fakeSchemaGetter struct {
	schema schema.Schema
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}
