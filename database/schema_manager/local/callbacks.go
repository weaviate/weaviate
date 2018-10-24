package local

import (
	db_schema "github.com/creativesoftwarefdn/weaviate/database/schema"
)

func (l *LocalSchemaManager) RegisterSchemaUpdateCallback(callback func(updatedSchema db_schema.Schema)) {
	l.callbacks = append(l.callbacks, callback)
}

func (l *LocalSchemaManager) triggerCallbacks() {
	schema := l.GetSchema()

	for _, cb := range l.callbacks {
		cb(schema)
	}
}
