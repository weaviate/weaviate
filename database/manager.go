/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package database

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
	db_schema "github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/database/schema_migrator"
	"github.com/go-openapi/strfmt"
)

type SchemaManager interface {
	schema_migrator.Migrator
	connector_state.StateManager

	// Update the Thing or Action schema's meta data.
	UpdateMeta(ctx context.Context, kind kind.Kind, atContext strfmt.URI, maintainer strfmt.Email, name string) error

	// Return a reference to the database schema.
	// Note that this function can be both called from having a ConnectorLock as a SchemaLock.
	GetSchema() db_schema.Schema

	// Register callbacks that will be called when the schema has been updated. These callbacks
	// will be invoked before the migration methods return.
	// Take care to not cause a deadlock by modifying the schema directly again from a callback.
	// The are also run _once_ after the system has configured itself.
	RegisterSchemaUpdateCallback(func(updatedSchema db_schema.Schema))

	// Trigger the callbacks to be send out. Used during initialization.
	TriggerSchemaUpdateCallbacks()

	// Sets a contextionary to be used for future correctness checks of the schema.
	SetContextionary(context contextionary.Contextionary)
}
