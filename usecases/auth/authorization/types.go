//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authorization

import (
	"fmt"

	"github.com/go-openapi/strfmt"
)

const (
	// HEAD: Represents the HTTP HEAD method, which is used to retrieve metadata of a resource without
	// fetching the resource itself.
	HEAD = "head"
	// VALIDATE: Represents a custom action to validate a resource.
	// This is not a standard HTTP method but can be used for specific validation operations.
	VALIDATE = "validate"
	// LIST: Represents a custom action to list resources.
	// This is not a standard HTTP method but can be used to list multiple resources.
	LIST = "list"
	// GET: Represents the HTTP GET method, which is used to retrieve a resource.
	GET = "get"
	// ADD: Represents a custom action to add a resource.
	// This is not a standard HTTP method but can be used for specific add operations.
	ADD = "add"
	// CREATE: Represents the HTTP POST method, which is used to create a new resource.
	CREATE = "create"
	// RESTORE: Represents a custom action to restore a resource.
	// This is not a standard HTTP method but can be used for specific restore operations.
	RESTORE = "restore"
	// DELETE: Represents the HTTP DELETE method, which is used to delete a resource.
	DELETE = "delete"
	// UPDATE: Represents the HTTP PUT method, which is used to update an existing resource.
	UPDATE = "update"
)

const (
	// ALL_SCHEMA represents all schema-related resources.
	ALL_SCHEMA = "schema/*"
	// SCHEMA_TENANTS represents the schema tenants resource.
	SCHEMA_TENANTS = "schema/tenants"
	// SCHEMA_OBJECTS represents the schema objects resource.
	SCHEMA_OBJECTS = "schema/objects"
	// ALL_TRAVERSAL represents all traversal-related resources.
	ALL_TRAVERSAL = "traversal/*"
	// ALL_CLASSIFICATIONS represents all classification-related resources.
	ALL_CLASSIFICATIONS = "classifications/*"
	// NODES represents the nodes resource.
	NODES = "nodes"
	// CLUSTER represents the cluster resource.
	CLUSTER = "cluster"
	// ALL_BATCH represents all batch-related resources.
	ALL_BATCH = "batch/*"
	// BATCH_OBJECTS represents the batch objects resource.
	BATCH_OBJECTS = "batch/objects"
	// OBJECTS represents the objects resource.
	OBJECTS = "objects"
)

// TODO add translation layer between weaviate and Casbin permissions

// SchemaShard returns the path for a specific schema shard.
// Parameters:
// - class: The class name.
// - shard: The shard name.
// Returns: The formatted schema shard path.
func SchemaShard(class, shard string) string {
	if class != "" && shard != "" {
		return fmt.Sprintf("schema/%s/shards/%s", class, shard)
	}

	return fmt.Sprintf("schema/%s/shards", class)
}

// Objects returns the path for a specific object or class.
// Parameters:
// - class: The class name (optional).
// - id: The object ID (optional).
// Returns: The formatted objects path.
func Objects(class string, id strfmt.UUID) string {
	if class != "" && id != "" {
		return fmt.Sprintf("objects/%s/%s", class, id.String())
	}

	if id != "" {
		return fmt.Sprintf("objects/%s", id.String())
	}

	if class != "" {
		return fmt.Sprintf("objects/%s", class)
	}

	return OBJECTS
}

// Backup returns the path for a specific backup.
// Parameters:
// - backend: The backup backend name.
// - id: The backup ID (optional).
// Returns: The formatted backup path.
func Backup(backend, id string) string {
	if id == "" {
		return fmt.Sprintf("backups/%s", backend)
	}
	return fmt.Sprintf("backups/%s/%s", backend, id)
}

// Restore returns the path for restoring a specific backup.
// Parameters:
// - backend: The backup backend name.
// - id: The backup ID.
// Returns: The formatted restore path.
func Restore(backend, id string) string {
	return fmt.Sprintf("backups/%s/%s/restore", backend, id)
}
