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
