/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package connector_utils

import (
	"encoding/json"
	"fmt"
	"time"

	gouuid "github.com/satori/go.uuid"
)

// NewDatabaseObjectFromPrincipal creates a new object with default values, out of principle object
func NewDatabaseObjectFromPrincipal(principal interface{}, refType string) *DatabaseObject {
	// Get user object
	UsersObject, _ := PrincipalMarshalling(principal)

	// Generate DatabaseObject without JSON-object in it.
	dbObject := NewDatabaseObject(UsersObject.Uuid, refType)

	return dbObject
}

// SetCreateTimeMsToNow gives the Object the current time in mili seconds
func (f *DatabaseObject) SetCreateTimeMsToNow() {
	f.CreateTimeMs = time.Now().UnixNano() / int64(time.Millisecond)
}

// GenerateAndSetUUID generates and sets a new Uuid
func (f *DatabaseObject) GenerateAndSetUUID() {
	f.Uuid = fmt.Sprintf("%v", gouuid.NewV4())
}

// MakeObjectDeleted gives the Object the current time in mili seconds and marks it as deleted
func (f *DatabaseObject) MakeObjectDeleted() {
	f.Deleted = true
	f.SetCreateTimeMsToNow()
}

// MergeRequestBodyIntoObject merges the Object with right body
func (f *DatabaseObject) MergeRequestBodyIntoObject(body interface{}) {
	databaseBody, _ := json.Marshal(body)
	f.Object = string(databaseBody)
}

// PrincipalMarshalling Marhshall and Unmarshall Principal and Principals Objects
func PrincipalMarshalling(Object interface{}) (DatabaseUsersObject, DatabaseUsersObjectsObject) {
	// marshall principal
	principalMarshall, _ := json.Marshal(Object)
	var Principal DatabaseUsersObject
	json.Unmarshal(principalMarshall, &Principal)

	// Unmarshall the Object inside the Principal (aka ObjectsObject)
	var ObjectsObject DatabaseUsersObjectsObject
	json.Unmarshal([]byte(Principal.Object), &ObjectsObject)

	return Principal, ObjectsObject
}

// ReadAllowed checks if reading is allowed
func ReadAllowed(validateObject interface{}) bool {
	_, ObjectsObject := PrincipalMarshalling(validateObject)
	return ObjectsObject.Read
}

// WriteAllowed checks if writing is allowed
func WriteAllowed(validateObject interface{}) bool {
	_, ObjectsObject := PrincipalMarshalling(validateObject)
	return ObjectsObject.Write
}

// DeleteAllowed checks if deleting is allowed
func DeleteAllowed(validateObject interface{}) bool {
	_, ObjectsObject := PrincipalMarshalling(validateObject)
	return ObjectsObject.Delete
}
