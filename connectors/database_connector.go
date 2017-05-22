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
package dbconnector

import (
	"encoding/json"
	"fmt"
	"time"

	gouuid "github.com/satori/go.uuid"
)

// DatabaseObject for a new row in de database
type DatabaseObject struct {
	Uuid         string // uuid, also used in Object's id
	Owner        string // uuid of the owner
	RefType      string // type, as defined
	CreateTimeMs int64  // creation time in ms
	Object       string // the JSON object, id will be collected from current uuid
	Deleted      bool   // if true, it does not exsist anymore
}

// DatabaseUsersObject for a new row in de database
type DatabaseUsersObject struct {
	Uuid         string // uuid, also used in Object's id
	KeyToken     string // uuid, also used in Object's id
	KeyExpiresMs int64  // uuid of the owner
	Object       string // type, as defined
	Parent       string // Parent Uuid (not key)
}

// Object of DatabaseUsersObject
type DatabaseUsersObjectsObject struct {
	Delete   bool   `json:"Delete"`
	Email    string `json:"Email"`
	IpOrigin string `json:"IpOrigin"`
	Read     bool   `json:"Read"`
	Write    bool   `json:"Write"`
}

// NewDatabaseObject creates a new object with default values
// Note: Only owner and refType has to be filled. New object automatically gets new UUID and TIME.
// 	Time is updatable by function.
func NewDatabaseObject(owner string, refType string) *DatabaseObject {
	dbo := new(DatabaseObject)

	// Set default values
	dbo.GenerateAndSetUUID()
	dbo.SetCreateTimeMsToNow()
	dbo.Deleted = false

	// Set values by function params
	dbo.Owner = owner
	dbo.RefType = refType

	return dbo
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

// Marhshall and Unmarshall Principal and Principals Objects
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

// check if reading is allowed
func ReadAllowed(validateObject interface{}) bool {
	_, ObjectsObject := PrincipalMarshalling(validateObject)
	return ObjectsObject.Read
}

// check if writing is allowed
func WriteAllowed(validateObject interface{}) bool {
	_, ObjectsObject := PrincipalMarshalling(validateObject)
	return ObjectsObject.Write
}

// check if deleting is allowed
func DeleteAllowed(validateObject interface{}) bool {
	_, ObjectsObject := PrincipalMarshalling(validateObject)
	return ObjectsObject.Delete
}

// DatabaseConnector is the interface that all connectors should have
type DatabaseConnector interface {
	Connect() error
	Add(DatabaseObject) (string, error)
	Get(string) (DatabaseObject, error)
	List(string, int) ([]DatabaseObject, error)
	ValidateKey(string) ([]DatabaseUsersObject, error)
	AddKey(string, DatabaseUsersObject) (DatabaseUsersObject, error)
}
