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

// NewDatabaseObject creates a new object with default values
// Note: TODO EXPLAIN FLOW
func NewDatabaseObject(owner string, refType string) *DatabaseObject {
	dbo := new(DatabaseObject)

	// Set default values
	dbo.GenerateAndSetUUID()
	dbo.SetTimeToNow()
	dbo.Deleted = false

	// Set values by function params
	dbo.Owner = owner
	dbo.RefType = refType

	return dbo
}

// SetTimeToNow gives the Object the current time in mili seconds
func (f *DatabaseObject) SetTimeToNow() {
	f.CreateTimeMs = time.Now().UnixNano() / int64(time.Millisecond)
}

// GenerateAndSetUUID generates and sets a new Uuid
func (f *DatabaseObject) GenerateAndSetUUID() {
	f.Uuid = fmt.Sprintf("%v", gouuid.NewV4())
}

// DatabaseConnector is the interface that all connectors should have
type DatabaseConnector interface {
	Connect() error
	Add(DatabaseObject) (string, error)
	Get(string) (DatabaseObject, error)
	List(string, int) ([]DatabaseObject, error)
	ValidateUser(string) bool
}
