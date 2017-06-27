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
	"github.com/go-openapi/strfmt"
)

// DatabaseObject for a new row in de database
type DatabaseObject struct {
	Uuid           string           // uuid, also used in Object's id
	Owner          string           // uuid of the owner
	RefType        string           // type, as defined
	CreateTimeMs   int64            // creation time in ms
	Object         string           // the JSON object, id will be collected from current uuid
	Deleted        bool             // if true, it does not exsist anymore
	RelatedObjects ObjectReferences // references to other objects
}

// ObjectReferences contains IDs that link to other objects
type ObjectReferences struct {
	ThingID strfmt.UUID `json:"thingID,omitempty"`
}

// DatabaseObjects type that is reused a lot
type DatabaseObjects []DatabaseObject

// Functions for sorting on CreateTimeMs
func (slice DatabaseObjects) Len() int {
	return len(slice)
}

func (slice DatabaseObjects) Less(i, j int) bool {
	return slice[i].CreateTimeMs > slice[j].CreateTimeMs
}

func (slice DatabaseObjects) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// DatabaseUsersObject for a new row in de database
type DatabaseUsersObject struct {
	Uuid           string // uuid, also used in Object's id
	KeyToken       string // uuid, token to login
	KeyExpiresUnix int64  // uuid of the owner
	Object         string // type, as defined
	Parent         string // Parent Uuid (not key)
	Deleted        bool   // if true, it does not exsist anymore
}

// DatabaseUsersObjectsObject is an Object of DatabaseUsersObject
type DatabaseUsersObjectsObject struct {
	Delete   bool     `json:"delete"`
	Email    string   `json:"email"`
	Execute  bool     `json:"execute"`
	IPOrigin []string `json:"ipOrigin"`
	Read     bool     `json:"read"`
	Write    bool     `json:"write"`
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
