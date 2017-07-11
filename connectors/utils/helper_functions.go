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
	"log"
	"net"
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

// CreateFirstUserObject creates a new user with new API key when none exists when starting server
func CreateFirstUserObject() DatabaseUsersObject {
	dbObject := DatabaseUsersObject{}

	// Create key token
	dbObject.KeyToken = fmt.Sprintf("%v", gouuid.NewV4())

	// Uuid + name
	uuid := fmt.Sprintf("%v", gouuid.NewV4())

	// Auto set the parent ID to root *
	dbObject.Parent = "*"

	// Set Uuid
	dbObject.Uuid = uuid

	// Set expiry to unlimited
	dbObject.KeyExpiresUnix = -1

	// Set chmod variables
	dbObjectObject := DatabaseUsersObjectsObject{}
	dbObjectObject.Read = true
	dbObjectObject.Write = true
	dbObjectObject.Delete = true
	dbObjectObject.Execute = true

	// Get ips as v6
	var ips []string
	ifaces, _ := net.Interfaces()
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			ipv6 := ip.To16()
			ips = append(ips, ipv6.String())
		}
	}

	dbObjectObject.IPOrigin = ips

	// Marshall and add to object
	dbObjectObjectJSON, _ := json.Marshal(dbObjectObject)
	dbObject.Object = string(dbObjectObjectJSON)

	// Print the key
	log.Println("INFO: No root key was found, a new root key is created. More info: https://github.com/weaviate/weaviate/blob/develop/README.md#authentication")
	log.Println("INFO: Auto set allowed IPs to: ", dbObjectObject.IPOrigin)
	log.Println("ROOTKEY=" + dbObject.KeyToken)

	return dbObject
}

// NowUnix returns the current Unix time
func NowUnix() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
