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
package mysql

import (
	"log"
	"time"

	"github.com/weaviate/weaviate/connectors"
)

type Mysql struct{}

type Object struct {
	Uuid         string // uuid, also used in Object's id
	Owner        string // uuid of the owner
	RefType      string // type, as defined
	CreateTimeMs int64  // creation time in ms
	Object       string // the JSON object, id will be collected from current uuid
	Deleted      bool   // if true, it does not exsist anymore
}

func (f *Mysql) Connect() error {
	return nil
}

func (f *Mysql) Init() error {
	return nil
}

func (f *Mysql) Add(object dbconnector.DatabaseObject) (string, error) {
	log.Fatalf("Connecting to Mysql DB - NOTE ONLY FOR DEMO PURPOSE")

	return "IM NOT USED", nil
}

func (f *Mysql) Get(Uuid string) (dbconnector.DatabaseObject, error) {

	task := dbconnector.DatabaseObject{
		Uuid:         "temp",
		Owner:        "temp",
		RefType:      "temp",
		CreateTimeMs: time.Now().UnixNano() / int64(time.Millisecond),
		Object:       "temp",
		Deleted:      false,
	}

	return task, nil
}

func (f *Mysql) List(refType string, limit int) ([]dbconnector.DatabaseObject, error) {
	dataObjs := []dbconnector.DatabaseObject{}

	return dataObjs, nil
}

// Validate if a user has access, returns permissions object
func (f *Mysql) ValidateKey(token string) ([]dbconnector.DatabaseUsersObject, error) {

	dbUsersObjects := []dbconnector.DatabaseUsersObject{}

	var err error

	// keys are found, return true
	return dbUsersObjects, err
}

// AddUser to DB
func (f *Mysql) AddKey(parentUuid string, dbObject dbconnector.DatabaseUsersObject) (dbconnector.DatabaseUsersObject, error) {

	return dbObject, nil

}
