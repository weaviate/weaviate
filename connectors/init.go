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
package dbinit

type Object struct {
	Uuid         string // uuid, also used in Object's id
	Owner        string // uuid of the owner
	RefType      string // type, as defined
	CreateTimeMs int64  // creation time in ms
	Object       string // the JSON object, id will be collected from current uuid
	Deleted      bool   // if true, it does not exsist anymore
}

// The interface that all connectors should have
type Intfc interface {
	Connect() bool
	Add(string, string, string) string
	Get(string) Object
}
