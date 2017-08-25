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

import ()

// Key for a new row in de database
type Key struct {
	UUID           string         // uuid, also used in Object's id
	KeyToken       string         // uuid, token to login
	KeyExpiresUnix int64          // expiry time in unix timestamp
	Permissions    KeyPermissions // type, as defined
	Parent         string         // Parent Uuid (not key)
	Deleted        bool           // if true, it does not exsist anymore
}

// KeyPermissions is an Object of Key
type KeyPermissions struct {
	Delete   bool     `json:"delete"`
	Email    string   `json:"email"`
	Execute  bool     `json:"execute"`
	IPOrigin []string `json:"ipOrigin"`
	Read     bool     `json:"read"`
	Write    bool     `json:"write"`
}
