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
import "github.com/go-openapi/strfmt"
import "github.com/weaviate/weaviate/models"

// RefTypeAction used for actions in DB and requests
const RefTypeAction string = "Action"

// RefTypeKey used for keys in DB and requests
const RefTypeKey string = "Key"

// RefTypeThing used for things in DB and requests
const RefTypeThing string = "Thing"

// Key for a new row in de database
type Key struct {
	KeyToken strfmt.UUID // UUID, token to login
	Parent   string      // UUID or *
	Root     bool
	UUID     strfmt.UUID // UUID, object's key
	models.KeyCreate
}
