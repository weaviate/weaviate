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
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package subscriptions




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

/*WeaveSubscriptionsDeleteOK Successful response

swagger:response weaveSubscriptionsDeleteOK
*/
type WeaveSubscriptionsDeleteOK struct {
}

// NewWeaveSubscriptionsDeleteOK creates WeaveSubscriptionsDeleteOK with default headers values
func NewWeaveSubscriptionsDeleteOK() *WeaveSubscriptionsDeleteOK {
	return &WeaveSubscriptionsDeleteOK{}
}

// WriteResponse to the client
func (o *WeaveSubscriptionsDeleteOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}
