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
   

package graphql

 
 

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeavaiteGraphqlGetNotImplementedCode is the HTTP code returned for type WeavaiteGraphqlGetNotImplemented
const WeavaiteGraphqlGetNotImplementedCode int = 501

/*WeavaiteGraphqlGetNotImplemented Not (yet) implemented.

swagger:response weavaiteGraphqlGetNotImplemented
*/
type WeavaiteGraphqlGetNotImplemented struct {
}

// NewWeavaiteGraphqlGetNotImplemented creates WeavaiteGraphqlGetNotImplemented with default headers values
func NewWeavaiteGraphqlGetNotImplemented() *WeavaiteGraphqlGetNotImplemented {
	return &WeavaiteGraphqlGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeavaiteGraphqlGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
