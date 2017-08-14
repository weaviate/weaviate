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
   

package groups

 
 

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateGroupsDeleteNoContentCode is the HTTP code returned for type WeaviateGroupsDeleteNoContent
const WeaviateGroupsDeleteNoContentCode int = 204

/*WeaviateGroupsDeleteNoContent Successful deleted.

swagger:response weaviateGroupsDeleteNoContent
*/
type WeaviateGroupsDeleteNoContent struct {
}

// NewWeaviateGroupsDeleteNoContent creates WeaviateGroupsDeleteNoContent with default headers values
func NewWeaviateGroupsDeleteNoContent() *WeaviateGroupsDeleteNoContent {
	return &WeaviateGroupsDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateGroupsDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateGroupsDeleteUnauthorizedCode is the HTTP code returned for type WeaviateGroupsDeleteUnauthorized
const WeaviateGroupsDeleteUnauthorizedCode int = 401

/*WeaviateGroupsDeleteUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateGroupsDeleteUnauthorized
*/
type WeaviateGroupsDeleteUnauthorized struct {
}

// NewWeaviateGroupsDeleteUnauthorized creates WeaviateGroupsDeleteUnauthorized with default headers values
func NewWeaviateGroupsDeleteUnauthorized() *WeaviateGroupsDeleteUnauthorized {
	return &WeaviateGroupsDeleteUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateGroupsDeleteUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateGroupsDeleteForbiddenCode is the HTTP code returned for type WeaviateGroupsDeleteForbidden
const WeaviateGroupsDeleteForbiddenCode int = 403

/*WeaviateGroupsDeleteForbidden The used API-key has insufficient permissions.

swagger:response weaviateGroupsDeleteForbidden
*/
type WeaviateGroupsDeleteForbidden struct {
}

// NewWeaviateGroupsDeleteForbidden creates WeaviateGroupsDeleteForbidden with default headers values
func NewWeaviateGroupsDeleteForbidden() *WeaviateGroupsDeleteForbidden {
	return &WeaviateGroupsDeleteForbidden{}
}

// WriteResponse to the client
func (o *WeaviateGroupsDeleteForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateGroupsDeleteNotFoundCode is the HTTP code returned for type WeaviateGroupsDeleteNotFound
const WeaviateGroupsDeleteNotFoundCode int = 404

/*WeaviateGroupsDeleteNotFound Successful query result but no resource was found.

swagger:response weaviateGroupsDeleteNotFound
*/
type WeaviateGroupsDeleteNotFound struct {
}

// NewWeaviateGroupsDeleteNotFound creates WeaviateGroupsDeleteNotFound with default headers values
func NewWeaviateGroupsDeleteNotFound() *WeaviateGroupsDeleteNotFound {
	return &WeaviateGroupsDeleteNotFound{}
}

// WriteResponse to the client
func (o *WeaviateGroupsDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateGroupsDeleteNotImplementedCode is the HTTP code returned for type WeaviateGroupsDeleteNotImplemented
const WeaviateGroupsDeleteNotImplementedCode int = 501

/*WeaviateGroupsDeleteNotImplemented Not (yet) implemented.

swagger:response weaviateGroupsDeleteNotImplemented
*/
type WeaviateGroupsDeleteNotImplemented struct {
}

// NewWeaviateGroupsDeleteNotImplemented creates WeaviateGroupsDeleteNotImplemented with default headers values
func NewWeaviateGroupsDeleteNotImplemented() *WeaviateGroupsDeleteNotImplemented {
	return &WeaviateGroupsDeleteNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateGroupsDeleteNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
