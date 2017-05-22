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
 package things




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateThingsDeleteNoContentCode is the HTTP code returned for type WeaviateThingsDeleteNoContent
const WeaviateThingsDeleteNoContentCode int = 204

/*WeaviateThingsDeleteNoContent Successful deleted.

swagger:response weaviateThingsDeleteNoContent
*/
type WeaviateThingsDeleteNoContent struct {
}

// NewWeaviateThingsDeleteNoContent creates WeaviateThingsDeleteNoContent with default headers values
func NewWeaviateThingsDeleteNoContent() *WeaviateThingsDeleteNoContent {
	return &WeaviateThingsDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateThingsDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateThingsDeleteUnauthorizedCode is the HTTP code returned for type WeaviateThingsDeleteUnauthorized
const WeaviateThingsDeleteUnauthorizedCode int = 401

/*WeaviateThingsDeleteUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsDeleteUnauthorized
*/
type WeaviateThingsDeleteUnauthorized struct {
}

// NewWeaviateThingsDeleteUnauthorized creates WeaviateThingsDeleteUnauthorized with default headers values
func NewWeaviateThingsDeleteUnauthorized() *WeaviateThingsDeleteUnauthorized {
	return &WeaviateThingsDeleteUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsDeleteUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsDeleteForbiddenCode is the HTTP code returned for type WeaviateThingsDeleteForbidden
const WeaviateThingsDeleteForbiddenCode int = 403

/*WeaviateThingsDeleteForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsDeleteForbidden
*/
type WeaviateThingsDeleteForbidden struct {
}

// NewWeaviateThingsDeleteForbidden creates WeaviateThingsDeleteForbidden with default headers values
func NewWeaviateThingsDeleteForbidden() *WeaviateThingsDeleteForbidden {
	return &WeaviateThingsDeleteForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsDeleteForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsDeleteNotFoundCode is the HTTP code returned for type WeaviateThingsDeleteNotFound
const WeaviateThingsDeleteNotFoundCode int = 404

/*WeaviateThingsDeleteNotFound Successful query result but no resource was found.

swagger:response weaviateThingsDeleteNotFound
*/
type WeaviateThingsDeleteNotFound struct {
}

// NewWeaviateThingsDeleteNotFound creates WeaviateThingsDeleteNotFound with default headers values
func NewWeaviateThingsDeleteNotFound() *WeaviateThingsDeleteNotFound {
	return &WeaviateThingsDeleteNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingsDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingsDeleteNotImplementedCode is the HTTP code returned for type WeaviateThingsDeleteNotImplemented
const WeaviateThingsDeleteNotImplementedCode int = 501

/*WeaviateThingsDeleteNotImplemented Not (yet) implemented.

swagger:response weaviateThingsDeleteNotImplemented
*/
type WeaviateThingsDeleteNotImplemented struct {
}

// NewWeaviateThingsDeleteNotImplemented creates WeaviateThingsDeleteNotImplemented with default headers values
func NewWeaviateThingsDeleteNotImplemented() *WeaviateThingsDeleteNotImplemented {
	return &WeaviateThingsDeleteNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsDeleteNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
