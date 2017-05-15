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
 package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateDevicesDeleteNoContentCode is the HTTP code returned for type WeaviateDevicesDeleteNoContent
const WeaviateDevicesDeleteNoContentCode int = 204

/*WeaviateDevicesDeleteNoContent Successful deleted.

swagger:response weaviateDevicesDeleteNoContent
*/
type WeaviateDevicesDeleteNoContent struct {
}

// NewWeaviateDevicesDeleteNoContent creates WeaviateDevicesDeleteNoContent with default headers values
func NewWeaviateDevicesDeleteNoContent() *WeaviateDevicesDeleteNoContent {
	return &WeaviateDevicesDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateDevicesDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateDevicesDeleteUnauthorizedCode is the HTTP code returned for type WeaviateDevicesDeleteUnauthorized
const WeaviateDevicesDeleteUnauthorizedCode int = 401

/*WeaviateDevicesDeleteUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateDevicesDeleteUnauthorized
*/
type WeaviateDevicesDeleteUnauthorized struct {
}

// NewWeaviateDevicesDeleteUnauthorized creates WeaviateDevicesDeleteUnauthorized with default headers values
func NewWeaviateDevicesDeleteUnauthorized() *WeaviateDevicesDeleteUnauthorized {
	return &WeaviateDevicesDeleteUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateDevicesDeleteUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateDevicesDeleteForbiddenCode is the HTTP code returned for type WeaviateDevicesDeleteForbidden
const WeaviateDevicesDeleteForbiddenCode int = 403

/*WeaviateDevicesDeleteForbidden The used API-key has insufficient permissions.

swagger:response weaviateDevicesDeleteForbidden
*/
type WeaviateDevicesDeleteForbidden struct {
}

// NewWeaviateDevicesDeleteForbidden creates WeaviateDevicesDeleteForbidden with default headers values
func NewWeaviateDevicesDeleteForbidden() *WeaviateDevicesDeleteForbidden {
	return &WeaviateDevicesDeleteForbidden{}
}

// WriteResponse to the client
func (o *WeaviateDevicesDeleteForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateDevicesDeleteNotFoundCode is the HTTP code returned for type WeaviateDevicesDeleteNotFound
const WeaviateDevicesDeleteNotFoundCode int = 404

/*WeaviateDevicesDeleteNotFound Successful query result but no resource was found.

swagger:response weaviateDevicesDeleteNotFound
*/
type WeaviateDevicesDeleteNotFound struct {
}

// NewWeaviateDevicesDeleteNotFound creates WeaviateDevicesDeleteNotFound with default headers values
func NewWeaviateDevicesDeleteNotFound() *WeaviateDevicesDeleteNotFound {
	return &WeaviateDevicesDeleteNotFound{}
}

// WriteResponse to the client
func (o *WeaviateDevicesDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateDevicesDeleteNotImplementedCode is the HTTP code returned for type WeaviateDevicesDeleteNotImplemented
const WeaviateDevicesDeleteNotImplementedCode int = 501

/*WeaviateDevicesDeleteNotImplemented Not (yet) implemented.

swagger:response weaviateDevicesDeleteNotImplemented
*/
type WeaviateDevicesDeleteNotImplemented struct {
}

// NewWeaviateDevicesDeleteNotImplemented creates WeaviateDevicesDeleteNotImplemented with default headers values
func NewWeaviateDevicesDeleteNotImplemented() *WeaviateDevicesDeleteNotImplemented {
	return &WeaviateDevicesDeleteNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateDevicesDeleteNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
