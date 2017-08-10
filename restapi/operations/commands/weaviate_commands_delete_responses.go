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
   

package commands

 
 

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateCommandsDeleteNoContentCode is the HTTP code returned for type WeaviateCommandsDeleteNoContent
const WeaviateCommandsDeleteNoContentCode int = 204

/*WeaviateCommandsDeleteNoContent Successful deleted.

swagger:response weaviateCommandsDeleteNoContent
*/
type WeaviateCommandsDeleteNoContent struct {
}

// NewWeaviateCommandsDeleteNoContent creates WeaviateCommandsDeleteNoContent with default headers values
func NewWeaviateCommandsDeleteNoContent() *WeaviateCommandsDeleteNoContent {
	return &WeaviateCommandsDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateCommandsDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateCommandsDeleteUnauthorizedCode is the HTTP code returned for type WeaviateCommandsDeleteUnauthorized
const WeaviateCommandsDeleteUnauthorizedCode int = 401

/*WeaviateCommandsDeleteUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateCommandsDeleteUnauthorized
*/
type WeaviateCommandsDeleteUnauthorized struct {
}

// NewWeaviateCommandsDeleteUnauthorized creates WeaviateCommandsDeleteUnauthorized with default headers values
func NewWeaviateCommandsDeleteUnauthorized() *WeaviateCommandsDeleteUnauthorized {
	return &WeaviateCommandsDeleteUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateCommandsDeleteUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateCommandsDeleteForbiddenCode is the HTTP code returned for type WeaviateCommandsDeleteForbidden
const WeaviateCommandsDeleteForbiddenCode int = 403

/*WeaviateCommandsDeleteForbidden The used API-key has insufficient permissions.

swagger:response weaviateCommandsDeleteForbidden
*/
type WeaviateCommandsDeleteForbidden struct {
}

// NewWeaviateCommandsDeleteForbidden creates WeaviateCommandsDeleteForbidden with default headers values
func NewWeaviateCommandsDeleteForbidden() *WeaviateCommandsDeleteForbidden {
	return &WeaviateCommandsDeleteForbidden{}
}

// WriteResponse to the client
func (o *WeaviateCommandsDeleteForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateCommandsDeleteNotFoundCode is the HTTP code returned for type WeaviateCommandsDeleteNotFound
const WeaviateCommandsDeleteNotFoundCode int = 404

/*WeaviateCommandsDeleteNotFound Successful query result but no resource was found.

swagger:response weaviateCommandsDeleteNotFound
*/
type WeaviateCommandsDeleteNotFound struct {
}

// NewWeaviateCommandsDeleteNotFound creates WeaviateCommandsDeleteNotFound with default headers values
func NewWeaviateCommandsDeleteNotFound() *WeaviateCommandsDeleteNotFound {
	return &WeaviateCommandsDeleteNotFound{}
}

// WriteResponse to the client
func (o *WeaviateCommandsDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateCommandsDeleteNotImplementedCode is the HTTP code returned for type WeaviateCommandsDeleteNotImplemented
const WeaviateCommandsDeleteNotImplementedCode int = 501

/*WeaviateCommandsDeleteNotImplemented Not (yet) implemented.

swagger:response weaviateCommandsDeleteNotImplemented
*/
type WeaviateCommandsDeleteNotImplemented struct {
}

// NewWeaviateCommandsDeleteNotImplemented creates WeaviateCommandsDeleteNotImplemented with default headers values
func NewWeaviateCommandsDeleteNotImplemented() *WeaviateCommandsDeleteNotImplemented {
	return &WeaviateCommandsDeleteNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateCommandsDeleteNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
