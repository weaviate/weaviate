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

package keys

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateKeysMeDeleteNoContentCode is the HTTP code returned for type WeaviateKeysMeDeleteNoContent
const WeaviateKeysMeDeleteNoContentCode int = 204

/*WeaviateKeysMeDeleteNoContent Successful deleted.

swagger:response weaviateKeysMeDeleteNoContent
*/
type WeaviateKeysMeDeleteNoContent struct {
}

// NewWeaviateKeysMeDeleteNoContent creates WeaviateKeysMeDeleteNoContent with default headers values
func NewWeaviateKeysMeDeleteNoContent() *WeaviateKeysMeDeleteNoContent {
	return &WeaviateKeysMeDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateKeysMeDeleteUnauthorizedCode is the HTTP code returned for type WeaviateKeysMeDeleteUnauthorized
const WeaviateKeysMeDeleteUnauthorizedCode int = 401

/*WeaviateKeysMeDeleteUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateKeysMeDeleteUnauthorized
*/
type WeaviateKeysMeDeleteUnauthorized struct {
}

// NewWeaviateKeysMeDeleteUnauthorized creates WeaviateKeysMeDeleteUnauthorized with default headers values
func NewWeaviateKeysMeDeleteUnauthorized() *WeaviateKeysMeDeleteUnauthorized {
	return &WeaviateKeysMeDeleteUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeDeleteUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateKeysMeDeleteNotFoundCode is the HTTP code returned for type WeaviateKeysMeDeleteNotFound
const WeaviateKeysMeDeleteNotFoundCode int = 404

/*WeaviateKeysMeDeleteNotFound Successful query result but no resource was found.

swagger:response weaviateKeysMeDeleteNotFound
*/
type WeaviateKeysMeDeleteNotFound struct {
}

// NewWeaviateKeysMeDeleteNotFound creates WeaviateKeysMeDeleteNotFound with default headers values
func NewWeaviateKeysMeDeleteNotFound() *WeaviateKeysMeDeleteNotFound {
	return &WeaviateKeysMeDeleteNotFound{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateKeysMeDeleteNotImplementedCode is the HTTP code returned for type WeaviateKeysMeDeleteNotImplemented
const WeaviateKeysMeDeleteNotImplementedCode int = 501

/*WeaviateKeysMeDeleteNotImplemented Not (yet) implemented.

swagger:response weaviateKeysMeDeleteNotImplemented
*/
type WeaviateKeysMeDeleteNotImplemented struct {
}

// NewWeaviateKeysMeDeleteNotImplemented creates WeaviateKeysMeDeleteNotImplemented with default headers values
func NewWeaviateKeysMeDeleteNotImplemented() *WeaviateKeysMeDeleteNotImplemented {
	return &WeaviateKeysMeDeleteNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeDeleteNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
