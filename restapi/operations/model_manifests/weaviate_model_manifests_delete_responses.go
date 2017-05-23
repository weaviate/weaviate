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
 package model_manifests




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateModelManifestsDeleteNoContentCode is the HTTP code returned for type WeaviateModelManifestsDeleteNoContent
const WeaviateModelManifestsDeleteNoContentCode int = 204

/*WeaviateModelManifestsDeleteNoContent Successful deleted.

swagger:response weaviateModelManifestsDeleteNoContent
*/
type WeaviateModelManifestsDeleteNoContent struct {
}

// NewWeaviateModelManifestsDeleteNoContent creates WeaviateModelManifestsDeleteNoContent with default headers values
func NewWeaviateModelManifestsDeleteNoContent() *WeaviateModelManifestsDeleteNoContent {
	return &WeaviateModelManifestsDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateModelManifestsDeleteUnauthorizedCode is the HTTP code returned for type WeaviateModelManifestsDeleteUnauthorized
const WeaviateModelManifestsDeleteUnauthorizedCode int = 401

/*WeaviateModelManifestsDeleteUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateModelManifestsDeleteUnauthorized
*/
type WeaviateModelManifestsDeleteUnauthorized struct {
}

// NewWeaviateModelManifestsDeleteUnauthorized creates WeaviateModelManifestsDeleteUnauthorized with default headers values
func NewWeaviateModelManifestsDeleteUnauthorized() *WeaviateModelManifestsDeleteUnauthorized {
	return &WeaviateModelManifestsDeleteUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsDeleteUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateModelManifestsDeleteForbiddenCode is the HTTP code returned for type WeaviateModelManifestsDeleteForbidden
const WeaviateModelManifestsDeleteForbiddenCode int = 403

/*WeaviateModelManifestsDeleteForbidden The used API-key has insufficient permissions.

swagger:response weaviateModelManifestsDeleteForbidden
*/
type WeaviateModelManifestsDeleteForbidden struct {
}

// NewWeaviateModelManifestsDeleteForbidden creates WeaviateModelManifestsDeleteForbidden with default headers values
func NewWeaviateModelManifestsDeleteForbidden() *WeaviateModelManifestsDeleteForbidden {
	return &WeaviateModelManifestsDeleteForbidden{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsDeleteForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateModelManifestsDeleteNotFoundCode is the HTTP code returned for type WeaviateModelManifestsDeleteNotFound
const WeaviateModelManifestsDeleteNotFoundCode int = 404

/*WeaviateModelManifestsDeleteNotFound Successful query result but no resource was found.

swagger:response weaviateModelManifestsDeleteNotFound
*/
type WeaviateModelManifestsDeleteNotFound struct {
}

// NewWeaviateModelManifestsDeleteNotFound creates WeaviateModelManifestsDeleteNotFound with default headers values
func NewWeaviateModelManifestsDeleteNotFound() *WeaviateModelManifestsDeleteNotFound {
	return &WeaviateModelManifestsDeleteNotFound{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateModelManifestsDeleteNotImplementedCode is the HTTP code returned for type WeaviateModelManifestsDeleteNotImplemented
const WeaviateModelManifestsDeleteNotImplementedCode int = 501

/*WeaviateModelManifestsDeleteNotImplemented Not (yet) implemented.

swagger:response weaviateModelManifestsDeleteNotImplemented
*/
type WeaviateModelManifestsDeleteNotImplemented struct {
}

// NewWeaviateModelManifestsDeleteNotImplemented creates WeaviateModelManifestsDeleteNotImplemented with default headers values
func NewWeaviateModelManifestsDeleteNotImplemented() *WeaviateModelManifestsDeleteNotImplemented {
	return &WeaviateModelManifestsDeleteNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsDeleteNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
