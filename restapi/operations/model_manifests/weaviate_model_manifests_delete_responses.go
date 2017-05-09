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
