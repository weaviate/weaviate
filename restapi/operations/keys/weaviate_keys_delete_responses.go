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

// WeaviateKeysDeleteNoContentCode is the HTTP code returned for type WeaviateKeysDeleteNoContent
const WeaviateKeysDeleteNoContentCode int = 204

/*WeaviateKeysDeleteNoContent Successful deleted.

swagger:response weaviateKeysDeleteNoContent
*/
type WeaviateKeysDeleteNoContent struct {
}

// NewWeaviateKeysDeleteNoContent creates WeaviateKeysDeleteNoContent with default headers values
func NewWeaviateKeysDeleteNoContent() *WeaviateKeysDeleteNoContent {
	return &WeaviateKeysDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateKeysDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateKeysDeleteNotFoundCode is the HTTP code returned for type WeaviateKeysDeleteNotFound
const WeaviateKeysDeleteNotFoundCode int = 404

/*WeaviateKeysDeleteNotFound Successful query result but no resource was found.

swagger:response weaviateKeysDeleteNotFound
*/
type WeaviateKeysDeleteNotFound struct {
}

// NewWeaviateKeysDeleteNotFound creates WeaviateKeysDeleteNotFound with default headers values
func NewWeaviateKeysDeleteNotFound() *WeaviateKeysDeleteNotFound {
	return &WeaviateKeysDeleteNotFound{}
}

// WriteResponse to the client
func (o *WeaviateKeysDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateKeysDeleteNotImplementedCode is the HTTP code returned for type WeaviateKeysDeleteNotImplemented
const WeaviateKeysDeleteNotImplementedCode int = 501

/*WeaviateKeysDeleteNotImplemented Not (yet) implemented.

swagger:response weaviateKeysDeleteNotImplemented
*/
type WeaviateKeysDeleteNotImplemented struct {
}

// NewWeaviateKeysDeleteNotImplemented creates WeaviateKeysDeleteNotImplemented with default headers values
func NewWeaviateKeysDeleteNotImplemented() *WeaviateKeysDeleteNotImplemented {
	return &WeaviateKeysDeleteNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateKeysDeleteNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
