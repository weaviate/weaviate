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
 package locations




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateLocationsDeleteNoContentCode is the HTTP code returned for type WeaviateLocationsDeleteNoContent
const WeaviateLocationsDeleteNoContentCode int = 204

/*WeaviateLocationsDeleteNoContent Successful deleted.

swagger:response weaviateLocationsDeleteNoContent
*/
type WeaviateLocationsDeleteNoContent struct {
}

// NewWeaviateLocationsDeleteNoContent creates WeaviateLocationsDeleteNoContent with default headers values
func NewWeaviateLocationsDeleteNoContent() *WeaviateLocationsDeleteNoContent {
	return &WeaviateLocationsDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateLocationsDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateLocationsDeleteNotImplementedCode is the HTTP code returned for type WeaviateLocationsDeleteNotImplemented
const WeaviateLocationsDeleteNotImplementedCode int = 501

/*WeaviateLocationsDeleteNotImplemented Not (yet) implemented.

swagger:response weaviateLocationsDeleteNotImplemented
*/
type WeaviateLocationsDeleteNotImplemented struct {
}

// NewWeaviateLocationsDeleteNotImplemented creates WeaviateLocationsDeleteNotImplemented with default headers values
func NewWeaviateLocationsDeleteNotImplemented() *WeaviateLocationsDeleteNotImplemented {
	return &WeaviateLocationsDeleteNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateLocationsDeleteNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
