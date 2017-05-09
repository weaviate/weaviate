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
 package acl_entries




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateACLEntriesDeleteNoContentCode is the HTTP code returned for type WeaviateACLEntriesDeleteNoContent
const WeaviateACLEntriesDeleteNoContentCode int = 204

/*WeaviateACLEntriesDeleteNoContent Successful deleted.

swagger:response weaviateAclEntriesDeleteNoContent
*/
type WeaviateACLEntriesDeleteNoContent struct {
}

// NewWeaviateACLEntriesDeleteNoContent creates WeaviateACLEntriesDeleteNoContent with default headers values
func NewWeaviateACLEntriesDeleteNoContent() *WeaviateACLEntriesDeleteNoContent {
	return &WeaviateACLEntriesDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateACLEntriesDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateACLEntriesDeleteNotFoundCode is the HTTP code returned for type WeaviateACLEntriesDeleteNotFound
const WeaviateACLEntriesDeleteNotFoundCode int = 404

/*WeaviateACLEntriesDeleteNotFound Successful query result but no resource was found.

swagger:response weaviateAclEntriesDeleteNotFound
*/
type WeaviateACLEntriesDeleteNotFound struct {
}

// NewWeaviateACLEntriesDeleteNotFound creates WeaviateACLEntriesDeleteNotFound with default headers values
func NewWeaviateACLEntriesDeleteNotFound() *WeaviateACLEntriesDeleteNotFound {
	return &WeaviateACLEntriesDeleteNotFound{}
}

// WriteResponse to the client
func (o *WeaviateACLEntriesDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateACLEntriesDeleteNotImplementedCode is the HTTP code returned for type WeaviateACLEntriesDeleteNotImplemented
const WeaviateACLEntriesDeleteNotImplementedCode int = 501

/*WeaviateACLEntriesDeleteNotImplemented Not (yet) implemented.

swagger:response weaviateAclEntriesDeleteNotImplemented
*/
type WeaviateACLEntriesDeleteNotImplemented struct {
}

// NewWeaviateACLEntriesDeleteNotImplemented creates WeaviateACLEntriesDeleteNotImplemented with default headers values
func NewWeaviateACLEntriesDeleteNotImplemented() *WeaviateACLEntriesDeleteNotImplemented {
	return &WeaviateACLEntriesDeleteNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateACLEntriesDeleteNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
