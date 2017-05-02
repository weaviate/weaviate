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

	"github.com/weaviate/weaviate/models"
)

// WeaviateACLEntriesGetOKCode is the HTTP code returned for type WeaviateACLEntriesGetOK
const WeaviateACLEntriesGetOKCode int = 200

/*WeaviateACLEntriesGetOK Successful response.

swagger:response weaviateAclEntriesGetOK
*/
type WeaviateACLEntriesGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.ACLEntry `json:"body,omitempty"`
}

// NewWeaviateACLEntriesGetOK creates WeaviateACLEntriesGetOK with default headers values
func NewWeaviateACLEntriesGetOK() *WeaviateACLEntriesGetOK {
	return &WeaviateACLEntriesGetOK{}
}

// WithPayload adds the payload to the weaviate Acl entries get o k response
func (o *WeaviateACLEntriesGetOK) WithPayload(payload *models.ACLEntry) *WeaviateACLEntriesGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate Acl entries get o k response
func (o *WeaviateACLEntriesGetOK) SetPayload(payload *models.ACLEntry) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateACLEntriesGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateACLEntriesGetNoContentCode is the HTTP code returned for type WeaviateACLEntriesGetNoContent
const WeaviateACLEntriesGetNoContentCode int = 204

/*WeaviateACLEntriesGetNoContent Successful query result but no content

swagger:response weaviateAclEntriesGetNoContent
*/
type WeaviateACLEntriesGetNoContent struct {
}

// NewWeaviateACLEntriesGetNoContent creates WeaviateACLEntriesGetNoContent with default headers values
func NewWeaviateACLEntriesGetNoContent() *WeaviateACLEntriesGetNoContent {
	return &WeaviateACLEntriesGetNoContent{}
}

// WriteResponse to the client
func (o *WeaviateACLEntriesGetNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateACLEntriesGetNotImplementedCode is the HTTP code returned for type WeaviateACLEntriesGetNotImplemented
const WeaviateACLEntriesGetNotImplementedCode int = 501

/*WeaviateACLEntriesGetNotImplemented Not (yet) implemented.

swagger:response weaviateAclEntriesGetNotImplemented
*/
type WeaviateACLEntriesGetNotImplemented struct {
}

// NewWeaviateACLEntriesGetNotImplemented creates WeaviateACLEntriesGetNotImplemented with default headers values
func NewWeaviateACLEntriesGetNotImplemented() *WeaviateACLEntriesGetNotImplemented {
	return &WeaviateACLEntriesGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateACLEntriesGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
