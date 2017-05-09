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

// WeaviateACLEntriesPatchOKCode is the HTTP code returned for type WeaviateACLEntriesPatchOK
const WeaviateACLEntriesPatchOKCode int = 200

/*WeaviateACLEntriesPatchOK Successful updated.

swagger:response weaviateAclEntriesPatchOK
*/
type WeaviateACLEntriesPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.ACLEntry `json:"body,omitempty"`
}

// NewWeaviateACLEntriesPatchOK creates WeaviateACLEntriesPatchOK with default headers values
func NewWeaviateACLEntriesPatchOK() *WeaviateACLEntriesPatchOK {
	return &WeaviateACLEntriesPatchOK{}
}

// WithPayload adds the payload to the weaviate Acl entries patch o k response
func (o *WeaviateACLEntriesPatchOK) WithPayload(payload *models.ACLEntry) *WeaviateACLEntriesPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate Acl entries patch o k response
func (o *WeaviateACLEntriesPatchOK) SetPayload(payload *models.ACLEntry) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateACLEntriesPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateACLEntriesPatchNotFoundCode is the HTTP code returned for type WeaviateACLEntriesPatchNotFound
const WeaviateACLEntriesPatchNotFoundCode int = 404

/*WeaviateACLEntriesPatchNotFound Successful query result but no resource was found.

swagger:response weaviateAclEntriesPatchNotFound
*/
type WeaviateACLEntriesPatchNotFound struct {
}

// NewWeaviateACLEntriesPatchNotFound creates WeaviateACLEntriesPatchNotFound with default headers values
func NewWeaviateACLEntriesPatchNotFound() *WeaviateACLEntriesPatchNotFound {
	return &WeaviateACLEntriesPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateACLEntriesPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateACLEntriesPatchNotImplementedCode is the HTTP code returned for type WeaviateACLEntriesPatchNotImplemented
const WeaviateACLEntriesPatchNotImplementedCode int = 501

/*WeaviateACLEntriesPatchNotImplemented Not (yet) implemented.

swagger:response weaviateAclEntriesPatchNotImplemented
*/
type WeaviateACLEntriesPatchNotImplemented struct {
}

// NewWeaviateACLEntriesPatchNotImplemented creates WeaviateACLEntriesPatchNotImplemented with default headers values
func NewWeaviateACLEntriesPatchNotImplemented() *WeaviateACLEntriesPatchNotImplemented {
	return &WeaviateACLEntriesPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateACLEntriesPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
