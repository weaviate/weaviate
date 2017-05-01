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

// WeaviateACLEntriesInsertAcceptedCode is the HTTP code returned for type WeaviateACLEntriesInsertAccepted
const WeaviateACLEntriesInsertAcceptedCode int = 202

/*WeaviateACLEntriesInsertAccepted Successfully received.

swagger:response weaviateAclEntriesInsertAccepted
*/
type WeaviateACLEntriesInsertAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.ACLEntry `json:"body,omitempty"`
}

// NewWeaviateACLEntriesInsertAccepted creates WeaviateACLEntriesInsertAccepted with default headers values
func NewWeaviateACLEntriesInsertAccepted() *WeaviateACLEntriesInsertAccepted {
	return &WeaviateACLEntriesInsertAccepted{}
}

// WithPayload adds the payload to the weaviate Acl entries insert accepted response
func (o *WeaviateACLEntriesInsertAccepted) WithPayload(payload *models.ACLEntry) *WeaviateACLEntriesInsertAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate Acl entries insert accepted response
func (o *WeaviateACLEntriesInsertAccepted) SetPayload(payload *models.ACLEntry) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateACLEntriesInsertAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateACLEntriesInsertNotImplementedCode is the HTTP code returned for type WeaviateACLEntriesInsertNotImplemented
const WeaviateACLEntriesInsertNotImplementedCode int = 501

/*WeaviateACLEntriesInsertNotImplemented Not (yet) implemented.

swagger:response weaviateAclEntriesInsertNotImplemented
*/
type WeaviateACLEntriesInsertNotImplemented struct {
}

// NewWeaviateACLEntriesInsertNotImplemented creates WeaviateACLEntriesInsertNotImplemented with default headers values
func NewWeaviateACLEntriesInsertNotImplemented() *WeaviateACLEntriesInsertNotImplemented {
	return &WeaviateACLEntriesInsertNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateACLEntriesInsertNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
