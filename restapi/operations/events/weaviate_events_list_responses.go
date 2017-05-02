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
 package events




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateEventsListOKCode is the HTTP code returned for type WeaviateEventsListOK
const WeaviateEventsListOKCode int = 200

/*WeaviateEventsListOK Successful response.

swagger:response weaviateEventsListOK
*/
type WeaviateEventsListOK struct {

	/*
	  In: Body
	*/
	Payload *models.EventsListResponse `json:"body,omitempty"`
}

// NewWeaviateEventsListOK creates WeaviateEventsListOK with default headers values
func NewWeaviateEventsListOK() *WeaviateEventsListOK {
	return &WeaviateEventsListOK{}
}

// WithPayload adds the payload to the weaviate events list o k response
func (o *WeaviateEventsListOK) WithPayload(payload *models.EventsListResponse) *WeaviateEventsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate events list o k response
func (o *WeaviateEventsListOK) SetPayload(payload *models.EventsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateEventsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateEventsListNoContentCode is the HTTP code returned for type WeaviateEventsListNoContent
const WeaviateEventsListNoContentCode int = 204

/*WeaviateEventsListNoContent Successful query result but no content

swagger:response weaviateEventsListNoContent
*/
type WeaviateEventsListNoContent struct {
}

// NewWeaviateEventsListNoContent creates WeaviateEventsListNoContent with default headers values
func NewWeaviateEventsListNoContent() *WeaviateEventsListNoContent {
	return &WeaviateEventsListNoContent{}
}

// WriteResponse to the client
func (o *WeaviateEventsListNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateEventsListNotImplementedCode is the HTTP code returned for type WeaviateEventsListNotImplemented
const WeaviateEventsListNotImplementedCode int = 501

/*WeaviateEventsListNotImplemented Not (yet) implemented.

swagger:response weaviateEventsListNotImplemented
*/
type WeaviateEventsListNotImplemented struct {
}

// NewWeaviateEventsListNotImplemented creates WeaviateEventsListNotImplemented with default headers values
func NewWeaviateEventsListNotImplemented() *WeaviateEventsListNotImplemented {
	return &WeaviateEventsListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateEventsListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
