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

// WeaviateEventsGetOKCode is the HTTP code returned for type WeaviateEventsGetOK
const WeaviateEventsGetOKCode int = 200

/*WeaviateEventsGetOK Successful response.

swagger:response weaviateEventsGetOK
*/
type WeaviateEventsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.Event `json:"body,omitempty"`
}

// NewWeaviateEventsGetOK creates WeaviateEventsGetOK with default headers values
func NewWeaviateEventsGetOK() *WeaviateEventsGetOK {
	return &WeaviateEventsGetOK{}
}

// WithPayload adds the payload to the weaviate events get o k response
func (o *WeaviateEventsGetOK) WithPayload(payload *models.Event) *WeaviateEventsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate events get o k response
func (o *WeaviateEventsGetOK) SetPayload(payload *models.Event) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateEventsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateEventsGetNoContentCode is the HTTP code returned for type WeaviateEventsGetNoContent
const WeaviateEventsGetNoContentCode int = 204

/*WeaviateEventsGetNoContent Successful query result but no content

swagger:response weaviateEventsGetNoContent
*/
type WeaviateEventsGetNoContent struct {
}

// NewWeaviateEventsGetNoContent creates WeaviateEventsGetNoContent with default headers values
func NewWeaviateEventsGetNoContent() *WeaviateEventsGetNoContent {
	return &WeaviateEventsGetNoContent{}
}

// WriteResponse to the client
func (o *WeaviateEventsGetNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateEventsGetNotImplementedCode is the HTTP code returned for type WeaviateEventsGetNotImplemented
const WeaviateEventsGetNotImplementedCode int = 501

/*WeaviateEventsGetNotImplemented Not (yet) implemented.

swagger:response weaviateEventsGetNotImplemented
*/
type WeaviateEventsGetNotImplemented struct {
}

// NewWeaviateEventsGetNotImplemented creates WeaviateEventsGetNotImplemented with default headers values
func NewWeaviateEventsGetNotImplemented() *WeaviateEventsGetNotImplemented {
	return &WeaviateEventsGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateEventsGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
