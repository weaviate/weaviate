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
 package adapters




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateAdaptersGetOKCode is the HTTP code returned for type WeaviateAdaptersGetOK
const WeaviateAdaptersGetOKCode int = 200

/*WeaviateAdaptersGetOK Successful response.

swagger:response weaviateAdaptersGetOK
*/
type WeaviateAdaptersGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.Adapter `json:"body,omitempty"`
}

// NewWeaviateAdaptersGetOK creates WeaviateAdaptersGetOK with default headers values
func NewWeaviateAdaptersGetOK() *WeaviateAdaptersGetOK {
	return &WeaviateAdaptersGetOK{}
}

// WithPayload adds the payload to the weaviate adapters get o k response
func (o *WeaviateAdaptersGetOK) WithPayload(payload *models.Adapter) *WeaviateAdaptersGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate adapters get o k response
func (o *WeaviateAdaptersGetOK) SetPayload(payload *models.Adapter) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateAdaptersGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateAdaptersGetNotImplementedCode is the HTTP code returned for type WeaviateAdaptersGetNotImplemented
const WeaviateAdaptersGetNotImplementedCode int = 501

/*WeaviateAdaptersGetNotImplemented Not (yet) implemented.

swagger:response weaviateAdaptersGetNotImplemented
*/
type WeaviateAdaptersGetNotImplemented struct {
}

// NewWeaviateAdaptersGetNotImplemented creates WeaviateAdaptersGetNotImplemented with default headers values
func NewWeaviateAdaptersGetNotImplemented() *WeaviateAdaptersGetNotImplemented {
	return &WeaviateAdaptersGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
