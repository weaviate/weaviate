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

// WeaviateAdaptersUpdateOKCode is the HTTP code returned for type WeaviateAdaptersUpdateOK
const WeaviateAdaptersUpdateOKCode int = 200

/*WeaviateAdaptersUpdateOK Successful updated.

swagger:response weaviateAdaptersUpdateOK
*/
type WeaviateAdaptersUpdateOK struct {

	/*
	  In: Body
	*/
	Payload *models.Adapter `json:"body,omitempty"`
}

// NewWeaviateAdaptersUpdateOK creates WeaviateAdaptersUpdateOK with default headers values
func NewWeaviateAdaptersUpdateOK() *WeaviateAdaptersUpdateOK {
	return &WeaviateAdaptersUpdateOK{}
}

// WithPayload adds the payload to the weaviate adapters update o k response
func (o *WeaviateAdaptersUpdateOK) WithPayload(payload *models.Adapter) *WeaviateAdaptersUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate adapters update o k response
func (o *WeaviateAdaptersUpdateOK) SetPayload(payload *models.Adapter) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateAdaptersUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateAdaptersUpdateNotImplementedCode is the HTTP code returned for type WeaviateAdaptersUpdateNotImplemented
const WeaviateAdaptersUpdateNotImplementedCode int = 501

/*WeaviateAdaptersUpdateNotImplemented Not (yet) implemented.

swagger:response weaviateAdaptersUpdateNotImplemented
*/
type WeaviateAdaptersUpdateNotImplemented struct {
}

// NewWeaviateAdaptersUpdateNotImplemented creates WeaviateAdaptersUpdateNotImplemented with default headers values
func NewWeaviateAdaptersUpdateNotImplemented() *WeaviateAdaptersUpdateNotImplemented {
	return &WeaviateAdaptersUpdateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersUpdateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
