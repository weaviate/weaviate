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

// WeaviateAdaptersListOKCode is the HTTP code returned for type WeaviateAdaptersListOK
const WeaviateAdaptersListOKCode int = 200

/*WeaviateAdaptersListOK Successful response.

swagger:response weaviateAdaptersListOK
*/
type WeaviateAdaptersListOK struct {

	/*
	  In: Body
	*/
	Payload *models.AdaptersListResponse `json:"body,omitempty"`
}

// NewWeaviateAdaptersListOK creates WeaviateAdaptersListOK with default headers values
func NewWeaviateAdaptersListOK() *WeaviateAdaptersListOK {
	return &WeaviateAdaptersListOK{}
}

// WithPayload adds the payload to the weaviate adapters list o k response
func (o *WeaviateAdaptersListOK) WithPayload(payload *models.AdaptersListResponse) *WeaviateAdaptersListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate adapters list o k response
func (o *WeaviateAdaptersListOK) SetPayload(payload *models.AdaptersListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateAdaptersListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateAdaptersListNoContentCode is the HTTP code returned for type WeaviateAdaptersListNoContent
const WeaviateAdaptersListNoContentCode int = 204

/*WeaviateAdaptersListNoContent Successful query result but no content

swagger:response weaviateAdaptersListNoContent
*/
type WeaviateAdaptersListNoContent struct {
}

// NewWeaviateAdaptersListNoContent creates WeaviateAdaptersListNoContent with default headers values
func NewWeaviateAdaptersListNoContent() *WeaviateAdaptersListNoContent {
	return &WeaviateAdaptersListNoContent{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersListNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(204)
}

// WeaviateAdaptersListNotImplementedCode is the HTTP code returned for type WeaviateAdaptersListNotImplemented
const WeaviateAdaptersListNotImplementedCode int = 501

/*WeaviateAdaptersListNotImplemented Not (yet) implemented.

swagger:response weaviateAdaptersListNotImplemented
*/
type WeaviateAdaptersListNotImplemented struct {
}

// NewWeaviateAdaptersListNotImplemented creates WeaviateAdaptersListNotImplemented with default headers values
func NewWeaviateAdaptersListNotImplemented() *WeaviateAdaptersListNotImplemented {
	return &WeaviateAdaptersListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
