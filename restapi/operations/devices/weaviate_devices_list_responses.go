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
 package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateDevicesListOKCode is the HTTP code returned for type WeaviateDevicesListOK
const WeaviateDevicesListOKCode int = 200

/*WeaviateDevicesListOK Successful response.

swagger:response weaviateDevicesListOK
*/
type WeaviateDevicesListOK struct {

	/*
	  In: Body
	*/
	Payload *models.DevicesListResponse `json:"body,omitempty"`
}

// NewWeaviateDevicesListOK creates WeaviateDevicesListOK with default headers values
func NewWeaviateDevicesListOK() *WeaviateDevicesListOK {
	return &WeaviateDevicesListOK{}
}

// WithPayload adds the payload to the weaviate devices list o k response
func (o *WeaviateDevicesListOK) WithPayload(payload *models.DevicesListResponse) *WeaviateDevicesListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate devices list o k response
func (o *WeaviateDevicesListOK) SetPayload(payload *models.DevicesListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateDevicesListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateDevicesListNotFoundCode is the HTTP code returned for type WeaviateDevicesListNotFound
const WeaviateDevicesListNotFoundCode int = 404

/*WeaviateDevicesListNotFound Successful query result but no resource was found

swagger:response weaviateDevicesListNotFound
*/
type WeaviateDevicesListNotFound struct {
}

// NewWeaviateDevicesListNotFound creates WeaviateDevicesListNotFound with default headers values
func NewWeaviateDevicesListNotFound() *WeaviateDevicesListNotFound {
	return &WeaviateDevicesListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateDevicesListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateDevicesListNotImplementedCode is the HTTP code returned for type WeaviateDevicesListNotImplemented
const WeaviateDevicesListNotImplementedCode int = 501

/*WeaviateDevicesListNotImplemented Not (yet) implemented.

swagger:response weaviateDevicesListNotImplemented
*/
type WeaviateDevicesListNotImplemented struct {
}

// NewWeaviateDevicesListNotImplemented creates WeaviateDevicesListNotImplemented with default headers values
func NewWeaviateDevicesListNotImplemented() *WeaviateDevicesListNotImplemented {
	return &WeaviateDevicesListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateDevicesListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
