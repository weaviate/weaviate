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

// WeaviateDevicesGetOKCode is the HTTP code returned for type WeaviateDevicesGetOK
const WeaviateDevicesGetOKCode int = 200

/*WeaviateDevicesGetOK Successful response.

swagger:response weaviateDevicesGetOK
*/
type WeaviateDevicesGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaviateDevicesGetOK creates WeaviateDevicesGetOK with default headers values
func NewWeaviateDevicesGetOK() *WeaviateDevicesGetOK {
	return &WeaviateDevicesGetOK{}
}

// WithPayload adds the payload to the weaviate devices get o k response
func (o *WeaviateDevicesGetOK) WithPayload(payload *models.Device) *WeaviateDevicesGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate devices get o k response
func (o *WeaviateDevicesGetOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateDevicesGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateDevicesGetNotFoundCode is the HTTP code returned for type WeaviateDevicesGetNotFound
const WeaviateDevicesGetNotFoundCode int = 404

/*WeaviateDevicesGetNotFound Successful query result but no resource was found

swagger:response weaviateDevicesGetNotFound
*/
type WeaviateDevicesGetNotFound struct {
}

// NewWeaviateDevicesGetNotFound creates WeaviateDevicesGetNotFound with default headers values
func NewWeaviateDevicesGetNotFound() *WeaviateDevicesGetNotFound {
	return &WeaviateDevicesGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateDevicesGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateDevicesGetNotImplementedCode is the HTTP code returned for type WeaviateDevicesGetNotImplemented
const WeaviateDevicesGetNotImplementedCode int = 501

/*WeaviateDevicesGetNotImplemented Not (yet) implemented.

swagger:response weaviateDevicesGetNotImplemented
*/
type WeaviateDevicesGetNotImplemented struct {
}

// NewWeaviateDevicesGetNotImplemented creates WeaviateDevicesGetNotImplemented with default headers values
func NewWeaviateDevicesGetNotImplemented() *WeaviateDevicesGetNotImplemented {
	return &WeaviateDevicesGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateDevicesGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
