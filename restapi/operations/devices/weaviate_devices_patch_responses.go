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

// WeaviateDevicesPatchOKCode is the HTTP code returned for type WeaviateDevicesPatchOK
const WeaviateDevicesPatchOKCode int = 200

/*WeaviateDevicesPatchOK Successful update.

swagger:response weaviateDevicesPatchOK
*/
type WeaviateDevicesPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaviateDevicesPatchOK creates WeaviateDevicesPatchOK with default headers values
func NewWeaviateDevicesPatchOK() *WeaviateDevicesPatchOK {
	return &WeaviateDevicesPatchOK{}
}

// WithPayload adds the payload to the weaviate devices patch o k response
func (o *WeaviateDevicesPatchOK) WithPayload(payload *models.Device) *WeaviateDevicesPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate devices patch o k response
func (o *WeaviateDevicesPatchOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateDevicesPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateDevicesPatchNotImplementedCode is the HTTP code returned for type WeaviateDevicesPatchNotImplemented
const WeaviateDevicesPatchNotImplementedCode int = 501

/*WeaviateDevicesPatchNotImplemented Not (yet) implemented.

swagger:response weaviateDevicesPatchNotImplemented
*/
type WeaviateDevicesPatchNotImplemented struct {
}

// NewWeaviateDevicesPatchNotImplemented creates WeaviateDevicesPatchNotImplemented with default headers values
func NewWeaviateDevicesPatchNotImplemented() *WeaviateDevicesPatchNotImplemented {
	return &WeaviateDevicesPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateDevicesPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
