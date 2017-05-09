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
 package locations




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateLocationsPatchOKCode is the HTTP code returned for type WeaviateLocationsPatchOK
const WeaviateLocationsPatchOKCode int = 200

/*WeaviateLocationsPatchOK Successful updated.

swagger:response weaviateLocationsPatchOK
*/
type WeaviateLocationsPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.Location `json:"body,omitempty"`
}

// NewWeaviateLocationsPatchOK creates WeaviateLocationsPatchOK with default headers values
func NewWeaviateLocationsPatchOK() *WeaviateLocationsPatchOK {
	return &WeaviateLocationsPatchOK{}
}

// WithPayload adds the payload to the weaviate locations patch o k response
func (o *WeaviateLocationsPatchOK) WithPayload(payload *models.Location) *WeaviateLocationsPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate locations patch o k response
func (o *WeaviateLocationsPatchOK) SetPayload(payload *models.Location) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateLocationsPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateLocationsPatchNotFoundCode is the HTTP code returned for type WeaviateLocationsPatchNotFound
const WeaviateLocationsPatchNotFoundCode int = 404

/*WeaviateLocationsPatchNotFound Successful query result but no resource was found.

swagger:response weaviateLocationsPatchNotFound
*/
type WeaviateLocationsPatchNotFound struct {
}

// NewWeaviateLocationsPatchNotFound creates WeaviateLocationsPatchNotFound with default headers values
func NewWeaviateLocationsPatchNotFound() *WeaviateLocationsPatchNotFound {
	return &WeaviateLocationsPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateLocationsPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateLocationsPatchNotImplementedCode is the HTTP code returned for type WeaviateLocationsPatchNotImplemented
const WeaviateLocationsPatchNotImplementedCode int = 501

/*WeaviateLocationsPatchNotImplemented Not (yet) implemented.

swagger:response weaviateLocationsPatchNotImplemented
*/
type WeaviateLocationsPatchNotImplemented struct {
}

// NewWeaviateLocationsPatchNotImplemented creates WeaviateLocationsPatchNotImplemented with default headers values
func NewWeaviateLocationsPatchNotImplemented() *WeaviateLocationsPatchNotImplemented {
	return &WeaviateLocationsPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateLocationsPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
