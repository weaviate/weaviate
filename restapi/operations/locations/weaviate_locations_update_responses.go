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

// WeaviateLocationsUpdateOKCode is the HTTP code returned for type WeaviateLocationsUpdateOK
const WeaviateLocationsUpdateOKCode int = 200

/*WeaviateLocationsUpdateOK Successful updated.

swagger:response weaviateLocationsUpdateOK
*/
type WeaviateLocationsUpdateOK struct {

	/*
	  In: Body
	*/
	Payload *models.Location `json:"body,omitempty"`
}

// NewWeaviateLocationsUpdateOK creates WeaviateLocationsUpdateOK with default headers values
func NewWeaviateLocationsUpdateOK() *WeaviateLocationsUpdateOK {
	return &WeaviateLocationsUpdateOK{}
}

// WithPayload adds the payload to the weaviate locations update o k response
func (o *WeaviateLocationsUpdateOK) WithPayload(payload *models.Location) *WeaviateLocationsUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate locations update o k response
func (o *WeaviateLocationsUpdateOK) SetPayload(payload *models.Location) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateLocationsUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateLocationsUpdateNotFoundCode is the HTTP code returned for type WeaviateLocationsUpdateNotFound
const WeaviateLocationsUpdateNotFoundCode int = 404

/*WeaviateLocationsUpdateNotFound Successful query result but no resource was found.

swagger:response weaviateLocationsUpdateNotFound
*/
type WeaviateLocationsUpdateNotFound struct {
}

// NewWeaviateLocationsUpdateNotFound creates WeaviateLocationsUpdateNotFound with default headers values
func NewWeaviateLocationsUpdateNotFound() *WeaviateLocationsUpdateNotFound {
	return &WeaviateLocationsUpdateNotFound{}
}

// WriteResponse to the client
func (o *WeaviateLocationsUpdateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateLocationsUpdateNotImplementedCode is the HTTP code returned for type WeaviateLocationsUpdateNotImplemented
const WeaviateLocationsUpdateNotImplementedCode int = 501

/*WeaviateLocationsUpdateNotImplemented Not (yet) implemented.

swagger:response weaviateLocationsUpdateNotImplemented
*/
type WeaviateLocationsUpdateNotImplemented struct {
}

// NewWeaviateLocationsUpdateNotImplemented creates WeaviateLocationsUpdateNotImplemented with default headers values
func NewWeaviateLocationsUpdateNotImplemented() *WeaviateLocationsUpdateNotImplemented {
	return &WeaviateLocationsUpdateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateLocationsUpdateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
