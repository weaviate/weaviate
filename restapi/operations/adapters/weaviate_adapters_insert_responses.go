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

// WeaviateAdaptersInsertAcceptedCode is the HTTP code returned for type WeaviateAdaptersInsertAccepted
const WeaviateAdaptersInsertAcceptedCode int = 202

/*WeaviateAdaptersInsertAccepted Successfully received.

swagger:response weaviateAdaptersInsertAccepted
*/
type WeaviateAdaptersInsertAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.Adapter `json:"body,omitempty"`
}

// NewWeaviateAdaptersInsertAccepted creates WeaviateAdaptersInsertAccepted with default headers values
func NewWeaviateAdaptersInsertAccepted() *WeaviateAdaptersInsertAccepted {
	return &WeaviateAdaptersInsertAccepted{}
}

// WithPayload adds the payload to the weaviate adapters insert accepted response
func (o *WeaviateAdaptersInsertAccepted) WithPayload(payload *models.Adapter) *WeaviateAdaptersInsertAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate adapters insert accepted response
func (o *WeaviateAdaptersInsertAccepted) SetPayload(payload *models.Adapter) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateAdaptersInsertAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateAdaptersInsertNotImplementedCode is the HTTP code returned for type WeaviateAdaptersInsertNotImplemented
const WeaviateAdaptersInsertNotImplementedCode int = 501

/*WeaviateAdaptersInsertNotImplemented Not (yet) implemented.

swagger:response weaviateAdaptersInsertNotImplemented
*/
type WeaviateAdaptersInsertNotImplemented struct {
}

// NewWeaviateAdaptersInsertNotImplemented creates WeaviateAdaptersInsertNotImplemented with default headers values
func NewWeaviateAdaptersInsertNotImplemented() *WeaviateAdaptersInsertNotImplemented {
	return &WeaviateAdaptersInsertNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersInsertNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
