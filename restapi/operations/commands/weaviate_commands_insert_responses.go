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
 package commands




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateCommandsInsertAcceptedCode is the HTTP code returned for type WeaviateCommandsInsertAccepted
const WeaviateCommandsInsertAcceptedCode int = 202

/*WeaviateCommandsInsertAccepted Successfully received.

swagger:response weaviateCommandsInsertAccepted
*/
type WeaviateCommandsInsertAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.Command `json:"body,omitempty"`
}

// NewWeaviateCommandsInsertAccepted creates WeaviateCommandsInsertAccepted with default headers values
func NewWeaviateCommandsInsertAccepted() *WeaviateCommandsInsertAccepted {
	return &WeaviateCommandsInsertAccepted{}
}

// WithPayload adds the payload to the weaviate commands insert accepted response
func (o *WeaviateCommandsInsertAccepted) WithPayload(payload *models.Command) *WeaviateCommandsInsertAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate commands insert accepted response
func (o *WeaviateCommandsInsertAccepted) SetPayload(payload *models.Command) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateCommandsInsertAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateCommandsInsertNotImplementedCode is the HTTP code returned for type WeaviateCommandsInsertNotImplemented
const WeaviateCommandsInsertNotImplementedCode int = 501

/*WeaviateCommandsInsertNotImplemented Not (yet) implemented.

swagger:response weaviateCommandsInsertNotImplemented
*/
type WeaviateCommandsInsertNotImplemented struct {
}

// NewWeaviateCommandsInsertNotImplemented creates WeaviateCommandsInsertNotImplemented with default headers values
func NewWeaviateCommandsInsertNotImplemented() *WeaviateCommandsInsertNotImplemented {
	return &WeaviateCommandsInsertNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateCommandsInsertNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
