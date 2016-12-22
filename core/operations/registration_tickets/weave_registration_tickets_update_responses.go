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
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package registration_tickets




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveRegistrationTicketsUpdateOK Successful response

swagger:response weaveRegistrationTicketsUpdateOK
*/
type WeaveRegistrationTicketsUpdateOK struct {

	// In: body
	Payload *models.RegistrationTicket `json:"body,omitempty"`
}

// NewWeaveRegistrationTicketsUpdateOK creates WeaveRegistrationTicketsUpdateOK with default headers values
func NewWeaveRegistrationTicketsUpdateOK() *WeaveRegistrationTicketsUpdateOK {
	return &WeaveRegistrationTicketsUpdateOK{}
}

// WithPayload adds the payload to the weave registration tickets update o k response
func (o *WeaveRegistrationTicketsUpdateOK) WithPayload(payload *models.RegistrationTicket) *WeaveRegistrationTicketsUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave registration tickets update o k response
func (o *WeaveRegistrationTicketsUpdateOK) SetPayload(payload *models.RegistrationTicket) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveRegistrationTicketsUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
