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
 package places




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeavePlacesHandleInvitationOK Successful response

swagger:response weavePlacesHandleInvitationOK
*/
type WeavePlacesHandleInvitationOK struct {

	// In: body
	Payload *models.PlacesHandleInvitationResponse `json:"body,omitempty"`
}

// NewWeavePlacesHandleInvitationOK creates WeavePlacesHandleInvitationOK with default headers values
func NewWeavePlacesHandleInvitationOK() *WeavePlacesHandleInvitationOK {
	return &WeavePlacesHandleInvitationOK{}
}

// WithPayload adds the payload to the weave places handle invitation o k response
func (o *WeavePlacesHandleInvitationOK) WithPayload(payload *models.PlacesHandleInvitationResponse) *WeavePlacesHandleInvitationOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave places handle invitation o k response
func (o *WeavePlacesHandleInvitationOK) SetPayload(payload *models.PlacesHandleInvitationResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePlacesHandleInvitationOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
