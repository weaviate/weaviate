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

/*WeavePlacesRemoveMemberOK Successful response

swagger:response weavePlacesRemoveMemberOK
*/
type WeavePlacesRemoveMemberOK struct {

	// In: body
	Payload *models.PlacesRemoveMemberResponse `json:"body,omitempty"`
}

// NewWeavePlacesRemoveMemberOK creates WeavePlacesRemoveMemberOK with default headers values
func NewWeavePlacesRemoveMemberOK() *WeavePlacesRemoveMemberOK {
	return &WeavePlacesRemoveMemberOK{}
}

// WithPayload adds the payload to the weave places remove member o k response
func (o *WeavePlacesRemoveMemberOK) WithPayload(payload *models.PlacesRemoveMemberResponse) *WeavePlacesRemoveMemberOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave places remove member o k response
func (o *WeavePlacesRemoveMemberOK) SetPayload(payload *models.PlacesRemoveMemberResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePlacesRemoveMemberOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
