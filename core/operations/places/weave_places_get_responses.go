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

/*WeavePlacesGetOK Successful response

swagger:response weavePlacesGetOK
*/
type WeavePlacesGetOK struct {

	// In: body
	Payload *models.Place `json:"body,omitempty"`
}

// NewWeavePlacesGetOK creates WeavePlacesGetOK with default headers values
func NewWeavePlacesGetOK() *WeavePlacesGetOK {
	return &WeavePlacesGetOK{}
}

// WithPayload adds the payload to the weave places get o k response
func (o *WeavePlacesGetOK) WithPayload(payload *models.Place) *WeavePlacesGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave places get o k response
func (o *WeavePlacesGetOK) SetPayload(payload *models.Place) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePlacesGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
