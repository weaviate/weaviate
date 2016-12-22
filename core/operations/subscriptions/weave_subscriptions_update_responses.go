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
 package subscriptions




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveSubscriptionsUpdateOK Successful response

swagger:response weaveSubscriptionsUpdateOK
*/
type WeaveSubscriptionsUpdateOK struct {

	// In: body
	Payload *models.Subscription `json:"body,omitempty"`
}

// NewWeaveSubscriptionsUpdateOK creates WeaveSubscriptionsUpdateOK with default headers values
func NewWeaveSubscriptionsUpdateOK() *WeaveSubscriptionsUpdateOK {
	return &WeaveSubscriptionsUpdateOK{}
}

// WithPayload adds the payload to the weave subscriptions update o k response
func (o *WeaveSubscriptionsUpdateOK) WithPayload(payload *models.Subscription) *WeaveSubscriptionsUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave subscriptions update o k response
func (o *WeaveSubscriptionsUpdateOK) SetPayload(payload *models.Subscription) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveSubscriptionsUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
