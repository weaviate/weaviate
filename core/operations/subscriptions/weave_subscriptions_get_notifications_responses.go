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

/*WeaveSubscriptionsGetNotificationsOK Successful response

swagger:response weaveSubscriptionsGetNotificationsOK
*/
type WeaveSubscriptionsGetNotificationsOK struct {

	// In: body
	Payload *models.SubscriptionsGetNotificationsResponse `json:"body,omitempty"`
}

// NewWeaveSubscriptionsGetNotificationsOK creates WeaveSubscriptionsGetNotificationsOK with default headers values
func NewWeaveSubscriptionsGetNotificationsOK() *WeaveSubscriptionsGetNotificationsOK {
	return &WeaveSubscriptionsGetNotificationsOK{}
}

// WithPayload adds the payload to the weave subscriptions get notifications o k response
func (o *WeaveSubscriptionsGetNotificationsOK) WithPayload(payload *models.SubscriptionsGetNotificationsResponse) *WeaveSubscriptionsGetNotificationsOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave subscriptions get notifications o k response
func (o *WeaveSubscriptionsGetNotificationsOK) SetPayload(payload *models.SubscriptionsGetNotificationsResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveSubscriptionsGetNotificationsOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
