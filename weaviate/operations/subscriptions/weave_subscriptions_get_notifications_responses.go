package subscriptions


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
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
