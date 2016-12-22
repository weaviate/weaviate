package subscriptions




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveSubscriptionsSubscribeOK Successful response

swagger:response weaveSubscriptionsSubscribeOK
*/
type WeaveSubscriptionsSubscribeOK struct {

	// In: body
	Payload *models.SubscriptionData `json:"body,omitempty"`
}

// NewWeaveSubscriptionsSubscribeOK creates WeaveSubscriptionsSubscribeOK with default headers values
func NewWeaveSubscriptionsSubscribeOK() *WeaveSubscriptionsSubscribeOK {
	return &WeaveSubscriptionsSubscribeOK{}
}

// WithPayload adds the payload to the weave subscriptions subscribe o k response
func (o *WeaveSubscriptionsSubscribeOK) WithPayload(payload *models.SubscriptionData) *WeaveSubscriptionsSubscribeOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave subscriptions subscribe o k response
func (o *WeaveSubscriptionsSubscribeOK) SetPayload(payload *models.SubscriptionData) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveSubscriptionsSubscribeOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
