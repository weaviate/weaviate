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
