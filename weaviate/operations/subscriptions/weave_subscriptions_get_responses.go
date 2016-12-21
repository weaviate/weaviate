package subscriptions




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveSubscriptionsGetOK Successful response

swagger:response weaveSubscriptionsGetOK
*/
type WeaveSubscriptionsGetOK struct {

	// In: body
	Payload *models.Subscription `json:"body,omitempty"`
}

// NewWeaveSubscriptionsGetOK creates WeaveSubscriptionsGetOK with default headers values
func NewWeaveSubscriptionsGetOK() *WeaveSubscriptionsGetOK {
	return &WeaveSubscriptionsGetOK{}
}

// WithPayload adds the payload to the weave subscriptions get o k response
func (o *WeaveSubscriptionsGetOK) WithPayload(payload *models.Subscription) *WeaveSubscriptionsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave subscriptions get o k response
func (o *WeaveSubscriptionsGetOK) SetPayload(payload *models.Subscription) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveSubscriptionsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
