package subscriptions




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveSubscriptionsListOK Successful response

swagger:response weaveSubscriptionsListOK
*/
type WeaveSubscriptionsListOK struct {

	// In: body
	Payload *models.SubscriptionsListResponse `json:"body,omitempty"`
}

// NewWeaveSubscriptionsListOK creates WeaveSubscriptionsListOK with default headers values
func NewWeaveSubscriptionsListOK() *WeaveSubscriptionsListOK {
	return &WeaveSubscriptionsListOK{}
}

// WithPayload adds the payload to the weave subscriptions list o k response
func (o *WeaveSubscriptionsListOK) WithPayload(payload *models.SubscriptionsListResponse) *WeaveSubscriptionsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave subscriptions list o k response
func (o *WeaveSubscriptionsListOK) SetPayload(payload *models.SubscriptionsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveSubscriptionsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
