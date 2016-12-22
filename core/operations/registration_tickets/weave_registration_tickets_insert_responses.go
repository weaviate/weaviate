package registration_tickets




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveRegistrationTicketsInsertOK Successful response

swagger:response weaveRegistrationTicketsInsertOK
*/
type WeaveRegistrationTicketsInsertOK struct {

	// In: body
	Payload *models.RegistrationTicket `json:"body,omitempty"`
}

// NewWeaveRegistrationTicketsInsertOK creates WeaveRegistrationTicketsInsertOK with default headers values
func NewWeaveRegistrationTicketsInsertOK() *WeaveRegistrationTicketsInsertOK {
	return &WeaveRegistrationTicketsInsertOK{}
}

// WithPayload adds the payload to the weave registration tickets insert o k response
func (o *WeaveRegistrationTicketsInsertOK) WithPayload(payload *models.RegistrationTicket) *WeaveRegistrationTicketsInsertOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave registration tickets insert o k response
func (o *WeaveRegistrationTicketsInsertOK) SetPayload(payload *models.RegistrationTicket) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveRegistrationTicketsInsertOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
