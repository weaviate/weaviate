package registration_tickets




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveRegistrationTicketsFinalizeOK Successful response

swagger:response weaveRegistrationTicketsFinalizeOK
*/
type WeaveRegistrationTicketsFinalizeOK struct {

	// In: body
	Payload *models.RegistrationTicket `json:"body,omitempty"`
}

// NewWeaveRegistrationTicketsFinalizeOK creates WeaveRegistrationTicketsFinalizeOK with default headers values
func NewWeaveRegistrationTicketsFinalizeOK() *WeaveRegistrationTicketsFinalizeOK {
	return &WeaveRegistrationTicketsFinalizeOK{}
}

// WithPayload adds the payload to the weave registration tickets finalize o k response
func (o *WeaveRegistrationTicketsFinalizeOK) WithPayload(payload *models.RegistrationTicket) *WeaveRegistrationTicketsFinalizeOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave registration tickets finalize o k response
func (o *WeaveRegistrationTicketsFinalizeOK) SetPayload(payload *models.RegistrationTicket) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveRegistrationTicketsFinalizeOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
