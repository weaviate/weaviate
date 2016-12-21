package registration_tickets


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveRegistrationTicketsGetOK Successful response

swagger:response weaveRegistrationTicketsGetOK
*/
type WeaveRegistrationTicketsGetOK struct {

	// In: body
	Payload *models.RegistrationTicket `json:"body,omitempty"`
}

// NewWeaveRegistrationTicketsGetOK creates WeaveRegistrationTicketsGetOK with default headers values
func NewWeaveRegistrationTicketsGetOK() *WeaveRegistrationTicketsGetOK {
	return &WeaveRegistrationTicketsGetOK{}
}

// WithPayload adds the payload to the weave registration tickets get o k response
func (o *WeaveRegistrationTicketsGetOK) WithPayload(payload *models.RegistrationTicket) *WeaveRegistrationTicketsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave registration tickets get o k response
func (o *WeaveRegistrationTicketsGetOK) SetPayload(payload *models.RegistrationTicket) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveRegistrationTicketsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
