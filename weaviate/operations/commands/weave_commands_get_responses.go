package commands


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveCommandsGetOK Successful response

swagger:response weaveCommandsGetOK
*/
type WeaveCommandsGetOK struct {

	// In: body
	Payload *models.Command `json:"body,omitempty"`
}

// NewWeaveCommandsGetOK creates WeaveCommandsGetOK with default headers values
func NewWeaveCommandsGetOK() *WeaveCommandsGetOK {
	return &WeaveCommandsGetOK{}
}

// WithPayload adds the payload to the weave commands get o k response
func (o *WeaveCommandsGetOK) WithPayload(payload *models.Command) *WeaveCommandsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave commands get o k response
func (o *WeaveCommandsGetOK) SetPayload(payload *models.Command) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveCommandsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
