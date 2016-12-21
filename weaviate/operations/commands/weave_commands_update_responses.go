package commands


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveCommandsUpdateOK Successful response

swagger:response weaveCommandsUpdateOK
*/
type WeaveCommandsUpdateOK struct {

	// In: body
	Payload *models.Command `json:"body,omitempty"`
}

// NewWeaveCommandsUpdateOK creates WeaveCommandsUpdateOK with default headers values
func NewWeaveCommandsUpdateOK() *WeaveCommandsUpdateOK {
	return &WeaveCommandsUpdateOK{}
}

// WithPayload adds the payload to the weave commands update o k response
func (o *WeaveCommandsUpdateOK) WithPayload(payload *models.Command) *WeaveCommandsUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave commands update o k response
func (o *WeaveCommandsUpdateOK) SetPayload(payload *models.Command) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveCommandsUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
