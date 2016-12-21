package personalized_infos


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeavePersonalizedInfosPatchOK Successful response

swagger:response weavePersonalizedInfosPatchOK
*/
type WeavePersonalizedInfosPatchOK struct {

	// In: body
	Payload *models.PersonalizedInfo `json:"body,omitempty"`
}

// NewWeavePersonalizedInfosPatchOK creates WeavePersonalizedInfosPatchOK with default headers values
func NewWeavePersonalizedInfosPatchOK() *WeavePersonalizedInfosPatchOK {
	return &WeavePersonalizedInfosPatchOK{}
}

// WithPayload adds the payload to the weave personalized infos patch o k response
func (o *WeavePersonalizedInfosPatchOK) WithPayload(payload *models.PersonalizedInfo) *WeavePersonalizedInfosPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave personalized infos patch o k response
func (o *WeavePersonalizedInfosPatchOK) SetPayload(payload *models.PersonalizedInfo) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePersonalizedInfosPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
