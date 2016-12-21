package personalized_infos


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeavePersonalizedInfosGetOK Successful response

swagger:response weavePersonalizedInfosGetOK
*/
type WeavePersonalizedInfosGetOK struct {

	// In: body
	Payload *models.PersonalizedInfo `json:"body,omitempty"`
}

// NewWeavePersonalizedInfosGetOK creates WeavePersonalizedInfosGetOK with default headers values
func NewWeavePersonalizedInfosGetOK() *WeavePersonalizedInfosGetOK {
	return &WeavePersonalizedInfosGetOK{}
}

// WithPayload adds the payload to the weave personalized infos get o k response
func (o *WeavePersonalizedInfosGetOK) WithPayload(payload *models.PersonalizedInfo) *WeavePersonalizedInfosGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave personalized infos get o k response
func (o *WeavePersonalizedInfosGetOK) SetPayload(payload *models.PersonalizedInfo) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePersonalizedInfosGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
