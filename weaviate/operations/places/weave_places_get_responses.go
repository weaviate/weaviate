package places


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeavePlacesGetOK Successful response

swagger:response weavePlacesGetOK
*/
type WeavePlacesGetOK struct {

	// In: body
	Payload *models.Place `json:"body,omitempty"`
}

// NewWeavePlacesGetOK creates WeavePlacesGetOK with default headers values
func NewWeavePlacesGetOK() *WeavePlacesGetOK {
	return &WeavePlacesGetOK{}
}

// WithPayload adds the payload to the weave places get o k response
func (o *WeavePlacesGetOK) WithPayload(payload *models.Place) *WeavePlacesGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave places get o k response
func (o *WeavePlacesGetOK) SetPayload(payload *models.Place) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePlacesGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
