package places




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeavePlacesCreateOK Successful response

swagger:response weavePlacesCreateOK
*/
type WeavePlacesCreateOK struct {

	// In: body
	Payload *models.PlacesCreateResponse `json:"body,omitempty"`
}

// NewWeavePlacesCreateOK creates WeavePlacesCreateOK with default headers values
func NewWeavePlacesCreateOK() *WeavePlacesCreateOK {
	return &WeavePlacesCreateOK{}
}

// WithPayload adds the payload to the weave places create o k response
func (o *WeavePlacesCreateOK) WithPayload(payload *models.PlacesCreateResponse) *WeavePlacesCreateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave places create o k response
func (o *WeavePlacesCreateOK) SetPayload(payload *models.PlacesCreateResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePlacesCreateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
