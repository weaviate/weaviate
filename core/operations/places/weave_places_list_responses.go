package places




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeavePlacesListOK Successful response

swagger:response weavePlacesListOK
*/
type WeavePlacesListOK struct {

	// In: body
	Payload *models.PlacesListResponse `json:"body,omitempty"`
}

// NewWeavePlacesListOK creates WeavePlacesListOK with default headers values
func NewWeavePlacesListOK() *WeavePlacesListOK {
	return &WeavePlacesListOK{}
}

// WithPayload adds the payload to the weave places list o k response
func (o *WeavePlacesListOK) WithPayload(payload *models.PlacesListResponse) *WeavePlacesListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave places list o k response
func (o *WeavePlacesListOK) SetPayload(payload *models.PlacesListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePlacesListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
