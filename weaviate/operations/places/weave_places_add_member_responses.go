package places




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeavePlacesAddMemberOK Successful response

swagger:response weavePlacesAddMemberOK
*/
type WeavePlacesAddMemberOK struct {

	// In: body
	Payload *models.PlacesAddMemberResponse `json:"body,omitempty"`
}

// NewWeavePlacesAddMemberOK creates WeavePlacesAddMemberOK with default headers values
func NewWeavePlacesAddMemberOK() *WeavePlacesAddMemberOK {
	return &WeavePlacesAddMemberOK{}
}

// WithPayload adds the payload to the weave places add member o k response
func (o *WeavePlacesAddMemberOK) WithPayload(payload *models.PlacesAddMemberResponse) *WeavePlacesAddMemberOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave places add member o k response
func (o *WeavePlacesAddMemberOK) SetPayload(payload *models.PlacesAddMemberResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePlacesAddMemberOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
