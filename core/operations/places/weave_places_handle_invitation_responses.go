package places




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeavePlacesHandleInvitationOK Successful response

swagger:response weavePlacesHandleInvitationOK
*/
type WeavePlacesHandleInvitationOK struct {

	// In: body
	Payload *models.PlacesHandleInvitationResponse `json:"body,omitempty"`
}

// NewWeavePlacesHandleInvitationOK creates WeavePlacesHandleInvitationOK with default headers values
func NewWeavePlacesHandleInvitationOK() *WeavePlacesHandleInvitationOK {
	return &WeavePlacesHandleInvitationOK{}
}

// WithPayload adds the payload to the weave places handle invitation o k response
func (o *WeavePlacesHandleInvitationOK) WithPayload(payload *models.PlacesHandleInvitationResponse) *WeavePlacesHandleInvitationOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave places handle invitation o k response
func (o *WeavePlacesHandleInvitationOK) SetPayload(payload *models.PlacesHandleInvitationResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePlacesHandleInvitationOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
