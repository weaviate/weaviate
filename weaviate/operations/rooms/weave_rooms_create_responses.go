package rooms


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveRoomsCreateOK Successful response

swagger:response weaveRoomsCreateOK
*/
type WeaveRoomsCreateOK struct {

	// In: body
	Payload *models.Room `json:"body,omitempty"`
}

// NewWeaveRoomsCreateOK creates WeaveRoomsCreateOK with default headers values
func NewWeaveRoomsCreateOK() *WeaveRoomsCreateOK {
	return &WeaveRoomsCreateOK{}
}

// WithPayload adds the payload to the weave rooms create o k response
func (o *WeaveRoomsCreateOK) WithPayload(payload *models.Room) *WeaveRoomsCreateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave rooms create o k response
func (o *WeaveRoomsCreateOK) SetPayload(payload *models.Room) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveRoomsCreateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
