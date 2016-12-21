package rooms


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveRoomsModifyOK Successful response

swagger:response weaveRoomsModifyOK
*/
type WeaveRoomsModifyOK struct {

	// In: body
	Payload *models.Room `json:"body,omitempty"`
}

// NewWeaveRoomsModifyOK creates WeaveRoomsModifyOK with default headers values
func NewWeaveRoomsModifyOK() *WeaveRoomsModifyOK {
	return &WeaveRoomsModifyOK{}
}

// WithPayload adds the payload to the weave rooms modify o k response
func (o *WeaveRoomsModifyOK) WithPayload(payload *models.Room) *WeaveRoomsModifyOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave rooms modify o k response
func (o *WeaveRoomsModifyOK) SetPayload(payload *models.Room) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveRoomsModifyOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
