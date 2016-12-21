package commands


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

/*WeaveCommandsDeleteOK Successful response

swagger:response weaveCommandsDeleteOK
*/
type WeaveCommandsDeleteOK struct {
}

// NewWeaveCommandsDeleteOK creates WeaveCommandsDeleteOK with default headers values
func NewWeaveCommandsDeleteOK() *WeaveCommandsDeleteOK {
	return &WeaveCommandsDeleteOK{}
}

// WriteResponse to the client
func (o *WeaveCommandsDeleteOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}
