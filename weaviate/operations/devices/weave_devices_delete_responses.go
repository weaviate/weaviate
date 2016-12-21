package devices


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

/*WeaveDevicesDeleteOK Successful response

swagger:response weaveDevicesDeleteOK
*/
type WeaveDevicesDeleteOK struct {
}

// NewWeaveDevicesDeleteOK creates WeaveDevicesDeleteOK with default headers values
func NewWeaveDevicesDeleteOK() *WeaveDevicesDeleteOK {
	return &WeaveDevicesDeleteOK{}
}

// WriteResponse to the client
func (o *WeaveDevicesDeleteOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}
