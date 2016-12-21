package devices


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

/*WeaveDevicesUpdateParentOK Successful response

swagger:response weaveDevicesUpdateParentOK
*/
type WeaveDevicesUpdateParentOK struct {
}

// NewWeaveDevicesUpdateParentOK creates WeaveDevicesUpdateParentOK with default headers values
func NewWeaveDevicesUpdateParentOK() *WeaveDevicesUpdateParentOK {
	return &WeaveDevicesUpdateParentOK{}
}

// WriteResponse to the client
func (o *WeaveDevicesUpdateParentOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}
