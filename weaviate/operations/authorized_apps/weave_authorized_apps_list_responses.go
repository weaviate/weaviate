package authorized_apps


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveAuthorizedAppsListOK Successful response

swagger:response weaveAuthorizedAppsListOK
*/
type WeaveAuthorizedAppsListOK struct {

	// In: body
	Payload *models.AuthorizedAppsListResponse `json:"body,omitempty"`
}

// NewWeaveAuthorizedAppsListOK creates WeaveAuthorizedAppsListOK with default headers values
func NewWeaveAuthorizedAppsListOK() *WeaveAuthorizedAppsListOK {
	return &WeaveAuthorizedAppsListOK{}
}

// WithPayload adds the payload to the weave authorized apps list o k response
func (o *WeaveAuthorizedAppsListOK) WithPayload(payload *models.AuthorizedAppsListResponse) *WeaveAuthorizedAppsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave authorized apps list o k response
func (o *WeaveAuthorizedAppsListOK) SetPayload(payload *models.AuthorizedAppsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveAuthorizedAppsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
