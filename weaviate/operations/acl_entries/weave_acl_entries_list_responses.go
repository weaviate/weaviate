package acl_entries


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveACLEntriesListOK Successful response

swagger:response weaveAclEntriesListOK
*/
type WeaveACLEntriesListOK struct {

	// In: body
	Payload *models.ACLEntriesListResponse `json:"body,omitempty"`
}

// NewWeaveACLEntriesListOK creates WeaveACLEntriesListOK with default headers values
func NewWeaveACLEntriesListOK() *WeaveACLEntriesListOK {
	return &WeaveACLEntriesListOK{}
}

// WithPayload adds the payload to the weave Acl entries list o k response
func (o *WeaveACLEntriesListOK) WithPayload(payload *models.ACLEntriesListResponse) *WeaveACLEntriesListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave Acl entries list o k response
func (o *WeaveACLEntriesListOK) SetPayload(payload *models.ACLEntriesListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveACLEntriesListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
