package acl_entries


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveACLEntriesInsertOK Successful response

swagger:response weaveAclEntriesInsertOK
*/
type WeaveACLEntriesInsertOK struct {

	// In: body
	Payload *models.ACLEntry `json:"body,omitempty"`
}

// NewWeaveACLEntriesInsertOK creates WeaveACLEntriesInsertOK with default headers values
func NewWeaveACLEntriesInsertOK() *WeaveACLEntriesInsertOK {
	return &WeaveACLEntriesInsertOK{}
}

// WithPayload adds the payload to the weave Acl entries insert o k response
func (o *WeaveACLEntriesInsertOK) WithPayload(payload *models.ACLEntry) *WeaveACLEntriesInsertOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave Acl entries insert o k response
func (o *WeaveACLEntriesInsertOK) SetPayload(payload *models.ACLEntry) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveACLEntriesInsertOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
