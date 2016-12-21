package acl_entries




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveACLEntriesGetOK Successful response

swagger:response weaveAclEntriesGetOK
*/
type WeaveACLEntriesGetOK struct {

	// In: body
	Payload *models.ACLEntry `json:"body,omitempty"`
}

// NewWeaveACLEntriesGetOK creates WeaveACLEntriesGetOK with default headers values
func NewWeaveACLEntriesGetOK() *WeaveACLEntriesGetOK {
	return &WeaveACLEntriesGetOK{}
}

// WithPayload adds the payload to the weave Acl entries get o k response
func (o *WeaveACLEntriesGetOK) WithPayload(payload *models.ACLEntry) *WeaveACLEntriesGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave Acl entries get o k response
func (o *WeaveACLEntriesGetOK) SetPayload(payload *models.ACLEntry) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveACLEntriesGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
