/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

/*WeaveDevicesHandleInvitationOK Successful response

swagger:response weaveDevicesHandleInvitationOK
*/
type WeaveDevicesHandleInvitationOK struct {
}

// NewWeaveDevicesHandleInvitationOK creates WeaveDevicesHandleInvitationOK with default headers values
func NewWeaveDevicesHandleInvitationOK() *WeaveDevicesHandleInvitationOK {
	return &WeaveDevicesHandleInvitationOK{}
}

// WriteResponse to the client
func (o *WeaveDevicesHandleInvitationOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}
