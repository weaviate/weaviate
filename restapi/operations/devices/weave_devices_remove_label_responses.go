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

/*WeaveDevicesRemoveLabelOK Successful response

swagger:response weaveDevicesRemoveLabelOK
*/
type WeaveDevicesRemoveLabelOK struct {
}

// NewWeaveDevicesRemoveLabelOK creates WeaveDevicesRemoveLabelOK with default headers values
func NewWeaveDevicesRemoveLabelOK() *WeaveDevicesRemoveLabelOK {
	return &WeaveDevicesRemoveLabelOK{}
}

// WriteResponse to the client
func (o *WeaveDevicesRemoveLabelOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}
