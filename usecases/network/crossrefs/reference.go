//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package crossrefs

import (
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// NetworkClass references one class in a remote peer
// Can be used against the network to verify both peer and class
// actually exist.
type NetworkClass struct {
	PeerName  string
	ClassName string
}

// NetworkKind is one particular kind (i.e. thing or action) of a peer
// identified by its UUID
type NetworkKind struct {
	Kind     kind.Kind
	PeerName string
	ID       strfmt.UUID
}

// ParseClass into a NetworkClass
func ParseClass(name string) (NetworkClass, error) {
	result := NetworkClass{}
	if !schema.ValidNetworkClassName(name) {
		return result, fmt.Errorf(
			"%s is not a valid Network class, must match <peerName>/<className>", name)
	}

	parts := strings.Split(name, "/")
	// No danger of nil-pointer derefs, because we passed validation before
	result.PeerName = parts[0]
	result.ClassName = parts[1]
	return result, nil
}

func (n NetworkClass) String() string {
	return fmt.Sprintf("%s/%s", n.PeerName, n.ClassName)
}
