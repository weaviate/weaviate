/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package crossrefs

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/go-openapi/strfmt"
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

var networkClassRegexp = regexp.MustCompile(`^([A-Za-z]+)+/([A-Z][a-z]+)+$`)

// ValidClassName verifies if the specified class is a valid
// crossReference name. This does not mean the class currently exists
// on the specified instance or that the instance exist, but simply
// that the name is valid.
// Receiving a false could also still mean the class is not network-ref, but
// simply a local-ref.
func ValidClassName(name string) bool {
	return networkClassRegexp.MatchString(name)
}

// ParseClass into a NetworkClass
func ParseClass(name string) (NetworkClass, error) {
	result := NetworkClass{}
	if !ValidClassName(name) {
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
