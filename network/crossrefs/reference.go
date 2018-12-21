package crossrefs

import (
	"fmt"
	"regexp"
	"strings"
)

// NetworkClass references one class in a remote peer
// Can be used against the network to verify both peer and class
// actually exist.
type NetworkClass struct {
	PeerName  string
	ClassName string
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
