//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package crossref

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/schema"
)

// RefSource is an abstraction of the source of a cross-ref. The opposite would
// be Ref which represents the target instead. A RefSource is specified in a URI
// format in the API. When this type is used it is safe to assume that a Ref is
// semantically valid. This guarantee would not be possible on the URI format,
// as the URI can be well-formed, but not contain the data we expect in it.  Do
// not use directly, such as crossref.RefSource{}, as you won't have any
// guarantees in this case. Always use one of the parsing options or New()
type RefSource struct {
	Local    bool                `json:"local"`
	PeerName string              `json:"peerName"`
	Property schema.PropertyName `json:"property"`
	Class    schema.ClassName    `json:"class"`
	TargetID strfmt.UUID         `json:"targetID"`
}

func NewSource(className schema.ClassName,
	property schema.PropertyName, id strfmt.UUID,
) *RefSource {
	return &RefSource{
		Local:    true,
		PeerName: "localhost",
		Class:    className,
		TargetID: id,
		Property: property,
	}
}

// ParseSource is a safe way to generate a RefSource, as it will error if any
// of the input parameters are not as expected.
func ParseSource(uriString string) (*RefSource, error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, fmt.Errorf("invalid cref URI: %s", err)
	}

	pathSegments := strings.Split(uri.Path, "/")
	if len(pathSegments) != 4 {
		return nil, fmt.Errorf(
			"invalid cref URI: must use long-form: path must be of format '/<className>/<uuid>/<propertyName>', but got '%s'",
			uri.Path)
	}

	if ok := strfmt.IsUUID(pathSegments[2]); !ok {
		return nil, fmt.Errorf("invalid cref URI: 2nd path segment must be uuid, but got '%s'",
			pathSegments[3])
	}

	class := pathSegments[1]
	if class == "" {
		return nil, fmt.Errorf("className cannot be empty")
	}

	if unicode.IsLower(rune(class[0])) {
		return nil, fmt.Errorf("className must start with an uppercase letter, but got %s", class)
	}

	property := pathSegments[3]
	if property == "" {
		return nil, fmt.Errorf("property cannot be empty")
	}

	return &RefSource{
		Local:    (uri.Host == "localhost"),
		PeerName: uri.Host,
		TargetID: strfmt.UUID(pathSegments[2]),
		Class:    schema.ClassName(class),
		Property: schema.PropertyName(property),
	}, nil
}

func (r *RefSource) String() string {
	uri := url.URL{
		Host:   r.PeerName,
		Scheme: "weaviate",
		Path:   fmt.Sprintf("/%s/%s/%s", r.Class, r.TargetID, r.Property),
	}

	return uri.String()
}
