//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package crossref

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// RefSource is an abstraction of the source of a cross-ref. The opposite would
// be Ref which represents the target insted. A RefSource is specified in a URI
// format in the API. When this type is used it is safe to assume that a Ref is
// semantically valid. This guarantuee would not be possible on the URI format,
// as the URI can be well-formed, but not contain the data we expect in it.  Do
// not use directly, such as crossref.RefSource{}, as you won't have any
// guarantees in this case. Always use one of the parsing options or New()
type RefSource struct {
	Local    bool
	PeerName string
	Property schema.PropertyName
	Class    schema.ClassName
	TargetID strfmt.UUID
	Kind     kind.Kind
}

// ParseSource is a safe way to generate a RefSource, as it will error if any
// of the input parameters are not as expected.
func ParseSource(uriString string) (*RefSource, error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, fmt.Errorf("invalid cref URI: %s", err)
	}

	pathSegments := strings.Split(uri.Path, "/")
	if len(pathSegments) != 5 {
		return nil, fmt.Errorf(
			"invalid cref URI: must use long-form: path must be of format '/{things,actions}/<className>/<uuid>/<propertyName>', but got '%s'",
			uri.Path)
	}

	if ok := strfmt.IsUUID(pathSegments[3]); !ok {
		return nil, fmt.Errorf("invalid cref URI: 2nd path segment must be uuid, but got '%s'",
			pathSegments[3])
	}

	k, err := parseKind(pathSegments[1])
	if err != nil {
		return nil, fmt.Errorf("invalid cref URI: %s", err)
	}

	class := pathSegments[2]
	if class == "" {
		return nil, fmt.Errorf("className cannot be empty")
	}

	if unicode.IsLower(rune(class[0])) {
		return nil, fmt.Errorf("className must start with an uppercase letter, but got %s", class)
	}

	property := pathSegments[4]
	if property == "" {
		return nil, fmt.Errorf("property cannot be empty")
	}

	return &RefSource{
		Local:    (uri.Host == "localhost"),
		PeerName: uri.Host,
		TargetID: strfmt.UUID(pathSegments[3]),
		Kind:     k,
		Class:    schema.ClassName(class),
		Property: schema.PropertyName(property),
	}, nil
}

func (r *RefSource) String() string {
	uri := url.URL{
		Host:   r.PeerName,
		Scheme: "weaviate",
		Path:   fmt.Sprintf("/%s/%s/%s/%s", pluralizeKindName(r.Kind), r.Class, r.TargetID, r.Property),
	}

	return uri.String()
}

// func pluralizeKindName(k kind.Kind) string {
// 	return strings.ToLower(k.Name()) + "s"
// }

// // SingleRef converts the parsed Ref back into the API helper construct
// // containing a stringified representation (URI format) of the Ref
// func (r *Ref) SingleRef() *models.SingleRef {
// 	return &models.SingleRef{
// 		NrDollarCref: strfmt.URI(r.String()),
// 	}
// }
