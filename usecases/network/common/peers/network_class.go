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

package peers

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
)

// HasClass verifies whether both the peer and the referenced
// class exist in the network. If not it tries to fail with a meaningful
// error
func (p Peers) HasClass(classRef crossrefs.NetworkClass) (bool, error) {
	peer, err := p.ByName(classRef.PeerName)
	if err != nil {
		return false, fmt.Errorf("class '%s' does not exist: %s", classRef.String(), err)
	}

	class := peer.Schema.FindClassByName(schema.ClassName(classRef.ClassName))
	if class == nil {
		return false, fmt.Errorf("class '%s' does not exist: peer '%s' has no class '%s'",
			classRef.String(), classRef.PeerName, classRef.ClassName)
	}

	return true, nil
}
