//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package schema

import (
	"fmt"
	"time"

	schemaclient "github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
)

func download(peer peers.Peer) (schema.Schema, error) {
	peerClient, err := peer.CreateClient()
	if err != nil {
		return schema.Schema{}, fmt.Errorf(
			"could not create client for %s: %s", peer.Name, err)
	}

	params := &schemaclient.SchemaDumpParams{}
	params.WithTimeout(2 * time.Second)
	ok, err := peerClient.Schema.SchemaDump(params, nil)
	if err != nil {
		return schema.Schema{}, fmt.Errorf(
			"could not download schema from %s: %s", peer.Name, err)
	}

	return schema.Schema{
		Things:  ok.Payload.Things,
		Actions: ok.Payload.Actions,
	}, nil
}
