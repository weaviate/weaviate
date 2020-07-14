//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
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
