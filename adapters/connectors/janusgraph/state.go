//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package janusgraph

import (
	"context"
	"encoding/json"
	"fmt"
)

// Called by a connector when it has updated it's internal state that needs to be shared across all connectors in other Weaviate instances.
func (j *Janusgraph) SetState(ctx context.Context, state json.RawMessage) {
	err := json.Unmarshal(state, &j.state)

	// Extra assertions if the connector has been initialized already.
	if j.initialized {
		if err != nil {
			panic(fmt.Sprintf("Could not deserialize a schema update, after Weaviate was initialized. Are you running multiple versions of Weaviate in the same cluster? Reason: %s", err.Error()))
		}
		if j.state.Version != SCHEMA_VERSION {
			panic(fmt.Sprintf("Received a schema update of version %v. We can only handle schema version %v. Are you running multiple versions of Weaviate in the same cluster?", j.state.Version, SCHEMA_VERSION))
		}
	} else {
		if err != nil {
			panic(fmt.Sprintf("Received an illegal JSON document as the connector state during initialization. Cannot recover from this. Error: %v", err))
		}
	}
}

func (j *Janusgraph) UpdateStateInStateManager(ctx context.Context) {
	rawState, err := json.Marshal(j.state)

	if err != nil {
		panic("Could not serialize internal state to json")
	}

	j.stateManager.SetState(ctx, rawState)
}
