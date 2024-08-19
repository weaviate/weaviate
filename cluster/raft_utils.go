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

package cluster

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/cluster/types"
)

// leaderErr decorates ErrLeaderNotFound by distinguishing between
// normal election happening and there is no leader been chosen yet
// and if it can't reach the other nodes either for intercluster
// communication issues or other nodes were down.
func (s *Raft) leaderErr() error {
	if s.store.raftResolver != nil && len(s.store.raftResolver.NotResolvedNodes()) > 0 {
		var nodes []string
		for n := range s.store.raftResolver.NotResolvedNodes() {
			nodes = append(nodes, string(n))
		}

		return fmt.Errorf("%w, can not resolve nodes [%s]", types.ErrLeaderNotFound, strings.Join(nodes, ","))
	}
	return types.ErrLeaderNotFound
}
