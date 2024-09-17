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
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
)

func (st *Store) LastAppliedCommand() (uint64, error) {
	if st.logStore == nil {
		return 0, fmt.Errorf("log store can't be nil")
	}

	first, err := st.logStore.FirstIndex()
	if err != nil {
		return 0, fmt.Errorf("first index: %w", err)
	}
	last, err := st.logStore.LastIndex()
	if err != nil {
		return 0, fmt.Errorf("last index: %w", err)
	}
	if last == 0 {
		return 0, nil
	}
	var rLog raft.Log
	for ; last >= first; last-- {
		err := st.logStore.GetLog(last, &rLog)
		if err != nil && !errors.Is(err, raft.ErrLogNotFound) {
			return 0, fmt.Errorf("get log at index %d: %w", last, err)
		}
		if rLog.Type == raft.LogCommand {
			return last, nil
		}
	}
	return 0, nil
}
