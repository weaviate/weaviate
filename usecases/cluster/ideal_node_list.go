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
	"sort"
	"strings"
	"sync"
	"time"
)

type IdealClusterState struct {
	memberNames  []string
	currentState MemberLister
	lock         sync.Mutex
}

func NewIdealClusterState(s MemberLister) *IdealClusterState {
	ics := &IdealClusterState{currentState: s}
	go ics.startPolling()
	return ics
}

// Validate returns an error if the actual state does not match the assumed
// ideal state, e.g. because a node has died, or left unexpectedly.
func (ics *IdealClusterState) Validate() error {
	ics.lock.Lock()
	defer ics.lock.Unlock()

	actual := map[string]struct{}{}
	for _, name := range ics.currentState.AllNames() {
		actual[name] = struct{}{}
	}

	var missing []string
	for _, name := range ics.memberNames {
		if _, ok := actual[name]; !ok {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("node(s) %s unhealthy or unavailable",
			strings.Join(missing, ", "))
	}

	return nil
}

func (ics *IdealClusterState) Members() []string {
	ics.lock.Lock()
	defer ics.lock.Unlock()

	return ics.memberNames
}

func (ics *IdealClusterState) startPolling() {
	t := time.NewTicker(1 * time.Second)
	for {
		<-t.C
		current := ics.currentState.AllNames()
		ics.extendList(current)
	}
}

func (ics *IdealClusterState) extendList(current []string) {
	ics.lock.Lock()
	defer ics.lock.Unlock()

	var unknown []string
	known := map[string]struct{}{}
	for _, name := range ics.memberNames {
		known[name] = struct{}{}
	}

	for _, name := range current {
		if _, ok := known[name]; !ok {
			unknown = append(unknown, name)
		}
	}

	ics.memberNames = append(ics.memberNames, unknown...)
	sort.Strings(ics.memberNames)
}
