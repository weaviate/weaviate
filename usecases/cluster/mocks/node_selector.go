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

package mocks

import "sort"

type memberlist struct {
	// nodes include the node names only
	nodes []string
}

func (m memberlist) StorageCandidates() []string {
	sort.Strings(m.nodes)
	return m.nodes
}

func (m memberlist) NonStorageNodes() []string {
	return []string{}
}

func (m memberlist) SortCandidates(nodes []string) []string {
	sort.Strings(nodes)
	return nodes
}

func (m memberlist) NodeHostname(name string) (string, bool) {
	for _, node := range m.nodes {
		if node == name {
			return name, true
		}
	}
	return "", false
}

func (m memberlist) LocalName() string {
	if len(m.nodes) == 0 {
		return ""
	}

	return m.nodes[0]
}

func (m memberlist) AllHostnames() []string {
	return m.nodes
}

func (m memberlist) NodeAddress(name string) string {
	for _, node := range m.nodes {
		if node == name {
			return name
		}
	}
	return ""
}

func NewMockNodeSelector(node ...string) memberlist {
	return memberlist{nodes: node}
}
