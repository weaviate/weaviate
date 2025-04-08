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

package rbac

import (
	"encoding/json"
	"fmt"
	"io"
)

type Snapshot struct {
	Policy         [][]string `json:"roles_policies"`
	GroupingPolicy [][]string `json:"grouping_policies"`
}

func (s *Manager) SnapShot() (*Snapshot, error) {
	if s.storage == nil {
		return nil, nil
	}
	policy, err := s.storage.GetPolicy()
	if err != nil {
		return nil, err
	}
	groupingPolicy, err := s.storage.GetGroupingPolicy()
	if err != nil {
		return nil, err
	}
	return &Snapshot{Policy: policy, GroupingPolicy: groupingPolicy}, nil
}

func (s *Manager) Restore(r io.Reader) error {
	if s.storage == nil {
		return nil
	}
	snapshot := Snapshot{}
	if err := json.NewDecoder(r).Decode(&snapshot); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %w", err)
	}
	if s.storage == nil {
		return nil
	}
	//TODO : migration has to be done here if needed
	_, err := s.storage.AddPolicies(snapshot.Policy)
	if err != nil {
		return fmt.Errorf("add policies: %w", err)
	}

	//TODO : migration has to be done here if needed
	_, err = s.storage.AddGroupingPolicies(snapshot.GroupingPolicy)
	if err != nil {
		return fmt.Errorf("add grouping policies: %w", err)
	}
	return s.storage.LoadPolicy()
}
