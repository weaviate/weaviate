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
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

type conflictDelegate struct {
	logger  logrus.FieldLogger
	localID string
	raft    *raft.Raft
}

func (d *conflictDelegate) SetRaft(raft *raft.Raft) {
	d.raft = raft
}

// NotifyConflict is invoked when a name conflict is detected
func (d *conflictDelegate) NotifyConflict(existing, other *memberlist.Node) {
	if d.raft == nil {
		return
	}

	if existing.Name == d.localID {
		d.logger.WithFields(logrus.Fields{
			"name":        existing.Name,
			"existing_ip": existing.Address(),
			"new_ip":      other.Address(),
		}).Warn("node conflicting IPs, i will shutdown ...")

		// we force panic here for immediate stop of the node to avoid any raft replication.
		panic("forced panic because of ip conflicts")
	}

	_, leaderID := d.raft.LeaderWithID()
	if d.localID != string(leaderID) {
		d.logger.WithFields(logrus.Fields{
			"name":        existing.Name,
			"existing_ip": existing.Address(),
			"new_ip":      other.Address(),
		}).Warn("there is ip conflicting in memberlist but i am not the leader")
		return
	}

	if err := d.raft.RemoveServer(raft.ServerID(existing.Addr), 0, 0).Error(); err != nil {
		d.logger.Error(err)
	}
	if err := d.raft.AddVoter(raft.ServerID(other.Name), raft.ServerAddress(other.Addr), 0, 0).Error(); err != nil {
		d.logger.Error(err)
	}
}
