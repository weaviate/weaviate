//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package resolver

import (
	"fmt"
	"io"

	raftImpl "github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/types"
)

// transport resolves the target of every outbound RAFT RPC itself, so it can refuse targets it knows are wrong.
type transport struct {
	types.RaftTransport
	resolver *raft
	log      logrus.FieldLogger
}

// dial returns id's memberlist address, else target, the address stored for id in the RAFT configuration. It refuses
// this node's own address: raft never sends itself an RPC, so id is down and this node took over its old IP.
func (t *transport) dial(id raftImpl.ServerID, target raftImpl.ServerAddress) (raftImpl.ServerAddress, error) {
	addr, err := t.resolver.ServerAddr(id)
	if err != nil {
		t.log.WithFields(logrus.Fields{"id": id, "fallback": target}).
			Warnf("unable to get address for server, using fallback address: %v", err)
		addr = target
	}
	if addr == t.LocalAddr() {
		return "", fmt.Errorf("raft address %s of %s is this node's own", addr, id)
	}
	return addr, nil
}

// AppendEntriesPipeline opens a pipeline only to an address memberlist confirms and that is not this node's own: a
// pipeline never re-resolves, so on any other address raft replicates with sync RPCs, each dialed anew.
func (t *transport) AppendEntriesPipeline(id raftImpl.ServerID, target raftImpl.ServerAddress) (raftImpl.AppendPipeline, error) {
	addr, err := t.resolver.ServerAddr(id)
	if err != nil || addr == t.LocalAddr() {
		return nil, raftImpl.ErrPipelineReplicationNotSupported
	}
	p, err := t.RaftTransport.AppendEntriesPipeline(id, addr)
	if err != nil {
		return nil, err
	}
	return &pipeline{AppendPipeline: p, resolver: t.resolver, id: id, addr: addr}, nil
}

// pipeline fails once memberlist drops or moves id, so raft stops using it. Otherwise a node holding id's old address
// keeps acking in id's place and the pipeline never fails.
type pipeline struct {
	raftImpl.AppendPipeline
	resolver *raft
	id       raftImpl.ServerID
	addr     raftImpl.ServerAddress
}

func (p *pipeline) AppendEntries(args *raftImpl.AppendEntriesRequest, resp *raftImpl.AppendEntriesResponse) (raftImpl.AppendFuture, error) {
	if addr, err := p.resolver.ServerAddr(p.id); err != nil || addr != p.addr {
		return nil, fmt.Errorf("raft address of %s moved from %s", p.id, p.addr)
	}
	return p.AppendPipeline.AppendEntries(args, resp)
}

func (t *transport) AppendEntries(id raftImpl.ServerID, target raftImpl.ServerAddress,
	args *raftImpl.AppendEntriesRequest, resp *raftImpl.AppendEntriesResponse,
) error {
	addr, err := t.dial(id, target)
	if err != nil {
		return err
	}
	return t.RaftTransport.AppendEntries(id, addr, args, resp)
}

func (t *transport) RequestVote(id raftImpl.ServerID, target raftImpl.ServerAddress,
	args *raftImpl.RequestVoteRequest, resp *raftImpl.RequestVoteResponse,
) error {
	addr, err := t.dial(id, target)
	if err != nil {
		return err
	}
	return t.RaftTransport.RequestVote(id, addr, args, resp)
}

func (t *transport) RequestPreVote(id raftImpl.ServerID, target raftImpl.ServerAddress,
	args *raftImpl.RequestPreVoteRequest, resp *raftImpl.RequestPreVoteResponse,
) error {
	addr, err := t.dial(id, target)
	if err != nil {
		return err
	}
	return t.RaftTransport.RequestPreVote(id, addr, args, resp)
}

func (t *transport) InstallSnapshot(id raftImpl.ServerID, target raftImpl.ServerAddress,
	args *raftImpl.InstallSnapshotRequest, resp *raftImpl.InstallSnapshotResponse, data io.Reader,
) error {
	addr, err := t.dial(id, target)
	if err != nil {
		return err
	}
	return t.RaftTransport.InstallSnapshot(id, addr, args, resp, data)
}

func (t *transport) TimeoutNow(id raftImpl.ServerID, target raftImpl.ServerAddress,
	args *raftImpl.TimeoutNowRequest, resp *raftImpl.TimeoutNowResponse,
) error {
	addr, err := t.dial(id, target)
	if err != nil {
		return err
	}
	return t.RaftTransport.TimeoutNow(id, addr, args, resp)
}
