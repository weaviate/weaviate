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
	"net/netip"
	"sync"
	"sync/atomic"

	raftImpl "github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// transport resolves the target of every outbound RAFT RPC itself, so it can refuse targets it knows are wrong, and
// refuses responses another server sent, which raft would count as the target's vote or ack.
type transport struct {
	types.RaftTransport
	resolver *raft
	log      logrus.FieldLogger
	local    raftImpl.ServerAddress // LocalAddr, which formats the listener address on every call
	// fallbacks holds the peers dialed at their stored address, so each episode is logged once.
	fallbacks sync.Map
}

// EncodePeer encodes the address the resolver gives id, else addr, as NetworkTransport did with the resolver as its
// address provider: this node advertises its configured address as written, which joins compare against.
func (t *transport) EncodePeer(id raftImpl.ServerID, addr raftImpl.ServerAddress) []byte {
	if resolved, err := t.resolver.ServerAddr(id); err == nil {
		return []byte(resolved)
	}
	return []byte(addr)
}

// sameAddr compares RAFT addresses by IP and port, so differently written forms of one IP (FD00::B, fd00::b,
// ::ffff:10.0.0.5) match. Anything else, such as a host name, compares as written.
func sameAddr(a, b raftImpl.ServerAddress) bool {
	if a == b {
		return true
	}
	pa, errA := netip.ParseAddrPort(string(a))
	pb, errB := netip.ParseAddrPort(string(b))
	return errA == nil && errB == nil && pa.Addr().Unmap() == pb.Addr().Unmap() && pa.Port() == pb.Port()
}

// dial returns id's memberlist address, else target, the address stored for id in the RAFT configuration. It refuses
// this node's own address: raft never sends itself an RPC, so id is down and this node took over its old IP. It refuses
// a target memberlist assigns to another node: that node would answer, and be counted as a vote or ack, in id's place.
func (t *transport) dial(id raftImpl.ServerID, target raftImpl.ServerAddress) (raftImpl.ServerAddress, error) {
	addr, err := t.resolver.ServerAddr(id)
	if err != nil {
		if holder := t.resolver.memberAt(target, id); holder != "" {
			return "", fmt.Errorf("raft address %s of %s now belongs to %s", target, id, holder)
		}
		if _, logged := t.fallbacks.LoadOrStore(id, struct{}{}); !logged {
			t.log.WithFields(logrus.Fields{"id": id, "fallback": target}).
				Warnf("unable to get address for server, using fallback address until memberlist resolves it: %v", err)
		}
		addr = target
	} else {
		t.fallbacks.Delete(id)
	}
	if sameAddr(addr, t.local) {
		return "", fmt.Errorf("raft address %s of %s is this node's own", addr, id)
	}
	return addr, nil
}

// fromPeer refuses a response another server sent in id's place: raft would count it as id's vote or ack. InstallSnapshot
// and TimeoutNow responses carry no ID, so dialUnverified guards those before sending.
func fromPeer(h raftImpl.RPCHeader, id raftImpl.ServerID) error {
	if len(h.ID) > 0 && raftImpl.ServerID(h.ID) != id {
		return fmt.Errorf("raft response for %s came from %s", id, h.ID)
	}
	return nil
}

// AppendEntriesPipeline opens a pipeline only to an address memberlist confirms and that is not this node's own: a
// pipeline never re-resolves, so on any other address raft replicates with sync RPCs, each dialed anew.
func (t *transport) AppendEntriesPipeline(id raftImpl.ServerID, target raftImpl.ServerAddress) (raftImpl.AppendPipeline, error) {
	addr, err := t.resolver.ServerAddr(id)
	if err != nil || sameAddr(addr, t.local) {
		return nil, raftImpl.ErrPipelineReplicationNotSupported
	}
	p, err := t.RaftTransport.AppendEntriesPipeline(id, addr)
	if err != nil {
		return nil, err
	}
	return newPipeline(p, t.resolver, id, addr, t.log), nil
}

// pipeline fails once memberlist drops or moves id, or another server answers, so raft stops using it. Otherwise a node
// holding id's old address keeps acking in id's place and the pipeline never fails.
type pipeline struct {
	raftImpl.AppendPipeline
	resolver  *raft
	id        raftImpl.ServerID
	addr      raftImpl.ServerAddress
	consumer  chan raftImpl.AppendFuture
	done      chan struct{}
	closeOnce sync.Once
	wrongPeer atomic.Bool
}

func newPipeline(inner raftImpl.AppendPipeline, resolver *raft, id raftImpl.ServerID, addr raftImpl.ServerAddress,
	log logrus.FieldLogger,
) *pipeline {
	p := &pipeline{
		AppendPipeline: inner,
		resolver:       resolver,
		id:             id,
		addr:           addr,
		consumer:       make(chan raftImpl.AppendFuture, cap(inner.Consumer())),
		done:           make(chan struct{}),
	}
	enterrors.GoWrapper(p.forward, log)
	return p
}

// forward hands raft only the responses id sent. It drops any other, and the pipeline's next send fails.
func (p *pipeline) forward() {
	in := p.AppendPipeline.Consumer()
	for {
		select {
		case f := <-in:
			if resp := f.Response(); resp != nil && fromPeer(resp.RPCHeader, p.id) != nil {
				p.wrongPeer.Store(true)
				continue
			}
			select {
			case p.consumer <- f:
			case <-p.done:
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *pipeline) Consumer() <-chan raftImpl.AppendFuture { return p.consumer }

func (p *pipeline) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	return p.AppendPipeline.Close()
}

func (p *pipeline) AppendEntries(args *raftImpl.AppendEntriesRequest, resp *raftImpl.AppendEntriesResponse) (raftImpl.AppendFuture, error) {
	if p.wrongPeer.Load() {
		return nil, fmt.Errorf("raft pipeline to %s at %s is answered by another server", p.id, p.addr)
	}
	addr, err := p.resolver.ServerAddr(p.id)
	if err != nil {
		return nil, fmt.Errorf("memberlist no longer lists %s, closing pipeline to %s", p.id, p.addr)
	}
	if addr != p.addr {
		return nil, fmt.Errorf("raft address of %s moved from %s to %s", p.id, p.addr, addr)
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
	if err := t.RaftTransport.AppendEntries(id, addr, args, resp); err != nil {
		return err
	}
	return fromPeer(resp.RPCHeader, id)
}

func (t *transport) RequestVote(id raftImpl.ServerID, target raftImpl.ServerAddress,
	args *raftImpl.RequestVoteRequest, resp *raftImpl.RequestVoteResponse,
) error {
	addr, err := t.dial(id, target)
	if err != nil {
		return err
	}
	if err := t.RaftTransport.RequestVote(id, addr, args, resp); err != nil {
		return err
	}
	return fromPeer(resp.RPCHeader, id)
}

func (t *transport) RequestPreVote(id raftImpl.ServerID, target raftImpl.ServerAddress,
	args *raftImpl.RequestPreVoteRequest, resp *raftImpl.RequestPreVoteResponse,
) error {
	addr, err := t.dial(id, target)
	if err != nil {
		return err
	}
	if err := t.RaftTransport.RequestPreVote(id, addr, args, resp); err != nil {
		return err
	}
	return fromPeer(resp.RPCHeader, id)
}

// dialUnverified dials an RPC whose response carries no ID: it also refuses an address memberlist gives another member,
// since a snapshot or leadership transfer that reached that member would roll back its state or start its election.
func (t *transport) dialUnverified(id raftImpl.ServerID, target raftImpl.ServerAddress) (raftImpl.ServerAddress, error) {
	addr, err := t.dial(id, target)
	if err != nil {
		return "", err
	}
	if holder := t.resolver.memberAt(addr, id); holder != "" {
		return "", fmt.Errorf("raft address %s of %s is also %s's", addr, id, holder)
	}
	return addr, nil
}

func (t *transport) InstallSnapshot(id raftImpl.ServerID, target raftImpl.ServerAddress,
	args *raftImpl.InstallSnapshotRequest, resp *raftImpl.InstallSnapshotResponse, data io.Reader,
) error {
	addr, err := t.dialUnverified(id, target)
	if err != nil {
		return err
	}
	return t.RaftTransport.InstallSnapshot(id, addr, args, resp, data)
}

func (t *transport) TimeoutNow(id raftImpl.ServerID, target raftImpl.ServerAddress,
	args *raftImpl.TimeoutNowRequest, resp *raftImpl.TimeoutNowResponse,
) error {
	addr, err := t.dialUnverified(id, target)
	if err != nil {
		return err
	}
	return t.RaftTransport.TimeoutNow(id, addr, args, resp)
}
