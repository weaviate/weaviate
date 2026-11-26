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
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	raftImpl "github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/types"
)

// fakeMemberlist maps member names to IPs; a missing name is not in memberlist.
type fakeMemberlist struct {
	ipv6NodeSelector
	mu  sync.Mutex
	ips map[string]string
}

func (f *fakeMemberlist) set(name, ip string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ip == "" {
		delete(f.ips, name)
		return
	}
	f.ips[name] = ip
}

func (f *fakeMemberlist) NodeAddress(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ips[id]
}

func (f *fakeMemberlist) AllNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.ips))
	for name := range f.ips {
		names = append(names, name)
	}
	return names
}

// startRaft starts a RAFT node with fast timeouts, bootstrapped with servers if there are any.
func startRaft(t *testing.T, id string, trans raftImpl.Transport, servers []raftImpl.Server, opts ...func(*raftImpl.Config)) *raftImpl.Raft {
	t.Helper()
	cfg := raftImpl.DefaultConfig()
	cfg.LocalID = raftImpl.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: id, Output: io.Discard})
	for _, opt := range opts {
		opt(cfg)
	}

	logs, snaps := raftImpl.NewInmemStore(), raftImpl.NewInmemSnapshotStore()
	if len(servers) > 0 {
		require.NoError(t, raftImpl.BootstrapCluster(cfg, logs, logs, snaps, trans, raftImpl.Configuration{Servers: servers}))
	}
	r, err := raftImpl.NewRaft(cfg, &raftImpl.MockFSM{}, logs, logs, snaps, trans)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown().Error() })
	return r
}

// newRaftNode starts a RAFT node; a non-empty bootstrapAddr bootstraps it as the single voter stored at that address.
func newRaftNode(t *testing.T, id string, trans raftImpl.Transport, bootstrapAddr raftImpl.ServerAddress, opts ...func(*raftImpl.Config)) *raftImpl.Raft {
	t.Helper()
	var servers []raftImpl.Server
	if bootstrapAddr != "" {
		servers = []raftImpl.Server{{Suffrage: raftImpl.Voter, ID: raftImpl.ServerID(id), Address: bootstrapAddr}}
	}
	return startRaft(t, id, trans, servers, opts...)
}

// leaderTransport returns L's transport from res, listening on 127.0.0.1:port (0 picks one).
func leaderTransport(t *testing.T, res *raft, port int) types.RaftTransport {
	t.Helper()
	tl, err := res.NewTCPTransport(net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), nil, 3, 2*time.Second, logrus.New())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tl.Close() })
	res.LocalAddress = string(tl.LocalAddr())
	return tl
}

// startLeader starts L on a transport from res listening on 127.0.0.1:port (0 picks one), stored at bootstrapAddr or,
// if empty, at the address it listens on.
func startLeader(t *testing.T, res *raft, port int, bootstrapAddr raftImpl.ServerAddress, opts ...func(*raftImpl.Config)) (*raftImpl.Raft, raftImpl.ServerAddress) {
	t.Helper()
	tl := leaderTransport(t, res, port)
	if bootstrapAddr == "" {
		bootstrapAddr = tl.LocalAddr()
	}
	leader := newRaftNode(t, "L", tl, bootstrapAddr, opts...)
	require.Eventually(t, func() bool { return leader.State() == raftImpl.Leader }, 5*time.Second, 10*time.Millisecond)
	return leader, tl.LocalAddr()
}

// newTransport returns a follower's transport listening on ip:port (port 0 picks one).
func newTransport(t *testing.T, ip string, port int) raftImpl.Transport {
	t.Helper()
	trans, err := raftImpl.NewTCPTransport(net.JoinHostPort(ip, strconv.Itoa(port)), nil, 3, 2*time.Second, io.Discard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = trans.Close() })
	return trans
}

func portOf(t *testing.T, addr raftImpl.ServerAddress) int {
	t.Helper()
	tcp, err := net.ResolveTCPAddr("tcp", string(addr))
	require.NoError(t, err)
	return tcp.Port
}

func deadAddress(t *testing.T) raftImpl.ServerAddress {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return raftImpl.ServerAddress(addr)
}

// sharedPort returns a port that is free on both IPv4 and IPv6 loopback, so two nodes can share one RAFT port.
func sharedPort(t *testing.T) int {
	t.Helper()
	for i := 0; i < 20; i++ {
		l4, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		port := l4.Addr().(*net.TCPAddr).Port
		l6, err := net.Listen("tcp", net.JoinHostPort("::1", fmt.Sprint(port)))
		_ = l4.Close()
		if err == nil {
			_ = l6.Close()
			return port
		}
	}
	t.Skip("IPv6 loopback unavailable")
	return 0
}

func applyN(t *testing.T, r *raftImpl.Raft, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		require.NoError(t, r.Apply([]byte(fmt.Sprintf("entry-%d", i)), time.Second).Error())
	}
}

func requireCaughtUp(t *testing.T, leader, follower *raftImpl.Raft, name string) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		return follower.AppliedIndex() >= leader.LastIndex()
	}, 5*time.Second, 20*time.Millisecond, "%s never caught up: applied=%d leader last=%d",
		name, follower.AppliedIndex(), leader.LastIndex())
}

// TestStaleFallbackAddressHeldByAnotherNode reproduces 0-weaviate-issues#707: the leader cannot resolve F1, the
// address stored for F1 now belongs to F2, and F1 must still catch up once it rejoins under a new address.
func TestStaleFallbackAddressHeldByAnotherNode(t *testing.T) {
	t1, t2 := newTransport(t, "127.0.0.1", 0), newTransport(t, "127.0.0.1", 0)
	members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1"}}
	res := NewRaft(RaftConfig{
		ClusterStateReader: members,
		IsLocalHost:        true,
		NodeNameToPortMap:  map[string]int{"F1": portOf(t, t1.LocalAddr()), "F2": portOf(t, t2.LocalAddr())},
		LocalName:          "L",
	})
	leader, _ := startLeader(t, res, 0, "")

	// F2's stored address is its stale old one; memberlist resolves it to where it listens now.
	f2 := newRaftNode(t, "F2", t2, "")
	members.set("F2", "127.0.0.1")
	require.NoError(t, leader.AddNonvoter("F2", deadAddress(t), 0, time.Second).Error())
	applyN(t, leader, 5)
	requireCaughtUp(t, leader, f2, "F2")

	// F1 is unknown to memberlist and its stored address is F2's: the old IP was reused.
	require.NoError(t, leader.AddNonvoter("F1", t2.LocalAddr(), 0, time.Second).Error())
	time.Sleep(300 * time.Millisecond)

	// F1 rejoins under its own address, as the join path does.
	f1 := newRaftNode(t, "F1", t1, "")
	members.set("F1", "127.0.0.1")
	require.NoError(t, leader.AddNonvoter("F1", t1.LocalAddr(), 0, time.Second).Error())
	applyN(t, leader, 5)

	requireCaughtUp(t, leader, f1, "F1")
	requireCaughtUp(t, leader, f2, "F2")
}

// TestStaleMemberlistAddressHeldByAnotherNode covers the variant without a fallback: memberlist still lists dead F1
// at the IP F2 now holds.
func TestStaleMemberlistAddressHeldByAnotherNode(t *testing.T) {
	port := sharedPort(t)
	t2, t1 := newTransport(t, "127.0.0.1", port), newTransport(t, "::1", port)
	members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1"}}
	res := NewRaft(RaftConfig{ClusterStateReader: members, RaftPort: port, LocalName: "L"})
	leader, _ := startLeader(t, res, 0, "")

	// F2 took F1's old IP while memberlist still lists F1 there.
	f2 := newRaftNode(t, "F2", t2, "")
	members.set("F2", "127.0.0.1")
	members.set("F1", "127.0.0.1")
	require.NoError(t, leader.AddNonvoter("F2", deadAddress(t), 0, time.Second).Error())
	require.NoError(t, leader.AddNonvoter("F1", deadAddress(t), 0, time.Second).Error())
	applyN(t, leader, 5)
	time.Sleep(300 * time.Millisecond)

	// memberlist drops the stale entry and lists F1 again only once it caught up, so the pipeline that reached F2 must
	// fail on the drop alone; F1 rejoins under a new IP.
	members.set("F1", "")
	time.Sleep(300 * time.Millisecond)
	f1 := newRaftNode(t, "F1", t1, "")
	require.NoError(t, leader.AddNonvoter("F1", t1.LocalAddr(), 0, time.Second).Error())
	applyN(t, leader, 5)

	requireCaughtUp(t, leader, f1, "F1")
	members.set("F1", "::1")
	applyN(t, leader, 5)
	requireCaughtUp(t, leader, f1, "F1")
	requireCaughtUp(t, leader, f2, "F2")
}

// TestFallbackToLiveStoredAddress covers a peer memberlist does not list that still listens at its stored address,
// such as a node whose memberlist join failed: the leader must reach it there.
func TestFallbackToLiveStoredAddress(t *testing.T) {
	t1 := newTransport(t, "127.0.0.1", 0)
	members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1"}}
	res := NewRaft(RaftConfig{ClusterStateReader: members, IsLocalHost: true, NodeNameToPortMap: map[string]int{}, LocalName: "L"})
	leader, _ := startLeader(t, res, 0, "")

	f1 := newRaftNode(t, "F1", t1, "")
	require.NoError(t, leader.AddNonvoter("F1", t1.LocalAddr(), 0, time.Second).Error())
	applyN(t, leader, 5)
	requireCaughtUp(t, leader, f1, "F1")
}

// TestStaleAddressHeldByLeader covers the leader restarting on the old IP of a follower that is still down: the
// fallback, or a memberlist entry not yet removed, makes the leader dial itself, and a leader steps down on any
// AppendEntries of its own term.
func TestStaleAddressHeldByLeader(t *testing.T) {
	for _, viaMemberlist := range []bool{false, true} {
		t.Run(fmt.Sprintf("via memberlist=%v", viaMemberlist), func(t *testing.T) {
			port := portOf(t, deadAddress(t))
			members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1"}}
			res := NewRaft(RaftConfig{ClusterStateReader: members, RaftPort: port, LocalName: "L"})

			// L is stored under its old address and now listens on F1's.
			leader, addr := startLeader(t, res, port, deadAddress(t))
			stored := addr
			if viaMemberlist {
				members.set("F1", "127.0.0.1")
				stored = deadAddress(t)
			}
			require.NoError(t, leader.AddNonvoter("F1", stored, 0, time.Second).Error())

			// Any step-down bumps the term at the next election, which takes ~50ms here.
			term := leader.CurrentTerm()
			time.Sleep(time.Second)
			require.Equal(t, term, leader.CurrentTerm(), "leader stepped down and re-elected itself")
			require.Equal(t, raftImpl.Leader, leader.State())
		})
	}
}

// TestStaleFallbackAddressHeldByUnknownNode covers a holder the leader's memberlist does not know, such as a node whose
// memberlist join failed or is slow.
func TestStaleFallbackAddressHeldByUnknownNode(t *testing.T) {
	t1, tx := newTransport(t, "127.0.0.1", 0), newTransport(t, "127.0.0.1", 0)
	members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1"}}
	res := NewRaft(RaftConfig{
		ClusterStateReader: members,
		IsLocalHost:        true,
		NodeNameToPortMap:  map[string]int{"F1": portOf(t, t1.LocalAddr())},
		LocalName:          "L",
	})
	leader, _ := startLeader(t, res, 0, "")
	applyN(t, leader, 5)

	// A node outside memberlist listens on the address stored for F1.
	newRaftNode(t, "X", tx, "")
	require.NoError(t, leader.AddNonvoter("F1", tx.LocalAddr(), 0, time.Second).Error())
	time.Sleep(300 * time.Millisecond)

	f1 := newRaftNode(t, "F1", t1, "")
	members.set("F1", "127.0.0.1")
	require.NoError(t, leader.AddNonvoter("F1", t1.LocalAddr(), 0, time.Second).Error())
	applyN(t, leader, 5)

	requireCaughtUp(t, leader, f1, "F1")
}

// TestStaleMemberlistAddressHeldByUnknownNode covers both windows at once: memberlist still lists dead F1 at an
// address now held by a node memberlist does not know.
func TestStaleMemberlistAddressHeldByUnknownNode(t *testing.T) {
	port := sharedPort(t)
	tx, t1 := newTransport(t, "127.0.0.1", port), newTransport(t, "::1", port)
	members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1"}}
	res := NewRaft(RaftConfig{ClusterStateReader: members, RaftPort: port, LocalName: "L"})
	leader, _ := startLeader(t, res, 0, "")
	applyN(t, leader, 5)

	newRaftNode(t, "X", tx, "")
	members.set("F1", "127.0.0.1")
	require.NoError(t, leader.AddNonvoter("F1", deadAddress(t), 0, time.Second).Error())
	time.Sleep(300 * time.Millisecond)

	// memberlist learns F1's new IP before F1's RAFT join updates its address, as in a real rejoin.
	f1 := newRaftNode(t, "F1", t1, "")
	members.set("F1", "::1")
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, leader.AddNonvoter("F1", t1.LocalAddr(), 0, time.Second).Error())
	applyN(t, leader, 5)

	requireCaughtUp(t, leader, f1, "F1")
}

// TestStaleAddressDoesNotFakeQuorum covers five voters where F1 is down and another node answers at the address the
// leader dials for F1. With F3 and F4 also down, only L and F2 are alive, two of five, so L must not commit. If the other
// node's answers counted as F1's, L would see three of five and commit entries that only two voters hold.
func TestStaleAddressDoesNotFakeQuorum(t *testing.T) {
	tests := []struct {
		name       string
		storedF1   string // whose address the RAFT configuration stores for F1: F2's, X's (a node outside memberlist), or dead
		listF1AtF2 bool   // memberlist still lists F1, at the address F2 took
	}{
		{name: "stored address taken by another member", storedF1: "F2"},
		{name: "memberlist lists the peer at another member's address", storedF1: "dead", listF1AtF2: true},
		{name: "stored address taken by a node outside memberlist", storedF1: "X"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t2, t3, t4, tx := newTransport(t, "127.0.0.1", 0), newTransport(t, "127.0.0.1", 0),
				newTransport(t, "127.0.0.1", 0), newTransport(t, "127.0.0.1", 0)
			newRaftNode(t, "X", tx, "")
			f1Port := portOf(t, deadAddress(t))
			if tc.listF1AtF2 {
				f1Port = portOf(t, t2.LocalAddr())
			}
			members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1"}}
			res := NewRaft(RaftConfig{
				ClusterStateReader: members,
				IsLocalHost:        true,
				NodeNameToPortMap: map[string]int{
					"F1": f1Port, "F2": portOf(t, t2.LocalAddr()),
					"F3": portOf(t, t3.LocalAddr()), "F4": portOf(t, t4.LocalAddr()),
				},
				LocalName: "L",
			})
			leader, _ := startLeader(t, res, 0, "")

			// F2 is stored under its old address; memberlist resolves it to where it listens now.
			followers := map[string]*raftImpl.Raft{}
			for id, tr := range map[string]raftImpl.Transport{"F2": t2, "F3": t3, "F4": t4} {
				followers[id] = newRaftNode(t, id, tr, "")
				members.set(id, "127.0.0.1")
				stored := tr.LocalAddr()
				if id == "F2" {
					stored = deadAddress(t)
				}
				require.NoError(t, leader.AddVoter(raftImpl.ServerID(id), stored, 0, time.Second).Error())
			}
			if tc.listF1AtF2 {
				members.set("F1", "127.0.0.1")
			}
			stored := map[string]raftImpl.ServerAddress{"F2": t2.LocalAddr(), "X": tx.LocalAddr(), "dead": deadAddress(t)}
			require.NoError(t, leader.AddVoter("F1", stored[tc.storedF1], 0, time.Second).Error())
			applyN(t, leader, 3)

			require.NoError(t, followers["F3"].Shutdown().Error())
			require.NoError(t, followers["F4"].Shutdown().Error())

			err := leader.Apply([]byte("needs a real majority"), 3*time.Second).Error()
			require.Errorf(t, err, "committed with only L and F2 alive (state %s): another node was counted as F1", leader.State())
		})
	}
}

// TestStaleAddressDoesNotFakeQuorumWithNonVoter covers three voters and a non-voter: memberlist still lists down F1
// at the non-voter's address. With F2 also down, L is the only live voter and must not commit.
func TestStaleAddressDoesNotFakeQuorumWithNonVoter(t *testing.T) {
	t2, tn := newTransport(t, "127.0.0.1", 0), newTransport(t, "127.0.0.1", 0)
	members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1", "F2": "127.0.0.1", "N": "127.0.0.1"}}
	res := NewRaft(RaftConfig{
		ClusterStateReader: members,
		IsLocalHost:        true,
		NodeNameToPortMap: map[string]int{
			"F1": portOf(t, tn.LocalAddr()), "F2": portOf(t, t2.LocalAddr()), "N": portOf(t, tn.LocalAddr()),
		},
		LocalName: "L",
	})
	leader, _ := startLeader(t, res, 0, "")

	f2 := newRaftNode(t, "F2", t2, "")
	newRaftNode(t, "N", tn, "")
	require.NoError(t, leader.AddVoter("F2", t2.LocalAddr(), 0, time.Second).Error())
	require.NoError(t, leader.AddNonvoter("N", tn.LocalAddr(), 0, time.Second).Error())
	members.set("F1", "127.0.0.1")
	require.NoError(t, leader.AddVoter("F1", deadAddress(t), 0, time.Second).Error())
	applyN(t, leader, 3)

	require.NoError(t, f2.Shutdown().Error())
	err := leader.Apply([]byte("needs a real majority"), 3*time.Second).Error()
	require.Errorf(t, err, "committed with L as the only live voter (state %s): N was counted as F1", leader.State())
}

// TestElectionResolvesVoters covers L starting an election among three voters stored at dead addresses: it must win
// through the addresses memberlist resolves, and must not win with a non-voter's answer counted as a down voter's vote.
func TestElectionResolvesVoters(t *testing.T) {
	tests := []struct {
		name       string
		votersUp   bool // F1 and F2 run at the addresses memberlist resolves
		nonVoterF1 bool // memberlist still lists down F1 at N's address, and F2 is down too
		wantLeader bool
	}{
		{name: "voters resolved through memberlist", votersUp: true, wantLeader: true},
		{name: "non-voter answers at a down voter's address", nonVoterF1: true, wantLeader: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t1, t2, tn := newTransport(t, "127.0.0.1", 0), newTransport(t, "127.0.0.1", 0), newTransport(t, "127.0.0.1", 0)
			members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1"}}
			ports := map[string]int{"F1": portOf(t, t1.LocalAddr()), "F2": portOf(t, t2.LocalAddr())}
			if tc.votersUp {
				startRaft(t, "F1", t1, nil)
				startRaft(t, "F2", t2, nil)
				members.set("F1", "127.0.0.1")
				members.set("F2", "127.0.0.1")
			}
			if tc.nonVoterF1 {
				startRaft(t, "N", tn, nil)
				ports["F1"] = portOf(t, tn.LocalAddr())
				members.set("F1", "127.0.0.1")
			}
			res := NewRaft(RaftConfig{ClusterStateReader: members, IsLocalHost: true, NodeNameToPortMap: ports, LocalName: "L"})
			tl := leaderTransport(t, res, 0)
			l := startRaft(t, "L", tl, []raftImpl.Server{
				{Suffrage: raftImpl.Voter, ID: "L", Address: tl.LocalAddr()},
				{Suffrage: raftImpl.Voter, ID: "F1", Address: deadAddress(t)},
				{Suffrage: raftImpl.Voter, ID: "F2", Address: deadAddress(t)},
			})

			isLeader := func() bool { return l.State() == raftImpl.Leader }
			if tc.wantLeader {
				require.Eventually(t, isLeader, 5*time.Second, 10*time.Millisecond, "L never won its election")
			} else {
				require.Never(t, isLeader, 2*time.Second, 10*time.Millisecond, "L won with N's vote counted as F1's")
			}
		})
	}
}

// TestSnapshotReachesPeerThroughMemberlist covers a peer stored at a dead address that needs a snapshot, because the
// leader compacted its log: the snapshot must go to the address memberlist resolves.
func TestSnapshotReachesPeerThroughMemberlist(t *testing.T) {
	t1 := newTransport(t, "127.0.0.1", 0)
	members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1", "F1": "127.0.0.1"}}
	res := NewRaft(RaftConfig{
		ClusterStateReader: members, IsLocalHost: true,
		NodeNameToPortMap: map[string]int{"F1": portOf(t, t1.LocalAddr())}, LocalName: "L",
	})
	leader, _ := startLeader(t, res, 0, "", func(c *raftImpl.Config) { c.TrailingLogs = 1 })
	applyN(t, leader, 20)
	require.NoError(t, leader.Snapshot().Error())

	f1 := newRaftNode(t, "F1", t1, "")
	require.NoError(t, leader.AddNonvoter("F1", deadAddress(t), 0, time.Second).Error())
	requireCaughtUp(t, leader, f1, "F1")
	require.NotEqual(t, "0", f1.Stats()["last_snapshot_index"], "F1 caught up without the snapshot")
}

// TestLeadershipTransferReachesPeerThroughMemberlist covers a transfer to a voter stored at a dead address: TimeoutNow
// must go to the address memberlist resolves.
func TestLeadershipTransferReachesPeerThroughMemberlist(t *testing.T) {
	t1 := newTransport(t, "127.0.0.1", 0)
	members := &fakeMemberlist{ips: map[string]string{"L": "127.0.0.1", "F1": "127.0.0.1"}}
	res := NewRaft(RaftConfig{
		ClusterStateReader: members, IsLocalHost: true,
		NodeNameToPortMap: map[string]int{"F1": portOf(t, t1.LocalAddr())}, LocalName: "L",
	})
	leader, _ := startLeader(t, res, 0, "")
	f1 := newRaftNode(t, "F1", t1, "")
	stored := deadAddress(t)
	require.NoError(t, leader.AddVoter("F1", stored, 0, time.Second).Error())
	applyN(t, leader, 5)
	requireCaughtUp(t, leader, f1, "F1")

	require.NoError(t, leader.LeadershipTransferToServer("F1", stored).Error())
	require.Eventually(t, func() bool { return f1.State() == raftImpl.Leader }, 5*time.Second, 10*time.Millisecond)
}

// TestEncodePeerKeepsResolverAddress covers the address this node advertises in RPC headers: the configured one as
// written, which a restarted leader's join compares against its stored entry.
func TestEncodePeerKeepsResolverAddress(t *testing.T) {
	members := &fakeMemberlist{ips: map[string]string{"F1": "10.0.0.5"}}
	res := NewRaft(RaftConfig{ClusterStateReader: members, RaftPort: 8300, LocalName: "L"})
	tr := leaderTransport(t, res, 0)
	res.LocalAddress = "[FD00::B]:8300"

	require.Equal(t, "[FD00::B]:8300", string(tr.EncodePeer("L", tr.LocalAddr())))
	require.Equal(t, "10.0.0.5:8300", string(tr.EncodePeer("F1", "10.0.0.9:8300")))
	require.Equal(t, "10.0.0.9:8300", string(tr.EncodePeer("F2", "10.0.0.9:8300")))
}

func TestSameAddr(t *testing.T) {
	tests := []struct {
		a, b raftImpl.ServerAddress
		same bool
	}{
		{"10.0.0.5:8300", "10.0.0.5:8300", true},
		{"[FD00::B]:8300", "[fd00::b]:8300", true},
		{"[0:0:0:0:0:0:0:1]:8300", "[::1]:8300", true},
		{"[::ffff:10.0.0.5]:8300", "10.0.0.5:8300", true},
		{"10.0.0.5:8300", "10.0.0.5:8301", false},
		{"10.0.0.5:8300", "10.0.0.6:8300", false},
		{"weaviate-0:8300", "weaviate-0:8300", true},
		{"weaviate-0:8300", "10.0.0.5:8300", false},
	}
	for _, tc := range tests {
		t.Run(string(tc.a)+"="+string(tc.b), func(t *testing.T) {
			require.Equal(t, tc.same, sameAddr(tc.a, tc.b))
		})
	}
}

// fakePipeline answers every AppendEntries at once, as responder.
type fakePipeline struct {
	responder string
	ch        chan raftImpl.AppendFuture
}

func (f *fakePipeline) AppendEntries(args *raftImpl.AppendEntriesRequest, resp *raftImpl.AppendEntriesResponse) (raftImpl.AppendFuture, error) {
	resp.ID = []byte(f.responder)
	resp.Success = true
	fut := &fakeFuture{req: args, resp: resp}
	f.ch <- fut
	return fut, nil
}
func (f *fakePipeline) Consumer() <-chan raftImpl.AppendFuture { return f.ch }
func (f *fakePipeline) Close() error                           { return nil }

type fakeFuture struct {
	req  *raftImpl.AppendEntriesRequest
	resp *raftImpl.AppendEntriesResponse
}

func (f *fakeFuture) Error() error                              { return nil }
func (f *fakeFuture) Start() time.Time                          { return time.Time{} }
func (f *fakeFuture) Request() *raftImpl.AppendEntriesRequest   { return f.req }
func (f *fakeFuture) Response() *raftImpl.AppendEntriesResponse { return f.resp }

// TestPipelineOnlyPassesResponsesFromItsPeer covers a pipeline that memberlist confirmed but another server answers:
// raft must never see that response, and the pipeline must fail so raft falls back to RPCs it checks one by one.
func TestPipelineOnlyPassesResponsesFromItsPeer(t *testing.T) {
	tests := []struct {
		responder string
		passed    bool
	}{
		{responder: "F1", passed: true},
		{responder: "", passed: true}, // no ID to check
		{responder: "F2", passed: false},
	}
	for _, tc := range tests {
		t.Run("responder="+tc.responder, func(t *testing.T) {
			members := &fakeMemberlist{ips: map[string]string{"F1": "127.0.0.1"}}
			res := NewRaft(RaftConfig{ClusterStateReader: members, RaftPort: 8300, LocalName: "L"})
			addr, err := res.ServerAddr("F1")
			require.NoError(t, err)
			p := newPipeline(&fakePipeline{responder: tc.responder, ch: make(chan raftImpl.AppendFuture, 4)},
				res, "F1", addr, logrus.New())
			t.Cleanup(func() { _ = p.Close() })

			_, err = p.AppendEntries(&raftImpl.AppendEntriesRequest{}, &raftImpl.AppendEntriesResponse{})
			require.NoError(t, err)
			select {
			case <-p.Consumer():
				require.True(t, tc.passed, "raft received a response %s sent for F1", tc.responder)
			case <-time.After(200 * time.Millisecond):
				require.False(t, tc.passed, "raft never received F1's response")
			}

			_, err = p.AppendEntries(&raftImpl.AppendEntriesRequest{}, &raftImpl.AppendEntriesResponse{})
			if tc.passed {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "the pipeline kept sending after another server answered")
			}
		})
	}
}
