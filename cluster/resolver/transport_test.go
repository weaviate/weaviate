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

// newRaftNode starts a RAFT node; a non-empty bootstrapAddr bootstraps it as the single voter stored at that address.
func newRaftNode(t *testing.T, id string, trans raftImpl.Transport, bootstrapAddr raftImpl.ServerAddress) *raftImpl.Raft {
	t.Helper()
	cfg := raftImpl.DefaultConfig()
	cfg.LocalID = raftImpl.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: id, Output: io.Discard})

	logs, snaps := raftImpl.NewInmemStore(), raftImpl.NewInmemSnapshotStore()
	if bootstrapAddr != "" {
		require.NoError(t, raftImpl.BootstrapCluster(cfg, logs, logs, snaps, trans, raftImpl.Configuration{
			Servers: []raftImpl.Server{{Suffrage: raftImpl.Voter, ID: cfg.LocalID, Address: bootstrapAddr}},
		}))
	}
	r, err := raftImpl.NewRaft(cfg, &raftImpl.MockFSM{}, logs, logs, snaps, trans)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown().Error() })
	return r
}

// startLeader starts L on a transport from res listening on 127.0.0.1:port (0 picks one), stored at bootstrapAddr or,
// if empty, at the address it listens on.
func startLeader(t *testing.T, res *raft, port int, bootstrapAddr raftImpl.ServerAddress) (*raftImpl.Raft, raftImpl.ServerAddress) {
	t.Helper()
	tl, err := res.NewTCPTransport(net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), nil, 3, 2*time.Second, logrus.New())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tl.Close() })
	res.LocalAddress = string(tl.LocalAddr())
	if bootstrapAddr == "" {
		bootstrapAddr = tl.LocalAddr()
	}
	leader := newRaftNode(t, "L", tl, bootstrapAddr)
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

	// memberlist drops the stale entry, then F1 rejoins under a new IP.
	members.set("F1", "")
	time.Sleep(300 * time.Millisecond)
	f1 := newRaftNode(t, "F1", t1, "")
	members.set("F1", "::1")
	require.NoError(t, leader.AddNonvoter("F1", t1.LocalAddr(), 0, time.Second).Error())
	applyN(t, leader, 5)

	requireCaughtUp(t, leader, f1, "F1")
	requireCaughtUp(t, leader, f2, "F2")
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

// TestStaleFallbackAddressHeldByUnknownNode covers a holder the leader's memberlist does not know (a failed or slow
// join, or another cluster's node).
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
