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

package cluster

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
)

// _OpCode represents the type of supported operation
type _OpCode uint8

const (
	// _ProtoVersion internal protocol version for exchanging messages
	_ProtoVersion uint8 = 1
	// _OpCodeDisk operation code for getting disk space
	_OpCodeDisk _OpCode = 1
	// _ProtoTTL used to decide when to update the cache
	_ProtoTTL = time.Second * 8
)

// spaceMsg is used to notify other nodes about current disk usage
type spaceMsg struct {
	header
	DiskUsage
	NodeLen uint8  // = len(Node) is required to marshal Node
	Node    string // node space
}

// header of an operation
type header struct {
	// OpCode operation code
	OpCode _OpCode
	// ProtoVersion protocol we will speak
	ProtoVersion uint8
}

// DiskUsage contains total and available space in B
type DiskUsage struct {
	// Total disk space
	Total uint64
	// Total available space
	Available uint64
}

// NodeInfo disk space
type NodeInfo struct {
	DiskUsage
	LastTimeMilli int64 // last update time in milliseconds
}

func (d *spaceMsg) marshal() (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, 24+len(d.Node)))
	if err := binary.Write(buf, binary.BigEndian, d.header); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, d.DiskUsage); err != nil {
		return nil, err
	}
	// code node name starting by its length
	if err := buf.WriteByte(d.NodeLen); err != nil {
		return nil, err
	}
	_, err = buf.Write([]byte(d.Node))
	return buf.Bytes(), err
}

func (d *spaceMsg) unmarshal(data []byte) (err error) {
	rd := bytes.NewReader(data)
	if err = binary.Read(rd, binary.BigEndian, &d.header); err != nil {
		return err
	}
	if err = binary.Read(rd, binary.BigEndian, &d.DiskUsage); err != nil {
		return err
	}

	// decode node name start by its length
	if d.NodeLen, err = rd.ReadByte(); err != nil {
		return err
	}
	begin := len(data) - rd.Len()
	end := begin + int(d.NodeLen)
	// make sure this version is backward compatible
	if _ProtoVersion <= 1 && begin+int(d.NodeLen) != len(data) {
		begin-- // since previous version doesn't encode the length
		end = len(data)
		d.NodeLen = uint8(end - begin)
	}
	d.Node = string(data[begin:end])
	return nil
}

// diskSpaceCache is a thread-safe cache of node disk space information.
// It replaces the old memberlist delegate cache.
type diskSpaceCache struct {
	sync.RWMutex
	data map[string]NodeInfo
}

func newDiskSpaceCache() *diskSpaceCache {
	return &diskSpaceCache{
		data: make(map[string]NodeInfo, 32),
	}
}

// get returns info about a specific node in the cluster
func (c *diskSpaceCache) get(node string) (NodeInfo, bool) {
	c.RLock()
	defer c.RUnlock()
	x, ok := c.data[node]
	return x, ok
}

func (c *diskSpaceCache) set(node string, info NodeInfo) {
	c.Lock()
	defer c.Unlock()
	c.data[node] = info
}

// delete removes a node from the cache
func (c *diskSpaceCache) delete(node string) {
	c.Lock()
	defer c.Unlock()
	delete(c.data, node)
}

// sortCandidates sorts passed node names by the free amount of disk space in descending order.
//
// Two nodes are considered equivalent if the difference between their
// free spaces is less than 32MB.
// The free space is just a rough estimate of the actual amount.
// The lower bound 32MB helps to mitigate the risk of selecting same set of nodes
// when selections happen concurrently on different initiator nodes.
func (c *diskSpaceCache) sortCandidates(names []string) []string {
	rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })

	c.RLock()
	defer c.RUnlock()
	m := c.data
	sort.Slice(names, func(i, j int) bool {
		return (m[names[j]].Available >> 25) < (m[names[i]].Available >> 25)
	})

	return names
}

// runSerfEventLoop processes Serf membership and user events, updating the disk space cache.
// It runs until eventCh is closed (on Serf shutdown).
func runSerfEventLoop(eventCh <-chan serf.Event, cache *diskSpaceCache, log logrus.FieldLogger) {
	for event := range eventCh {
		switch e := event.(type) {
		case serf.MemberEvent:
			if e.EventType() == serf.EventMemberLeave || e.EventType() == serf.EventMemberFailed {
				for _, m := range e.Members {
					cache.delete(m.Name)
				}
			}
		case serf.UserEvent:
			if e.Name == "disk_space" {
				var msg spaceMsg
				if err := msg.unmarshal(e.Payload); err == nil && msg.Node != "" {
					cache.set(msg.Node, NodeInfo{msg.DiskUsage, time.Now().UnixMilli()})
				}
			}
		}
	}
}

// runDiskSpaceUpdater periodically measures local disk usage and broadcasts it
// to other cluster nodes via a Serf user event. It also updates the local cache entry.
func runDiskSpaceUpdater(serfNode *serf.Serf, nodeName string, dataPath string,
	cache *diskSpaceCache, log logrus.FieldLogger,
) {
	t := time.NewTicker(_ProtoTTL)
	defer t.Stop()
	for range t.C {
		space, err := diskSpace(dataPath)
		if err != nil {
			log.WithField("action", "disk_space_updater").Error(err)
			continue
		}
		cache.set(nodeName, NodeInfo{space, time.Now().UnixMilli()})
		msg := spaceMsg{
			header{_OpCodeDisk, _ProtoVersion},
			space,
			uint8(len(nodeName)),
			nodeName,
		}
		payload, err := msg.marshal()
		if err != nil {
			log.WithField("action", "disk_space_updater.marshal").Error(err)
			continue
		}
		if err := serfNode.UserEvent("disk_space", payload, false); err != nil {
			log.WithField("action", "disk_space_updater.broadcast").Error(err)
		}
	}
}
