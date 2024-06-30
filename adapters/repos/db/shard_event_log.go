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

package db

import (
	"fmt"
	"sync"
	"time"
)

type shardEventLog struct {
	shards map[string]shardEventWrapper
	mu     sync.Mutex
}

type shardEventWrapper struct {
	initialized bool // this is our flag to see if we attempt a second initialization. If yes, we crash and dump all events.
	events      []shardEvent
}

type shardEvent struct {
	name string
	time time.Time
}

func newShardEventLog() *shardEventLog {
	return &shardEventLog{
		shards: make(map[string]shardEventWrapper),
	}
}

var ShardEventLog *shardEventLog

func init() {
	ShardEventLog = newShardEventLog()
}

func (l *shardEventLog) BeginLoad(shardName string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	shard, ok := l.shards[shardName]
	if !ok {
		shard = shardEventWrapper{}
	}

	if shard.initialized {
		// conflict: we are trying to load a shard that is already initialized
		// first add current event, then dump all events
		shard.events = append(shard.events, shardEvent{
			name: "BEGIN_LOAD",
			time: time.Now(),
		})
		l.shards[shardName] = shard

		l.dumpEvents(shardName)
		return
	}

	shard.initialized = true
	shard.events = append(shard.events, shardEvent{
		name: "BEGIN_LOAD",
		time: time.Now(),
	})

	l.shards[shardName] = shard
}

func (l *shardEventLog) dumpEvents(shardName string) {
	fmt.Printf("DUMP_EVENTS shard=%q\n", shardName)
	shard, ok := l.shards[shardName]
	if !ok {
		fmt.Printf("no events\n")
		return
	}

	for i, event := range shard.events {
		fmt.Printf("index=%05d event=%q time=%q\n", i, event.name, event.time)
	}
	fmt.Printf("END_DUMP_EVENTS\n")
}

func (l *shardEventLog) EndLoad(shardName string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	shard, ok := l.shards[shardName]
	if !ok {
		shard = shardEventWrapper{}
	}

	name := "END_LOAD_SUCCESS"
	if err != nil {
		name = "END_LOAD_ERROR"
	}

	shard.events = append(shard.events, shardEvent{
		name: name,
		time: time.Now(),
	})

	l.shards[shardName] = shard
}

func (l *shardEventLog) BeginRemove(shardName string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	shard, ok := l.shards[shardName]
	if !ok {
		shard = shardEventWrapper{}
	}

	shard.events = append(shard.events, shardEvent{
		name: "BEGIN_REMOVE",
		time: time.Now(),
	})

	l.shards[shardName] = shard
}

func (l *shardEventLog) EndRemove(shardName string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	shard, ok := l.shards[shardName]
	if !ok {
		shard = shardEventWrapper{}
	}

	name := "END_REMOVE_SUCCESS"
	if err != nil {
		name = "END_REMOVE_ERROR"
	}

	shard.events = append(shard.events, shardEvent{
		name: name,
		time: time.Now(),
	})
	shard.initialized = false

	l.shards[shardName] = shard
}
