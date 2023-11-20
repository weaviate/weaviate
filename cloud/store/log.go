//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"fmt"
	"log"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type rLog struct {
	*raftboltdb.BoltStore
}

func (l rLog) LastAppliedCommand() (uint64, error) {
	first, err := l.FirstIndex()
	if err != nil {
		return 0, fmt.Errorf("first index: %w", err)
	}
	last, err := l.LastIndex()
	if err != nil {
		return 0, fmt.Errorf("last index: %w", err)
	}
	if last == 0 {
		return 0, nil
	}
	var rLog raft.Log
	for ; last >= first; last-- {
		if err := l.GetLog(last, &rLog); err != nil {
			return 0, fmt.Errorf("get log at index: %w", err)
		}
		if rLog.Type == raft.LogCommand {
			return last, nil
		}
	}
	return 0, nil
}

func (l rLog) Apply(snapIdx uint64, f func(l *raft.Log) interface{}) (uint64, error) {
	first, err := l.FirstIndex()
	if err != nil {
		return 0, fmt.Errorf("first index: %w", err)
	}
	last, err := l.LastIndex()
	if err != nil {
		return 0, fmt.Errorf("last index: %w", err)
	}
	if last == 0 {
		return 0, err
	}

	var lastAppliedIndex uint64
	var rl raft.Log
	for ; first < last; first++ {
		if err := l.GetLog(first, &rl); err != nil {
			return 0, fmt.Errorf("get log at index: %w", err)
		}
		log.Printf("%v> %+v", first, rl)
		if rl.Type != raft.LogCommand {
			fmt.Println(rl.Type)
			continue
		}
		if rl.Index > snapIdx {
			f(&rl)
		}
		lastAppliedIndex = first

	}
	return lastAppliedIndex, nil
}
