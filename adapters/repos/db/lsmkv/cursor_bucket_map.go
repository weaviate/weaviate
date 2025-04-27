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

package lsmkv

import (
	"context"
	"encoding/json"
)

type CursorMap struct {


	listCfg      MapListOptionConfig
	keyOnly      bool
	realCursor   *StreamingCursor
}

type cursorStateMap struct {
	key   []byte
	value []MapPair
	err   error
}

type innerCursorMap interface {
	first() ([]byte, []MapPair, error)
	next() ([]byte, []MapPair, error)
	seek([]byte) ([]byte, []MapPair, error)
}

func (b *Bucket) MapCursor(cfgs ...MapListOption) *CursorMap {
	b.flushLock.RLock()

	c := MapListOptionConfig{}
	for _, cfg := range cfgs {
		cfg(&c)
	}

	return &CursorMap{
		realCursor: NewStreamingCursor(context.Background(), b.dir, false),
		listCfg:      c,
	}
}

func (b *Bucket) MapCursorKeyOnly(cfgs ...MapListOption) *CursorMap {
	c := b.MapCursor(cfgs...)
	c.keyOnly = true
	return c
}

func (c *CursorMap) Seek(ctx context.Context, key []byte) ([]byte, []MapPair) {
	key, val :=  c.realCursor.Seek( key)
	out := []MapPair{}
	json.Unmarshal(val, &out)
	return key, out

}

func (c *CursorMap) Next(ctx context.Context) ([]byte, []MapPair) {
	// before := time.Now()
	// defer func() {
	// 	fmt.Printf("-- total next took %s\n", time.Since(before))
	// }()
	key, val := c.realCursor.Next()
	out := []MapPair{}
	json.Unmarshal(val, &out)
	return key, out

}

func (c *CursorMap) First(ctx context.Context) ([]byte, []MapPair) {
	key, val := c.realCursor.First()
	out := []MapPair{}
	json.Unmarshal(val, &out)
	return key, out
}

func (c *CursorMap) Close() {
	c.realCursor.Close()
}
