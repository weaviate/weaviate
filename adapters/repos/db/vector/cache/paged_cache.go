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

package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

const maxCallerId = 20

type genericDistancer[T float32 | uint64 | byte] interface {
	SingleDist(x, y []T) (float32, error)
}

type page[T float32 | byte | uint64] struct {
	sync.RWMutex
	ids            map[uint64]int
	rawVectors     []byte
	dimensions     int
	typeSize       int
	extractElement func([]byte) T
	putElement     func([]byte, T)
	dirty          bool
	vectors        [][]T
}

func newFloat32Page() *page[float32] {
	return &page[float32]{
		typeSize: 4,
		ids:      make(map[uint64]int),
		extractElement: func(raw []byte) float32 {
			return math.Float32frombits(binary.LittleEndian.Uint32(raw))
		},
		putElement: func(raw []byte, value float32) {
			binary.LittleEndian.PutUint32(raw, math.Float32bits(value))
		},
		vectors: make([][]float32, 110),
	}
}

func newUint64Page() *page[uint64] {
	return &page[uint64]{
		typeSize: 8,
		ids:      make(map[uint64]int),
		extractElement: func(raw []byte) uint64 {
			return binary.LittleEndian.Uint64(raw)
		},
		putElement: func(raw []byte, value uint64) {
			binary.LittleEndian.PutUint64(raw, value)
		},
	}
}

func newBytePage() *page[byte] {
	return &page[byte]{
		typeSize: 1,
		ids:      make(map[uint64]int),
		extractElement: func(raw []byte) byte {
			return raw[0]
		},
		putElement: func(raw []byte, value byte) {
			raw[0] = value
		},
	}
}

func (p *page[T]) extractVector(id uint64) ([]T, error) {
	p.RLock()
	pos, found := p.ids[id]
	if !found {
		p.RUnlock()
		return nil, fmt.Errorf("no vector found on page with id: %d", id)
	}
	if p.vectors[pos] != nil {
		p.RUnlock()
		return p.vectors[pos], nil
	}
	p.RUnlock()
	rawPos := pos * p.dimensions * p.typeSize
	result := make([]T, p.dimensions)
	for i := range result {
		result[i] = p.extractElement(p.rawVectors[rawPos+i*p.typeSize:])
	}
	p.Lock()
	p.vectors[pos] = result
	p.Unlock()
	return result, nil
}

func (p *page[T]) add(id uint64, vector []T) {
	p.Lock()
	defer p.Unlock()
	if p.dimensions == 0 {
		p.dimensions = len(vector)
	}
	p.ids[id] = len(p.ids)
	raw := make([]byte, p.dimensions*p.typeSize)
	for i, x := range vector {
		p.putElement(raw[i*p.typeSize:], x)
	}
	p.rawVectors = append(p.rawVectors, raw...)
	p.dirty = true
}

func (p *page[T]) len() int {
	return len(p.ids)
}

func (p *page[T]) load(path string, pageSize int) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	buff := make([]byte, pageSize*(p.typeSize*p.dimensions+8))
	n, err := f.Read(buff)
	if err != io.EOF && err != nil {
		return err
	}
	total := n / (p.typeSize*p.dimensions + 8)
	for i := 0; i < total; i++ {
		p.ids[binary.LittleEndian.Uint64(buff[i*8:])] = i
	}
	p.rawVectors = buff[total*8 : n]
	return nil
}

func (p *page[T]) store(path string) error {
	p.Lock()
	defer p.Unlock()
	if !p.dirty {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	total := p.len()
	buff := make([]byte, total*(p.typeSize*p.dimensions+8))
	for key, pos := range p.ids {
		binary.LittleEndian.PutUint64(buff[pos*8:], key)
	}
	copy(buff[total*8:], p.rawVectors)

	_, err = f.Write(buff)
	if err != io.EOF && err != nil {
		return err
	}
	p.dirty = false
	return nil
}

type PagedCache[T float32 | byte | uint64] struct {
	sync.RWMutex
	shardedLocks      *common.ShardedRWLocks
	pageForId         []int
	pages             []*page[T]
	createPage        func() *page[T]
	temporalVectors   map[uint64][]T
	pageSize          int
	distancer         genericDistancer[T]
	path              string
	dimensions        int
	callerIds         []bool
	pagesUsedByCaller []map[int]bool
	logger            logrus.FieldLogger
}

func NewFloat32PagedCache(path string, pageSize int, distancer distancer.Provider, logger logrus.FieldLogger) *PagedCache[float32] {
	return &PagedCache[float32]{
		shardedLocks:      common.NewDefaultShardedRWLocks(),
		pageForId:         make([]int, 10000000),
		createPage:        newFloat32Page,
		temporalVectors:   make(map[uint64][]float32),
		pageSize:          pageSize,
		distancer:         distancer,
		path:              path,
		callerIds:         make([]bool, maxCallerId),
		pagesUsedByCaller: make([]map[int]bool, maxCallerId),
		logger:            logger,
	}
}

func NewUint64PagedCache(path string, pageSize int) *PagedCache[uint64] {
	return &PagedCache[uint64]{
		shardedLocks:      common.NewDefaultShardedRWLocks(),
		pageForId:         make([]int, 10000000),
		createPage:        newUint64Page,
		temporalVectors:   make(map[uint64][]uint64),
		pageSize:          pageSize,
		callerIds:         make([]bool, maxCallerId),
		pagesUsedByCaller: make([]map[int]bool, maxCallerId),
	}
}

func NewBytePagedCache(path string, pageSize int) *PagedCache[byte] {
	return &PagedCache[byte]{
		shardedLocks:      common.NewDefaultShardedRWLocks(),
		pageForId:         make([]int, 10000000),
		createPage:        newBytePage,
		temporalVectors:   make(map[uint64][]byte),
		pageSize:          pageSize,
		callerIds:         make([]bool, maxCallerId),
		pagesUsedByCaller: make([]map[int]bool, maxCallerId),
	}
}

func (pc *PagedCache[T]) GetFreeCallerId() int {
	pc.Lock()
	defer pc.Unlock()
	for i, bussy := range pc.callerIds {
		if !bussy {
			pc.callerIds[i] = true
			pc.pagesUsedByCaller[i] = make(map[int]bool)
			return i
		}
	}
	return -1
}

func (pc *PagedCache[T]) ReturnCallerId(id int) {
	pc.Lock()
	defer pc.Unlock()
	pc.collectMemoryAfterReturningCallerId(id)
	pc.callerIds[id] = false
	pc.pagesUsedByCaller[id] = nil
}

func (pc *PagedCache[T]) CollectMemory() {
	pc.Lock()
	defer pc.Unlock()
	bussy := make([]bool, len(pc.pages))
	for _, m := range pc.pagesUsedByCaller {
		for i := range m {
			bussy[i] = true
		}
	}
	for i, page := range pc.pages {
		if page == nil || bussy[i] {
			continue
		}
		pc.pages[i].store(pc.fileNameForId(i))
		pc.pages[i] = nil
	}
}

func (pc *PagedCache[T]) collectMemoryAfterReturningCallerId(id int) {
	bussy := make([]bool, len(pc.pages))
	for _, m := range pc.pagesUsedByCaller {
		for i := range m {
			bussy[i] = true
		}
	}
	for i := range pc.pagesUsedByCaller[id] {
		if bussy[i] || pc.pages[i] == nil {
			continue
		}
		pc.pages[i].store(pc.fileNameForId(i))
		pc.pages[i] = nil
	}
}

func (pc *PagedCache[T]) allocatePageAndRecordCaller(ctx context.Context, id uint64, callerId int, asyncLoad bool) (*page[T], int, error) {
	if callerId == -1 {
		return nil, 0, errors.New("invalid callerId")
	}
	pc.RLock()
	if len(pc.pageForId) <= int(id) {
		pc.RUnlock()
		return nil, 0, fmt.Errorf("unknown vector for id: %d", id)
	}
	pc.shardedLocks.RLock(id)
	pageId := pc.pageForId[id]
	pc.shardedLocks.RUnlock(id)

	if err := pc.recordCallerId(callerId, pageId); err != nil {
		return nil, 0, errors.Wrapf(err, "getting vector %d", id)
	}

	if len(pc.pages) <= pageId {
		pc.RUnlock()
		return nil, 0, errors.New("unknown page")
	}
	pc.RUnlock()
	pc.shardedLocks.RLock(id)
	page := pc.pages[pageId]
	pc.shardedLocks.RUnlock(id)

	if page == nil {
		pc.shardedLocks.Lock(id)
		defer pc.shardedLocks.Unlock(id)
		if pc.pages[pageId] != nil {
			return pc.pages[pageId], pageId, nil
		}
		page = pc.createPage()
		page.Lock()
		pc.pages[pageId] = page
		if asyncLoad {
			go func() {
				pc.loadPage(ctx, pageId, page)
			}()
		} else {
			if err := pc.loadPage(ctx, pageId, page); err != nil {
				return nil, 0, errors.Wrap(err, "loading page from Get method")
			}
		}
	}

	return page, pageId, nil
}

func (pc *PagedCache[T]) Get(ctx context.Context, callerId int, id uint64) ([]T, error) {
	pc.RLock()
	vec, found := pc.temporalVectors[id]
	if found {
		pc.RUnlock()
		return vec, nil
	}
	pc.RUnlock()

	page, _, err := pc.allocatePageAndRecordCaller(ctx, id, callerId, false)
	if err != nil {
		return nil, err
	}

	return page.extractVector(id)
}

func (pc *PagedCache[T]) Connect(callerId int, id, closest uint64) {
	page, pageId, err := pc.allocatePageAndRecordCaller(context.Background(), closest, callerId, false)
	if err != nil {
		page = pc.createPage()
		pc.Lock()
		pc.pages = append(pc.pages, page)
		pageId = len(pc.pages) - 1
		pc.Unlock()
		if err := pc.recordCallerId(callerId, pageId); err != nil {
			return
		}
	}

	pc.Lock()
	page.add(id, pc.temporalVectors[id])
	if len(pc.pageForId) <= pageId {
		old := pc.pageForId
		pc.pageForId = make([]int, pageId+pageId/10)
		copy(pc.pageForId, old)
	}
	pc.pageForId[id] = pageId
	delete(pc.temporalVectors, id)

	if page.len() > pc.pageSize {
		pc.splitPageNoLock(pageId, page)
	}
	pc.Unlock()
}

func (pc *PagedCache[T]) MultiGet(ctx context.Context, callerId int, ids []uint64) ([][]T, []error) {
	results := make([][]T, len(ids))
	errs := make([]error, len(ids))
	for i, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, []error{err}
		}
		results[i], errs[i] = pc.Get(ctx, callerId, id)
	}
	return results, errs
}

func (pc *PagedCache[T]) Len() int32 {
	return 0
}

func (pc *PagedCache[T]) CountVectors() int64 {
	return 0
}

func (pc *PagedCache[T]) Delete(ctx context.Context, id uint64) {}

func (pc *PagedCache[T]) Preload(id uint64, vec []T) {
	pc.Lock()
	defer pc.Unlock()
	if pc.dimensions == 0 {
		pc.dimensions = len(vec)
	}
	pc.temporalVectors[id] = vec
}

func (pc *PagedCache[T]) PreloadNoLock(id uint64, vec []T) {
	pc.temporalVectors[id] = vec
}

func (pc *PagedCache[T]) SetSizeAndGrowNoLock(id uint64) {}

func (pc *PagedCache[T]) Prefetch(callerId int, id uint64) {
	pc.allocatePageAndRecordCaller(context.Background(), id, callerId, true)
}

func (pc *PagedCache[T]) Grow(size uint64) {}

func (pc *PagedCache[T]) Drop() {}

func (pc *PagedCache[T]) UpdateMaxSize(size int64) {}

func (pc *PagedCache[T]) CopyMaxSize() int64 {
	return 0
}

func (pc *PagedCache[T]) All() [][]T {
	return nil
}

func (pc *PagedCache[T]) LockAll() {
	pc.shardedLocks.LockAll()
}

func (pc *PagedCache[T]) UnlockAll() {
	pc.shardedLocks.UnlockAll()
}

func (pc *PagedCache[T]) loadPage(ctx context.Context, pageId int, page *page[T]) error {
	err := ctx.Err()
	if err != nil {
		return err
	}
	page.dimensions = pc.dimensions
	page.load(pc.fileNameForId(pageId), pc.pageSize)
	page.Unlock()
	return nil
}

func (pc *PagedCache[T]) recordCallerId(callerId, pageId int) error {
	if len(pc.pagesUsedByCaller) <= callerId {
		return errors.Errorf("invalid callerId: %d", callerId)
	}
	pc.pagesUsedByCaller[callerId][pageId] = true
	return nil
}

func (pc *PagedCache[T]) splitPageNoLock(id int, page *page[T]) {
	v := make([][]T, 2)
	i := 0
	ids := make([]uint64, 0, len(page.ids))
	for id := range page.ids {
		ids = append(ids, id)
	}
	for _, id := range ids {
		v[i], _ = page.extractVector(id)
		i++
		if i == 2 {
			break
		}
	}
	page0 := pc.createPage()
	page1 := pc.createPage()
	for _, id := range ids {
		vec, _ := page.extractVector(id)
		d0, _ := pc.distancer.SingleDist(vec, v[0])
		d1, _ := pc.distancer.SingleDist(vec, v[1])
		if d0 < d1 {
			page0.add(id, vec)
		} else {
			page1.add(id, vec)
			pc.shardedLocks.Lock(id)
			pc.pageForId[id] = len(pc.pages)
			pc.shardedLocks.Unlock(id)
		}
	}
	pc.pages[id] = page0
	pc.pages = append(pc.pages, page1)
}

func (pc *PagedCache[T]) fileNameForId(id int) string {
	return path.Join(pc.path, fmt.Sprint(id, ".bin"))
}
